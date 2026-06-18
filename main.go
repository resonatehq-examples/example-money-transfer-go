// Package main demonstrates the saga pattern — multi-step work with
// compensation on failure — using the Resonate Go SDK.
//
// A transfer moves money between two accounts in an in-memory ledger as two
// durable steps: withdraw from the source, then deposit to the target. If the
// deposit fails, the saga compensates inline by refunding the source, so the
// ledger is never left half-applied.
//
// Two layers keep a crash safe. The promise layer: a settled ctx.Run step is
// not re-executed when the workflow resumes — its result is replayed from the
// durable promise. The ledger layer: every entry is keyed by a deterministic
// operation id, so even a step that does re-run applies its entry at most once.
//
// # Modes
//
// By default the program uses localnet — an in-process transport that needs no
// external server, convenient for exploring the API. Localnet state lives in
// process memory, so a process crash also loses the ledger; to demonstrate true
// crash recovery, start a Resonate dev server and point the binary at it:
//
//	resonate dev                                  # terminal 1
//	go run . -url=http://localhost:8001           # terminal 2 (happy path)
//	go run . -url=http://localhost:8001 -fail -id=money-transfer-2  # compensation
//
// Pass -n>1 to run a batch of transfers and print a throughput summary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// verbose controls the play-by-play logging. It is set once in main before any
// saga runs — so the worker goroutines that read it through logf never race the
// write — and lets benchmark mode (-n>1) or -quiet silence the trace. logf is
// the single reader.
var verbose = true

// logf prints the play-by-play unless verbose has been turned off. Centralizing
// the check keeps the saga and the ledger readable: each line of narration is a
// single call rather than a wrapped if-block.
func logf(format string, a ...any) {
	if verbose {
		fmt.Printf(format, a...)
	}
}

// ── Ledger ──────────────────────────────────────────────────────────────
//
// A tiny in-memory double-entry ledger. Balances are derived by accumulating
// signed entries; an entry is keyed by a deterministic operation id so that
// applying the same entry twice (a replay after a crash) is a no-op. This is
// the Go analogue of the SQLite `INSERT OR IGNORE` idempotency the Python and
// Rust money-transfer examples use.
type ledger struct {
	mu       sync.Mutex
	applied  map[string]bool    // opID -> already applied
	balances map[string]float64 // account -> balance
}

func newLedger() *ledger {
	return &ledger{applied: map[string]bool{}, balances: map[string]float64{}}
}

// apply records a signed entry against an account, keyed by opID. It returns
// the account's resulting balance. Calling apply again with the same opID is an
// idempotent no-op — the entry is already reflected in the balance.
func (l *ledger) apply(opID, account string, amount float64, note string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.applied[opID] {
		logf("  [ledger] %s already applied (idempotent no-op)\n", opID)
		return l.balances[account]
	}
	l.applied[opID] = true
	l.balances[account] += amount

	sign := "+"
	if amount < 0 {
		sign = ""
	}
	logf("  [ledger] %s: %s %s%.2f  // %s\n", opID, account, sign, amount, note)
	return l.balances[account]
}

func (l *ledger) balance(account string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balances[account]
}

// bank is a process-global dependency the step functions write to. An example
// keeps this simple; a real service would inject a database handle instead.
var bank = newLedger()

// ── Domain types ────────────────────────────────────────────────────────

// TransferArgs is the single serialisable argument to the saga workflow.
type TransferArgs struct {
	TransferID  string  `json:"transfer_id"`
	Source      string  `json:"source"`
	Target      string  `json:"target"`
	Amount      float64 `json:"amount"`
	FailDeposit bool    `json:"fail_deposit"` // force the deposit step to fail
}

// TransferResult reports how the saga ended: "committed" or "compensated".
type TransferResult struct {
	TransferID string  `json:"transfer_id"`
	Status     string  `json:"status"`
	Source     string  `json:"source,omitempty"`
	Target     string  `json:"target,omitempty"`
	Amount     float64 `json:"amount,omitempty"`
	Error      string  `json:"error,omitempty"`
}

// AccountOp is the argument to a single ledger step.
type AccountOp struct {
	OpID    string  `json:"op_id"`
	Account string  `json:"account"`
	Amount  float64 `json:"amount"`
	Fail    bool    `json:"fail"`
}

// OpResult is what a ledger step returns.
type OpResult struct {
	OpID    string  `json:"op_id"`
	Balance float64 `json:"balance"`
}

// ── Step functions (each invoked via ctx.Run) ───────────────────────────

// withdraw debits the source account. Idempotent on op.OpID.
func withdraw(_ *resonate.Context, op AccountOp) (OpResult, error) {
	bal := bank.apply(op.OpID, op.Account, -op.Amount, "withdraw")
	return OpResult{OpID: op.OpID, Balance: bal}, nil
}

// deposit credits the target account. Pass Fail=true to simulate the target
// rejecting the credit. Idempotent on op.OpID.
func deposit(_ *resonate.Context, op AccountOp) (OpResult, error) {
	if op.Fail {
		return OpResult{}, fmt.Errorf("account %s rejected the deposit", op.Account)
	}
	bal := bank.apply(op.OpID, op.Account, op.Amount, "deposit")
	return OpResult{OpID: op.OpID, Balance: bal}, nil
}

// refund is the compensating step for a completed withdraw: it credits the
// amount back to the source. Idempotent on op.OpID.
func refund(_ *resonate.Context, op AccountOp) (OpResult, error) {
	bal := bank.apply(op.OpID, op.Account, op.Amount, "refund")
	return OpResult{OpID: op.OpID, Balance: bal}, nil
}

// ── The saga ────────────────────────────────────────────────────────────

// transferMoney moves args.Amount from the source account to the target
// account as a saga:
//
//  1. withdraw the source   (durable step)
//  2. deposit the target    (durable step; may fail)
//  3. on a deposit failure, compensate by refunding the source.
//
// Each step is a ctx.Run child. On resume the body re-executes from the top,
// but settled steps short-circuit by promise id, so a crash mid-saga never
// repeats a completed step or double-applies a ledger entry.
func transferMoney(ctx *resonate.Context, args TransferArgs) (TransferResult, error) {
	logf("\n[saga] transfer %s: %s -> %s  $%.2f\n",
		args.TransferID, args.Source, args.Target, args.Amount)

	withdrawID := args.TransferID + "-withdraw"
	depositID := args.TransferID + "-deposit"
	refundID := args.TransferID + "-refund"

	// Step 1 — withdraw from the source.
	f1, err := ctx.Run(withdraw, AccountOp{OpID: withdrawID, Account: args.Source, Amount: args.Amount})
	if err != nil {
		return TransferResult{}, fmt.Errorf("withdraw dispatch: %w", err)
	}
	var w OpResult
	if err := f1.Await(&w); err != nil {
		// Nothing has moved yet — no compensation required.
		return TransferResult{}, fmt.Errorf("withdraw: %w", err)
	}

	// Step 2 — deposit to the target. RetryPolicy: NoRetry is intentional —
	// this saga's compensation IS the response to a deposit-side failure, so we
	// don't want the SDK's default exponential backoff to delay it. In
	// production you might allow a few retries first (network blips happen) and
	// compensate only once the target has clearly rejected the deposit.
	f2, err := ctx.Run(deposit,
		AccountOp{OpID: depositID, Account: args.Target, Amount: args.Amount, Fail: args.FailDeposit},
		resonate.RunOpts{RetryPolicy: resonate.NoRetry},
	)
	if err != nil {
		return TransferResult{}, fmt.Errorf("deposit dispatch: %w", err)
	}
	var d OpResult
	if err := f2.Await(&d); err != nil {
		// The deposit failed after the withdraw settled, so the transfer is
		// half-done. Undo the one step that completed by refunding the source.
		// A longer saga undoes each completed step in reverse order — track
		// which settled and run their inverses LIFO. The refund is itself a
		// durable, idempotent ctx.Run step.
		logf("[saga] deposit failed: %v — compensating\n", err)
		fr, cerr := ctx.Run(refund, AccountOp{OpID: refundID, Account: args.Source, Amount: args.Amount})
		if cerr == nil {
			var ref OpResult
			if err := fr.Await(&ref); err != nil {
				logf("[saga] refund failed: %v\n", err) // best-effort: the saga has already failed
			}
		}
		return TransferResult{
			TransferID: args.TransferID,
			Status:     "compensated",
			Error:      err.Error(),
		}, nil
	}

	logf("[saga] transfer %s committed\n", args.TransferID)
	return TransferResult{
		TransferID: args.TransferID,
		Status:     "committed",
		Source:     args.Source,
		Target:     args.Target,
		Amount:     args.Amount,
	}, nil
}

// ── main ────────────────────────────────────────────────────────────────

func main() {
	serverURL := flag.String("url", "", "Resonate server URL (e.g. http://localhost:8001). Omit to use localnet.")
	promiseID := flag.String("id", "money-transfer-1", "Promise ID (idempotency key). Re-use the same ID after a crash to re-attach to the existing workflow. In -n mode this becomes the ID prefix.")
	failDeposit := flag.Bool("fail", false, "Force the deposit step to fail so the saga compensates.")
	amount := flag.Float64("amount", 50, "Amount to transfer from source to target.")
	n := flag.Int("n", 1, "Number of transfers to run sequentially. n>1 enables benchmark mode: a throughput summary is printed at the end.")
	quiet := flag.Bool("quiet", false, "Suppress the per-step play-by-play and print only the final result. Implied by -n>1.")
	flag.Parse()

	benchmark := *n > 1
	verbose = !(*quiet || benchmark)

	// Seed the source account so it has funds to send. In benchmark mode we
	// seed enough to cover all N transfers without going negative.
	const source, target = "alice", "bob"
	seedAmount := 200.0
	if benchmark {
		seedAmount = float64(*n) * (*amount) * 2 // generous headroom
	}
	bank.apply("seed-"+source, source, seedAmount, "seed")
	logf("opening balances: %s=%.2f %s=%.2f\n",
		source, bank.balance(source), target, bank.balance(target))

	var cfg resonate.Config
	if *serverURL != "" {
		// Real-server mode: crash recovery is fully demonstrable here.
		cfg = resonate.Config{URL: *serverURL}
		logf("[main] connecting to server at %s\n", *serverURL)
	} else {
		// Localnet mode: in-process transport, no external server needed.
		// NoopHeartbeat is required — localnet has no HTTP endpoint for the
		// default AsyncHeartbeat to reach.
		pid := "money-transfer-worker"
		cfg = resonate.Config{
			Network:   localnet.NewLocal("default", &pid),
			Heartbeat: resonate.NoopHeartbeat{},
			TTL:       5 * time.Minute,
		}
		logf("[main] using localnet (in-process, no external server required)\n")
		logf("[main] note: localnet state is ephemeral — crash recovery requires -url=<server>\n")
	}

	r, err := resonate.New(cfg)
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	// Only the top-level saga is registered. The step functions (withdraw,
	// deposit, refund) are invoked via ctx.Run, which takes the function value
	// directly — they don't need to be registered.
	transferFn, err := resonate.Register(r, "transferMoney", transferMoney)
	if err != nil {
		log.Fatalf("Register: %v", err)
	}

	ctx := context.Background()

	if !benchmark {
		// ── Single-transfer mode ─────────────────────────────────────────────
		// The promise ID is the idempotency key. Resonate deduplicates on this
		// ID, so re-running with the same ID after a crash re-attaches to the
		// existing workflow rather than starting a new one. Pass -id=<value> to
		// override — use a fresh ID for the failure run so it isn't served the
		// committed result cached under the happy-path ID.
		id := *promiseID
		args := TransferArgs{
			TransferID:  id,
			Source:      source,
			Target:      target,
			Amount:      *amount,
			FailDeposit: *failDeposit,
		}

		logf("[main] invoking saga id=%s fail=%t\n", id, *failDeposit)

		h, err := transferFn.Run(ctx, id, args)
		if err != nil {
			log.Fatalf("Run: %v", err)
		}

		out, err := h.Result(ctx)
		if err != nil {
			log.Fatalf("Result: %v", err)
		}

		fmt.Printf("[main] result: %+v\n", out)
		logf("closing balances: %s=%.2f %s=%.2f\n",
			source, bank.balance(source), target, bank.balance(target))
		return
	}

	// ── Benchmark mode (-n > 1) ──────────────────────────────────────────────
	// Run N transfers sequentially, each with a unique promise ID derived from
	// the -id prefix. The play-by-play is silenced (verbose=false), and a
	// throughput summary is printed at the end so this can drive benchmarks.
	//
	// Each ID is unique so Resonate creates a fresh promise per transfer — this
	// measures the full create→execute→settle round trip, not cache hits.
	fmt.Printf("[bench] running %d sequential transfers (id prefix=%s amount=%.2f fail=%t)\n",
		*n, *promiseID, *amount, *failDeposit)

	committed := 0
	compensated := 0
	start := time.Now()

	for i := 0; i < *n; i++ {
		id := fmt.Sprintf("%s-%d", *promiseID, i+1)
		args := TransferArgs{
			TransferID:  id,
			Source:      source,
			Target:      target,
			Amount:      *amount,
			FailDeposit: *failDeposit,
		}

		h, err := transferFn.Run(ctx, id, args)
		if err != nil {
			log.Fatalf("Run[%d]: %v", i, err)
		}
		out, err := h.Result(ctx)
		if err != nil {
			log.Fatalf("Result[%d]: %v", i, err)
		}
		switch out.Status {
		case "committed":
			committed++
		case "compensated":
			compensated++
		}
	}

	elapsed := time.Since(start)
	tps := float64(*n) / elapsed.Seconds()
	fmt.Printf("[bench] done  n=%d committed=%d compensated=%d elapsed=%s tps=%.1f\n",
		*n, committed, compensated, elapsed.Round(time.Millisecond), tps)
}
