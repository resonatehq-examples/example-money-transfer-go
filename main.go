// Package main demonstrates the saga pattern — multi-step work with
// compensation on failure — using the Resonate Go SDK.
//
// A transfer moves money between two accounts in an in-memory ledger as two
// durable steps: withdraw from the source, then deposit to the target. If the
// deposit fails, the workflow compensates inline by refunding the source, so
// the ledger is never left in a half-applied state. Each step is durable (its
// result is recorded in a promise) and idempotent (keyed by a deterministic
// operation id), so a worker crash mid-transfer replays without double-applying
// an entry.
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
		fmt.Printf("  [ledger] %s already applied (idempotent no-op)\n", opID)
		return l.balances[account]
	}
	l.applied[opID] = true
	l.balances[account] += amount
	sign := "+"
	if amount < 0 {
		sign = ""
	}
	fmt.Printf("  [ledger] %s: %s %s%.2f  // %s\n", opID, account, sign, amount, note)
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
//	1. withdraw the source     (durable checkpoint)
//	2. deposit the target      (durable checkpoint; may fail)
//	3. on a deposit failure, compensate inline by refunding the source.
//
// The compensation runs in the error branch, guarded by which steps actually
// completed — only the inverse of settled steps runs. Each compensation is
// itself a durable, idempotent ctx.Run step. Because the workflow body
// re-executes from the top on resume, already-settled steps short-circuit by
// promise id, so a crash mid-saga never double-applies a ledger entry.
func transferMoney(ctx *resonate.Context, args TransferArgs) (TransferResult, error) {
	fmt.Printf("\n[saga] transfer %s: %s -> %s  $%.2f\n",
		args.TransferID, args.Source, args.Target, args.Amount)

	withdrawID := args.TransferID + "-withdraw"
	depositID := args.TransferID + "-deposit"
	refundID := args.TransferID + "-refund"

	// Track which steps have settled, so compensation only undoes real work.
	withdrawn := false

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
	withdrawn = true

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
		// Inline guarded compensation. Undo the completed steps in reverse
		// order; here only the withdraw has settled, so we run exactly its
		// inverse — a refund back to the source. The same shape scales to N
		// steps: one `if <step>n { run inverse }` block per step, checked LIFO.
		fmt.Printf("[saga] deposit failed: %v — compensating\n", err)
		if withdrawn {
			fr, cerr := ctx.Run(refund, AccountOp{OpID: refundID, Account: args.Source, Amount: args.Amount})
			if cerr == nil {
				var r OpResult
				if err := fr.Await(&r); err != nil {
					// Best-effort rollback: the saga has already failed.
					fmt.Printf("[saga] refund failed: %v\n", err)
				}
			}
		}
		return TransferResult{
			TransferID: args.TransferID,
			Status:     "compensated",
			Error:      err.Error(),
		}, nil
	}

	fmt.Printf("[saga] transfer %s committed\n", args.TransferID)
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
	promiseID := flag.String("id", "money-transfer-1", "Promise ID (idempotency key). Re-use the same ID after a crash to re-attach to the existing workflow.")
	failDeposit := flag.Bool("fail", false, "Force the deposit step to fail so the saga compensates.")
	amount := flag.Float64("amount", 50, "Amount to transfer from source to target.")
	flag.Parse()

	// Seed the source account so it has funds to send.
	const source, target = "alice", "bob"
	bank.apply("seed-"+source, source, 200, "seed")
	fmt.Printf("opening balances: %s=%.2f %s=%.2f\n",
		source, bank.balance(source), target, bank.balance(target))

	var cfg resonate.Config
	if *serverURL != "" {
		// Real-server mode: crash recovery is fully demonstrable here.
		cfg = resonate.Config{URL: *serverURL}
		fmt.Printf("[main] connecting to server at %s\n", *serverURL)
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
		fmt.Println("[main] using localnet (in-process, no external server required)")
		fmt.Println("[main] note: localnet state is ephemeral — crash recovery requires -url=<server>")
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
	// The promise ID is the idempotency key. Resonate deduplicates on this ID,
	// so re-running with the same ID after a crash re-attaches to the existing
	// workflow rather than starting a new one. Pass -id=<value> to override —
	// use a fresh ID for the failure run so it isn't served the committed
	// result cached under the happy-path ID.
	id := *promiseID
	args := TransferArgs{
		TransferID:  id,
		Source:      source,
		Target:      target,
		Amount:      *amount,
		FailDeposit: *failDeposit,
	}

	fmt.Printf("[main] invoking saga id=%s fail=%t\n", id, *failDeposit)

	h, err := transferFn.Run(ctx, id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	out, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}

	fmt.Printf("[main] result: %+v\n", out)
	fmt.Printf("closing balances: %s=%.2f %s=%.2f\n",
		source, bank.balance(source), target, bank.balance(target))
}
