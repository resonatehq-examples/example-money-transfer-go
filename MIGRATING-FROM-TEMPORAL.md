# Coming from Temporal: Saga / Compensation

This guide maps the [`temporalio/samples-go/saga`](https://github.com/temporalio/samples-go/tree/main/saga) example — a money transfer with compensation — to this Resonate example, so you can port the saga pattern without guessing at API differences.

## The pattern

A saga is a multi-step operation where each step can be undone by a matching compensation, so that if a later step fails the already-completed steps are rolled back and the system is left consistent. Both samples model the same business operation: withdraw money from one account, deposit it into another, and if something goes wrong, put the money back.

In Temporal the steps are activities, and compensation is registered with `defer` blocks that run in reverse (LIFO) order when the workflow returns an error. In Resonate the steps are plain functions made durable by `ctx.Run`, and compensation runs inline in the error branch, guarded by which steps actually settled.

## Side by side

### Temporal (`samples-go/saga`)

```go
// saga/workflow.go
func TransferMoney(ctx workflow.Context, transferDetails TransferDetails) (err error) {
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute,
		MaximumAttempts:    3,
	}

	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         retryPolicy,
	}

	ctx = workflow.WithActivityOptions(ctx, options)

	err = workflow.ExecuteActivity(ctx, Withdraw, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			errCompensation := workflow.ExecuteActivity(ctx, WithdrawCompensation, transferDetails).Get(ctx, nil)
			err = multierr.Append(err, errCompensation)
		}
	}()

	err = workflow.ExecuteActivity(ctx, Deposit, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			errCompensation := workflow.ExecuteActivity(ctx, DepositCompensation, transferDetails).Get(ctx, nil)
			err = multierr.Append(err, errCompensation)
		}
	}()

	err = workflow.ExecuteActivity(ctx, StepWithError, transferDetails).Get(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}
```

Each step that needs undoing registers a `defer` immediately after it succeeds. When the workflow returns an error — here `StepWithError` always returns one to demonstrate the rollback — Go runs the deferred closures in reverse order: `DepositCompensation` first, then `WithdrawCompensation`. `multierr.Append` accumulates any compensation errors alongside the original failure. The activities themselves (`Withdraw`, `Deposit`, `WithdrawCompensation`, `DepositCompensation`, `StepWithError`) live in `saga/activity.go`.

### Resonate (this example)

```go
func transferMoney(ctx *resonate.Context, args TransferArgs) (TransferResult, error) {
	logf("\n[saga] transfer %s: %s -> %s  $%.2f\n",
		args.TransferID, args.Source, args.Target, args.Amount)

	withdrawID := args.TransferID + "-withdraw"
	depositID  := args.TransferID + "-deposit"
	refundID   := args.TransferID + "-refund"

	// Step 1 — withdraw from the source.
	f1, err := ctx.Run(withdraw, AccountOp{OpID: withdrawID, Account: args.Source, Amount: args.Amount})
	if err != nil {
		return TransferResult{}, fmt.Errorf("withdraw dispatch: %w", err)
	}
	var w OpResult
	if err := f1.Await(&w); err != nil {
		return TransferResult{}, fmt.Errorf("withdraw: %w", err)
	}

	// Step 2 — deposit to the target. NoRetry so a failure compensates at once.
	f2, err := ctx.Run(deposit,
		AccountOp{OpID: depositID, Account: args.Target, Amount: args.Amount, Fail: args.FailDeposit},
		resonate.RunOpts{RetryPolicy: resonate.NoRetry},
	)
	if err != nil {
		return TransferResult{}, fmt.Errorf("deposit dispatch: %w", err)
	}
	var d OpResult
	if err := f2.Await(&d); err != nil {
		// The deposit failed after the withdraw settled — undo it by refunding
		// the source. A longer saga undoes each completed step in reverse (LIFO).
		logf("[saga] deposit failed: %v — compensating\n", err)
		fr, cerr := ctx.Run(refund, AccountOp{OpID: refundID, Account: args.Source, Amount: args.Amount})
		if cerr == nil {
			var ref OpResult
			if err := fr.Await(&ref); err != nil {
				logf("[saga] refund failed: %v\n", err) // best-effort: the saga already failed
			}
		}
		return TransferResult{TransferID: args.TransferID, Status: "compensated", Error: err.Error()}, nil
	}

	logf("[saga] transfer %s committed\n", args.TransferID)
	return TransferResult{TransferID: args.TransferID, Status: "committed",
		Source: args.Source, Target: args.Target, Amount: args.Amount}, nil
}
```

`ctx.Run(fn, args)` runs a step as a durable child and returns a `*Future`; `f.Await(&out)` blocks until it settles and decodes the result. When the deposit fails, the error branch undoes the one step that completed — the withdraw — by refunding the source. A longer saga tracks which steps settled and undoes them in reverse (LIFO) order. (`logf` is a thin `fmt.Printf` wrapper that prints the trace unless `-quiet` or benchmark mode silences it.)

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.ExecuteActivity(ctx, Withdraw, …).Get(ctx, nil)` | `ctx.Run(withdraw, args)` + `f.Await(&out)` | A step is a plain function made durable by `ctx.Run`. There is no separate activity type and no `@activity`-style annotation. |
| `Withdraw` / `Deposit` activity funcs (`func(context.Context, T) error`) | `withdraw` / `deposit` step funcs (`func(*resonate.Context, T) (R, error)`) | Resonate steps take `*resonate.Context` and may return a typed result alongside the error. |
| `defer func(){ if err != nil { …Compensation… } }()` | inline `ctx.Run(<inverse>, …)` in the error branch | Temporal registers compensation eagerly and runs it via deferred LIFO unwind; Resonate runs it inline. This two-step transfer undoes the one completed step directly; a longer saga guards each inverse on whether its step settled and runs them LIFO. |
| `WithdrawCompensation` / `DepositCompensation` activities | `refund` step (and one inverse step per forward step) | Compensations are ordinary durable steps too. |
| `multierr.Append(err, errCompensation)` | explicit handling in the error branch | Resonate has no built-in multierror; decide per step whether a compensation failure should surface or be logged best-effort. |
| `workflow.ActivityOptions{StartToCloseTimeout, RetryPolicy}` + `WithActivityOptions` | `resonate.RunOpts{Timeout, RetryPolicy}` (optional 3rd arg to `ctx.Run`) | Per-call timeout and retry policy. `resonate.NoRetry` is the single-attempt policy used here so a failure compensates immediately. |
| `temporal.RetryPolicy{...}` | `resonate.ExponentialRetry{...}` / `resonate.ConstantRetry{...}` / `resonate.NoRetry` | Resonate's default is exponential backoff; pass a policy in `RunOpts` to override. |
| `w.RegisterWorkflow` / `w.RegisterActivity` on a `worker.Worker` (task queue) | `resonate.Register(r, "transferMoney", transferMoney)` | Only the top-level saga is registered. Step functions passed to `ctx.Run` are invoked by value and need no registration. |
| Workflow ID (`StartWorkflowOptions.ID`) | promise ID passed to `transferFn.Run(ctx, id, args)` | Both are the stable idempotency key. Re-running with the same ID re-attaches to the existing run. |
| `ReferenceID` threaded into each activity for idempotency | deterministic op ids (`<id>-withdraw`, `<id>-deposit`, `<id>-refund`) | Both make each step's effect idempotent so a replay applies it at most once. |

## Porting it, step by step

1. **Replace the imports.** Swap `go.temporal.io/sdk/workflow`, `go.temporal.io/sdk/temporal`, and `go.uber.org/multierr` for `github.com/resonatehq/resonate-sdk-go` (plus `github.com/resonatehq/resonate-sdk-go/localnet` if you run in in-process mode).

2. **Change the signatures.** The Temporal workflow receiver is `workflow.Context`; the Resonate saga receiver is `*resonate.Context`. Activities take `context.Context`; Resonate steps take `*resonate.Context` and may return a typed result. Pass workflow arguments as a single serialisable struct.

3. **Turn each activity call into a `ctx.Run` + `Await`.** `workflow.ExecuteActivity(ctx, Withdraw, d).Get(ctx, nil)` becomes `f, err := ctx.Run(withdraw, op)` then `err := f.Await(&out)`. Drop `WithActivityOptions`; pass `resonate.RunOpts{Timeout, RetryPolicy}` as the optional third argument to `ctx.Run` only where you need a non-default timeout or retry policy.

4. **Replace the `defer` compensation stack with inline undo.** Instead of registering a `defer` after each successful step, undo the completed steps in the error branch. For this two-step transfer that is a single refund of the one step that settled; for a longer saga, track which steps settled (a boolean per step) and run the inverse of each in reverse order. Either way the forward step and its compensation sit in the same code path rather than in a separate deferred stack.

5. **Make every step idempotent.** Temporal threads a `ReferenceID` through the activities; here, derive a deterministic op id per step from the saga's promise ID (`<id>-withdraw`, etc.) and have each step short-circuit if that op id was already applied. This keeps a replay after a crash from double-applying an entry.

6. **Drop the task-queue and worker/starter split.** Temporal's `saga/worker` registers the workflow and activities on `TransferMoneyTaskQueue`, and `saga/start` submits a run by that task queue. In Resonate you `resonate.Register` the saga and call `transferFn.Run(ctx, id, args)`; only the top-level saga is registered, and the promise ID is the idempotency key.

7. **Decide how compensation errors surface.** Temporal folds them into the returned error with `multierr.Append`. Choose per step: re-raise, or log best-effort because the saga has already failed (this example logs and returns a `compensated` status).

## What's different (and why)

**Deferred LIFO stack vs. inline guarded undo.** Temporal's idiom registers a `defer` right after each successful step; on any later error, Go unwinds the deferred closures in reverse order, which gives you correct LIFO rollback "for free." Resonate's idiom keeps the undo in the error branch, gated on which steps settled. The deferred version is more compact when there are many steps; the inline version keeps each failure's response visible at the point it can occur and avoids relying on a named return value being mutated by closures. Both produce the same reverse-order rollback.

**Where the failure is forced.** The Temporal sample appends a `StepWithError` activity that always fails, specifically to exercise *both* compensations (it triggers `DepositCompensation` then `WithdrawCompensation`). This example instead fails the deposit itself when you pass `-fail` — the realistic money-transfer failure — so only the withdraw needs undoing and a single refund runs. The structure generalises: add a forward step and its guarded inverse, and the error branch undoes whichever steps settled, in reverse order. The Temporal sample's two-compensation shape is exactly that pattern with the failure moved one step later.

**No decorators, and no workflow/activity split.** Go has no decorators in either system. Temporal differentiates workflows from activities by receiver type (`workflow.Context` vs `context.Context`) and registers them separately as methods on a `worker.Worker` (`w.RegisterWorkflow` / `w.RegisterActivity`); the split enforces a deterministic replay sandbox for workflow code while activities run arbitrary I/O. Resonate makes a plain function durable with `ctx.Run` and does not separate the two roles — the same function can do I/O and be replayed, so you keep side effects inside `ctx.Run` steps and out of the surrounding body.

**Replay model.** Both systems resume a crashed run by re-executing the workflow body. In Resonate the body runs again from the top; already-settled `ctx.Run` steps short-circuit by promise ID from the durable promise layer, so they are not re-executed. Anything *outside* a `ctx.Run` / `ctx.RPC` / `ctx.Sleep` boundary — a bare `fmt.Println`, a direct map write — runs again on every resume. That is why the ledger writes live inside the step functions, not in the saga body.

**Idempotency is yours to design.** Temporal's `WorkflowIDReusePolicy` handles re-submission of the whole workflow, and the sample threads a `ReferenceID` to keep activity effects idempotent. Resonate gives you the promise ID for workflow-level dedup; per-step idempotency is something you build, here with deterministic op ids and an applied-set in the ledger.

## Notes & coverage

- **Compensation failures.** This example treats the refund as best-effort (it logs and still returns a `compensated` status) because the saga has already failed. If you need the Temporal `multierr` behaviour — surfacing both the original error and a compensation error — collect them explicitly in the error branch; the Go SDK does not provide a multierror helper.
- **`localnet` mode.** This example ships an in-process `localnet` mode that needs no server, useful for exploring the API. Localnet state is in-memory, so it does not demonstrate crash recovery; use a real `resonate dev` server for that. Temporal always requires a server.
- **In-memory ledger.** The ledger here is a process-global map, kept deliberately small. A real service would inject a database handle and let each step's idempotency key be a row constraint, the way the Python and Rust money-transfer examples use SQLite `INSERT OR IGNORE`.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample: https://github.com/temporalio/samples-go/tree/main/saga
- This example's README
