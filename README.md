# ratchet

Checkpointed, resumable multi-step pipelines with indefinite human-approval
interrupts. Linear named steps, per-step retry and timeout policies, Saga-style
compensation. LangGraph's useful semantics without a graph VM.

## Install

```bash
npm install @methran1304/ratchet
```

Node >= 20. `pg` is an optional peer dep, only needed for `PgPipelineStore`.
Apply `schema/pipeline-db-init.sql` (and `schema/obs-db-init.sql` for tracing).

## Usage

```ts
import {
  definePipeline,
  startPipeline,
  resumePipeline,
  resolvePipelineStore,
} from "@methran1304/ratchet";

const pipeline = definePipeline<{ invoiceId: string; total?: number }>({
  name: "invoice-run",
  steps: [
    { name: "fetch", run: async (s) => ({ ...s, total: await fetchTotal(s.invoiceId) }) },
    {
      name: "approve",
      run: async (s, ctx) => {
        if (s.total! > 10_000) await ctx.interrupt("over threshold", { total: s.total });
        return s;
      },
    },
    {
      name: "post",
      run: async (s, ctx) => post(s, { signal: ctx.signal }),
      retry: { maxAttempts: 3, initialDelayMs: 500 },
      timeoutMs: 30_000,
      onError: "skip",
    },
  ],
});

const store = resolvePipelineStore()!; // PgPipelineStore from PIPELINE_PG_CONN
const outcome = await startPipeline(pipeline, { store, initialState: { invoiceId: "INV-1" } });
// outcome.status === "waiting_approval" → the process may now exit.

// Later, in any process holding the same store:
await resumePipeline(pipeline, outcome.runId, { store });
```

## API

| Export | Purpose |
| --- | --- |
| `definePipeline(def)` | Declare a named, ordered list of steps |
| `startPipeline(def, opts)` | Run from step 0; returns a `PipelineOutcome` |
| `resumePipeline(def, runId, opts)` | Re-enter a parked or failed run |
| `InMemoryPipelineStore` | Zero-dependency store for tests |
| `PgPipelineStore(conn)` / `resolvePipelineStore(env?)` | Postgres-backed checkpointer |
| `PipelineInterrupt` / `PipelineRejectedError` / `StepTimeoutError` | Control-flow errors |

Per step: `skipIf`, `retry` (`maxAttempts`, backoff factor, ceiling, full jitter,
typed `retryOn`), `timeoutMs`, and `onError`: `"fail"`, `"skip"`, or a
compensation function whose return value becomes the state.

Inside a step, `ctx` gives you `interrupt()`, `emit()`, `log()` and a `signal`
aborted when the step's timeout fires.

## Execution contract

- **State is checkpointed after every completed step.** `resumePipeline` re-executes
  only the failed or unstarted step; completed steps never re-run, so side effects
  need to be idempotent at step granularity only.
- **`ctx.interrupt()` parks the run indefinitely.** The run goes `waiting_approval`
  and the process can die. Any process with the store resumes it once the interrupt
  row is approved or rejected. On re-execution after approval the call *resolves*
  with the approver's value, so the step stays deterministic.
- **State is size-capped** (`MAX_STATE_BYTES`, 256 KB). Failing loudly beats
  writing a truncated checkpoint.

## Tracing

Every run and step emits spans through `@methran1304/ratchet/traces`, a small
LangSmith-shaped tracer. Spans are ordered by `dotted_order`, one
`<timestamp><uuid>` segment per ancestor, so a flat `ORDER BY dotted_order`
yields depth-first execution order and a UI rebuilds the tree with no recursive
joins. Segment count is depth; the last segment is the run id.

Emission is fire-and-forget, payloads are size-capped and redacted before they
are enqueued. Cost is caller-supplied: the tracer stores it, it never prices
a call.

## Development

```bash
pnpm install
pnpm build
pnpm test
```

## License

MIT
