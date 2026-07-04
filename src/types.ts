// Types. Mirrors schema/pipeline-db-init.sql, keep the two in step.
//
// Deliberately smaller than LangGraph. Linear named steps with conditional
// skip, per-step retry/timeout/error policy, a checkpoint after every step,
// crash resume, and approvals that park indefinitely. That covers the real
// shapes (build a pack, run the invoices, assemble a report) without a graph
// VM to reason about. Fan-out stays inside a step as a plain Promise.all.

export type PipelineRunStatus =
  "pending" | "running" | "waiting_approval" | "completed" | "failed" | "cancelled";

export type StepStatus = "running" | "ok" | "error" | "interrupted" | "skipped" | "compensated";

export type InterruptStatus = "pending" | "approved" | "rejected" | "expired";

/** JSON-serialisable pipeline state. */
export type PipelineState = Record<string, unknown>;

export interface RetryPolicy {
  /** Total attempts including the first (default 1 = no retry). */
  maxAttempts?: number;
  /** First backoff delay (default 1_000ms). */
  initialDelayMs?: number;
  /** Exponential factor (default 2). */
  backoffFactor?: number;
  /** Backoff ceiling (default 60_000ms). */
  maxDelayMs?: number;
  /** Full jitter fraction [0,1] applied to each delay (default 0.2). */
  jitter?: number;
  /** Which errors retry (default: everything except PipelineInterrupt/Rejected). */
  retryOn?: (err: unknown) => boolean;
}

export interface StepContext {
  runId: string;
  pipeline: string;
  stepName: string;
  attempt: number;
  agentId: string | null;
  /**
   * Park the run for human approval. Behaviour:
   *   - no interrupt row yet → creates a `pending` row and throws
   *     `PipelineInterrupt` (run parks as waiting_approval, and stays parked
   *     across process restarts);
   *   - row `approved` → RESOLVES with the operator's `resolution` value
   *     (deterministic on step re-execution after a crash);
   *   - row `rejected` → throws `PipelineRejectedError` (run fails);
   *   - row still `pending` → throws `PipelineInterrupt` again.
   */
  interrupt(reason: string, payload?: unknown): Promise<unknown>;
  /** Emit a custom trace event (child span under the step). */
  emit(name: string, data?: unknown): void;
  /** Structured log line bound to this run/step. */
  log(msg: string, fields?: Record<string, unknown>): void;
  /** Aborted when the step times out. Pass it to fetch etc. */
  signal: AbortSignal;
}

export interface StepDefinition<S extends PipelineState> {
  name: string;
  /** Return the next state (or void to keep the current state). */
  run(state: S, ctx: StepContext): Promise<S | void> | S | void;
  /** Skip this step (recorded as `skipped`) when true. */
  skipIf?(state: S): boolean;
  retry?: RetryPolicy;
  /** Per-attempt wall clock; timeout errors are retryable. */
  timeoutMs?: number;
  /**
   * Once retries are gone: "fail" (default), "skip" (record the
   * error, continue with unchanged state), or a compensation function whose
   * return value becomes the state (recorded as `compensated`, run continues).
   */
  onError?: "fail" | "skip" | ((state: S, err: unknown, ctx: StepContext) => Promise<S> | S);
}

export interface PipelineDefinition<S extends PipelineState> {
  name: string;
  version?: string;
  steps: ReadonlyArray<StepDefinition<S>>;
}

export interface PipelineRunRecord {
  run_id: string;
  pipeline: string;
  pipeline_version: string | null;
  agent_id: string | null;
  status: PipelineRunStatus;
  state: PipelineState;
  current_step: string | null;
  step_index: number;
  error: string | null;
  trace_id: string | null;
  metadata: Record<string, unknown> | null;
  resume_count: number;
  created_at: Date;
  updated_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
}

export interface StepRecord {
  run_id: string;
  step_name: string;
  step_index: number;
  attempt: number;
  status: StepStatus;
  state_after: PipelineState | null;
  error: string | null;
  started_at: Date;
  completed_at: Date | null;
  latency_ms: number | null;
}

export interface InterruptRecord {
  id: string;
  run_id: string;
  step_name: string;
  reason: string;
  payload: unknown;
  status: InterruptStatus;
  requested_at: Date;
  resolved_at: Date | null;
  resolved_by: string | null;
  resolution: unknown;
  expires_at: Date | null;
}

/** Thrown (internally) when a step parks on a pending interrupt. */
export class PipelineInterrupt extends Error {
  readonly interruptId: string;
  readonly reason: string;
  constructor(interruptId: string, reason: string) {
    super(`pipeline interrupted: ${reason}`);
    this.name = "PipelineInterrupt";
    this.interruptId = interruptId;
    this.reason = reason;
  }
}

/** Thrown into the step when its interrupt was rejected by the operator. */
export class PipelineRejectedError extends Error {
  readonly interruptId: string;
  constructor(interruptId: string, reason: string) {
    super(`pipeline interrupt rejected: ${reason}`);
    this.name = "PipelineRejectedError";
    this.interruptId = interruptId;
  }
}

/** Thrown when a step's per-attempt timeout fires (retryable by default). */
export class StepTimeoutError extends Error {
  constructor(stepName: string, timeoutMs: number) {
    super(`step "${stepName}" timed out after ${timeoutMs}ms`);
    this.name = "StepTimeoutError";
  }
}

/** Serialisation cap. A truncated checkpoint is worse than a loud failure. */
export const MAX_STATE_BYTES = 256 * 1024;

export const PIPELINE_PG_CONN_ENV = "PIPELINE_PG_CONN";
export const PIPELINE_PG_CONN_FALLBACK_ENV = "APP_STATE_PG_CONN";
