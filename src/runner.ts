// The runner. What it guarantees:
//
// - full state is checkpointed after every completed step, so resume
//   re-executes only the failed or unstarted one. Completed steps never run
//   again, which means side effects only have to be idempotent per step.
// - ctx.interrupt() parks the run as waiting_approval and leaves it there.
//   The process can die. Anything holding the store resumes it once somebody
//   approves or rejects.
// - per-step retry (jittered exponential backoff, typed retryOn) and a
//   per-attempt timeout delivered as an AbortSignal. Timeouts are retryable.
// - onError "skip", or a compensate fn, records the failure and carries on.
//   Saga compensation without needing a graph VM to express it.
// - every run and step emits a span, so the trace view and the run list
//   describe the same execution instead of two half-views of it.

import { randomUUID } from "node:crypto";

import { startRun, type RunHandle } from "./traces/index.js";
import { newInterruptRecord, type PipelineStore } from "./store.js";
import {
  PipelineInterrupt,
  PipelineRejectedError,
  StepTimeoutError,
  type PipelineDefinition,
  type PipelineRunRecord,
  type PipelineState,
  type RetryPolicy,
  type StepContext,
  type StepDefinition,
} from "./types.js";

export interface StartPipelineOptions<S extends PipelineState> {
  store: PipelineStore;
  initialState: S;
  agentId?: string;
  metadata?: Record<string, unknown>;
  runId?: string;
  /** Injectables for tests. */
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

export interface PipelineOutcome<S extends PipelineState> {
  runId: string;
  status: PipelineRunRecord["status"];
  state: S;
  error: string | null;
  /** Present when the run parked on an approval. */
  pendingInterruptId?: string;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

function resolveRetry(policy: RetryPolicy | undefined): Required<Omit<RetryPolicy, "retryOn">> & {
  retryOn: (err: unknown) => boolean;
} {
  return {
    maxAttempts: Math.max(1, policy?.maxAttempts ?? 1),
    initialDelayMs: policy?.initialDelayMs ?? 1_000,
    backoffFactor: policy?.backoffFactor ?? 2,
    maxDelayMs: policy?.maxDelayMs ?? 60_000,
    jitter: policy?.jitter ?? 0.2,
    retryOn:
      policy?.retryOn ??
      ((err: unknown) =>
        !(err instanceof PipelineInterrupt) && !(err instanceof PipelineRejectedError)),
  };
}

function errText(err: unknown): string {
  return (err instanceof Error ? err.message : String(err)).slice(0, 2_000);
}

async function runWithTimeout<T>(
  fn: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number | undefined,
  stepName: string,
): Promise<T> {
  const controller = new AbortController();
  if (!timeoutMs || timeoutMs <= 0) return fn(controller.signal);
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      fn(controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new StepTimeoutError(stepName, timeoutMs));
        }, timeoutMs);
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

interface EngineDeps {
  now: () => Date;
  sleep: (ms: number) => Promise<void>;
  random: () => number;
}

/**
 * Execute steps from `run.step_index` to completion / interrupt / failure.
 * Shared by start + resume.
 */
async function drive<S extends PipelineState>(
  definition: PipelineDefinition<S>,
  store: PipelineStore,
  run: PipelineRunRecord,
  deps: EngineDeps,
): Promise<PipelineOutcome<S>> {
  let state = run.state as S;

  const rootSpan: RunHandle = startRun({
    name: `pipeline:${definition.name}`,
    runType: "pipeline",
    source: "pipeline",
    agentId: run.agent_id ?? "system",
    sessionKey: `pipeline:${run.run_id}`,
    metadata: {
      run_id: run.run_id,
      resume_count: run.resume_count,
      pipeline_version: definition.version ?? null,
    },
  });
  await store.updateRun(run.run_id, {
    status: "running",
    trace_id: rootSpan.sampled ? rootSpan.traceId : null,
    started_at: run.started_at ?? deps.now(),
  });

  for (let index = run.step_index; index < definition.steps.length; index++) {
    const step = definition.steps[index] as StepDefinition<S>;

    if (step.skipIf?.(state)) {
      const at = deps.now();
      await store.recordStep({
        run_id: run.run_id,
        step_name: step.name,
        step_index: index,
        attempt: 1,
        status: "skipped",
        state_after: state,
        error: null,
        started_at: at,
        completed_at: at,
        latency_ms: 0,
      });
      await store.updateRun(run.run_id, { step_index: index + 1, current_step: step.name, state });
      continue;
    }

    const retry = resolveRetry(step.retry);
    let attempt = 0;
    let stepDone = false;

    while (!stepDone) {
      attempt += 1;
      const startedAt = deps.now();
      const stepSpan = rootSpan.child({
        name: `step:${step.name}`,
        runType: "step",
        metadata: { step_index: index, attempt },
      });
      const ctx: StepContext = {
        runId: run.run_id,
        pipeline: definition.name,
        stepName: step.name,
        attempt,
        agentId: run.agent_id,
        signal: new AbortController().signal, // replaced by runWithTimeout's signal below
        emit: (name, data) => {
          const child = stepSpan.child({ name: `event:${name}`, runType: "chain", inputs: data });
          child.end();
        },
        log: (msg, fields) => {
          console.error(
            JSON.stringify({
              level: "info",
              msg,
              pipeline: definition.name,
              run_id: run.run_id,
              step: step.name,
              attempt,
              ...fields,
            }),
          );
        },
        interrupt: async (reason, payload) => {
          const existing = await store.findInterrupt(run.run_id, step.name, reason);
          if (existing?.status === "approved") return existing.resolution;
          if (existing?.status === "rejected") {
            throw new PipelineRejectedError(existing.id, reason);
          }
          if (existing?.status === "pending") {
            throw new PipelineInterrupt(existing.id, reason);
          }
          const record = newInterruptRecord({
            runId: run.run_id,
            stepName: step.name,
            reason,
            payload,
          });
          await store.createInterrupt(record);
          throw new PipelineInterrupt(record.id, reason);
        },
      };

      try {
        const next = await runWithTimeout(
          async (signal) => {
            (ctx as { signal: AbortSignal }).signal = signal;
            return step.run(state, ctx);
          },
          step.timeoutMs,
          step.name,
        );
        if (next !== undefined) state = next;
        const completedAt = deps.now();
        // THE checkpoint. One write, so the step row and the cursor land
        // together: a crash between them would leave the step recorded "ok"
        // with step_index still pointing at it, and resume would re-run a side
        // effect that already happened.
        await store.commitStep(
          {
            run_id: run.run_id,
            step_name: step.name,
            step_index: index,
            attempt,
            status: "ok",
            state_after: state,
            error: null,
            started_at: startedAt,
            completed_at: completedAt,
            latency_ms: Math.max(0, completedAt.getTime() - startedAt.getTime()),
          },
          { state, step_index: index + 1, current_step: step.name },
        );
        stepSpan.end({ outputs: { attempt } });
        stepDone = true;
      } catch (err) {
        const completedAt = deps.now();
        const latency = Math.max(0, completedAt.getTime() - startedAt.getTime());

        if (err instanceof PipelineInterrupt) {
          await store.recordStep({
            run_id: run.run_id,
            step_name: step.name,
            step_index: index,
            attempt,
            status: "interrupted",
            state_after: null,
            error: err.reason,
            started_at: startedAt,
            completed_at: completedAt,
            latency_ms: latency,
          });
          // Park: state stays at the PRE-step checkpoint; on approval the
          // step re-runs from its top and ctx.interrupt resolves.
          await store.updateRun(run.run_id, {
            status: "waiting_approval",
            current_step: step.name,
            step_index: index,
          });
          stepSpan.end({
            outputs: { interrupted: err.reason },
            metadata: { interrupt_id: err.interruptId },
          });
          rootSpan.end({ outputs: { status: "waiting_approval", step: step.name } });
          return {
            runId: run.run_id,
            status: "waiting_approval",
            state,
            error: null,
            pendingInterruptId: err.interruptId,
          };
        }

        const retryable = attempt < retry.maxAttempts && retry.retryOn(err);
        await store.recordStep({
          run_id: run.run_id,
          step_name: step.name,
          step_index: index,
          attempt,
          status: "error",
          state_after: null,
          error: errText(err),
          started_at: startedAt,
          completed_at: completedAt,
          latency_ms: latency,
        });

        if (retryable) {
          stepSpan.fail(err, { metadata: { will_retry: true } });
          const base = Math.min(
            retry.maxDelayMs,
            retry.initialDelayMs * Math.pow(retry.backoffFactor, attempt - 1),
          );
          const jittered = base * (1 - retry.jitter * deps.random());
          await deps.sleep(Math.max(0, Math.round(jittered)));
          continue;
        }

        // Out of retries, so apply the step's error policy.
        if (step.onError === "skip") {
          stepSpan.fail(err, { metadata: { on_error: "skip" } });
          await store.updateRun(run.run_id, { step_index: index + 1, current_step: step.name });
          stepDone = true;
          continue;
        }
        if (typeof step.onError === "function") {
          try {
            state = await step.onError(state, err, ctx);
            const at = deps.now();
            await store.recordStep({
              run_id: run.run_id,
              step_name: step.name,
              step_index: index,
              attempt: attempt + 1,
              status: "compensated",
              state_after: state,
              error: errText(err),
              started_at: at,
              completed_at: at,
              latency_ms: 0,
            });
            await store.updateRun(run.run_id, {
              state,
              step_index: index + 1,
              current_step: step.name,
            });
            stepSpan.fail(err, { metadata: { on_error: "compensated" } });
            stepDone = true;
            continue;
          } catch (compErr) {
            stepSpan.fail(compErr);
            await store.updateRun(run.run_id, {
              status: "failed",
              error: `compensation failed: ${errText(compErr)} (original: ${errText(err)})`,
              current_step: step.name,
            });
            rootSpan.fail(compErr);
            return {
              runId: run.run_id,
              status: "failed",
              state,
              error: errText(compErr),
            };
          }
        }

        stepSpan.fail(err);
        await store.updateRun(run.run_id, {
          status: "failed",
          error: errText(err),
          current_step: step.name,
        });
        rootSpan.fail(err);
        return { runId: run.run_id, status: "failed", state, error: errText(err) };
      }
    }
  }

  const completedAt = deps.now();
  await store.updateRun(run.run_id, {
    status: "completed",
    completed_at: completedAt,
    error: null,
  });
  rootSpan.end({ outputs: { status: "completed", steps: definition.steps.length } });
  return { runId: run.run_id, status: "completed", state, error: null };
}

/** Create + drive a new run. */
export async function startPipeline<S extends PipelineState>(
  definition: PipelineDefinition<S>,
  options: StartPipelineOptions<S>,
): Promise<PipelineOutcome<S>> {
  const now = options.now ?? (() => new Date());
  const run: PipelineRunRecord = {
    run_id: options.runId ?? randomUUID(),
    pipeline: definition.name,
    pipeline_version: definition.version ?? null,
    agent_id: options.agentId ?? null,
    status: "pending",
    state: options.initialState,
    current_step: null,
    step_index: 0,
    error: null,
    trace_id: null,
    metadata: options.metadata ?? null,
    resume_count: 0,
    created_at: now(),
    updated_at: now(),
    started_at: null,
    completed_at: null,
  };
  await options.store.createRun(run);
  return drive(definition, options.store, run, {
    now,
    sleep: options.sleep ?? defaultSleep,
    random: options.random ?? Math.random,
  });
}

export interface ResumePipelineOptions {
  store: PipelineStore;
  now?: () => Date;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
}

/**
 * Resume a parked/failed run from its checkpoint. Completed steps are never
 * re-executed; the step that failed/interrupted re-runs from its top.
 */
export async function resumePipeline<S extends PipelineState>(
  definition: PipelineDefinition<S>,
  runId: string,
  options: ResumePipelineOptions,
): Promise<PipelineOutcome<S>> {
  const run = await options.store.loadRun(runId);
  if (!run) throw new Error(`pipeline run not found: ${runId}`);
  if (run.pipeline !== definition.name) {
    throw new Error(`run ${runId} belongs to pipeline "${run.pipeline}", not "${definition.name}"`);
  }
  if (run.status === "completed" || run.status === "cancelled") {
    return {
      runId,
      status: run.status,
      state: run.state as S,
      error: run.error,
    };
  }
  if (definition.version && run.pipeline_version && definition.version !== run.pipeline_version) {
    throw new Error(
      `pipeline version drift: run ${runId} checkpointed at "${run.pipeline_version}" ` +
        `but the loaded definition is "${definition.version}". Deploy the matching ` +
        "version or migrate the run state explicitly.",
    );
  }
  await options.store.updateRun(runId, { resume_count: run.resume_count + 1 });
  run.resume_count += 1;
  return drive(definition, options.store, run, {
    now: options.now ?? (() => new Date()),
    sleep: options.sleep ?? defaultSleep,
    random: options.random ?? Math.random,
  });
}

/** Typed pipeline definition helper (inference anchor). */
export function definePipeline<S extends PipelineState>(
  definition: PipelineDefinition<S>,
): PipelineDefinition<S> {
  if (definition.steps.length === 0) throw new Error(`pipeline "${definition.name}" has no steps`);
  const names = new Set<string>();
  for (const step of definition.steps) {
    if (names.has(step.name)) {
      throw new Error(`pipeline "${definition.name}" has duplicate step name "${step.name}"`);
    }
    names.add(step.name);
  }
  return definition;
}
