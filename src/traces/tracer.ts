/**
 * Span construction + context propagation.
 *
 * `startRun()` opens a root span (fresh trace_id); `RunHandle.child()` /
 * `withSpan()` nest under it. An AsyncLocalStorage carries the ambient parent
 * so instrumented code deep in a call stack (an MCP tool handler, a pipeline
 * step) nests correctly without threading handles through every signature.
 *
 * Emission model: enqueue the complete row snapshot at START (status=running)
 * and again at END, so a crashed process leaves an honest `running` row and
 * the sink's upsert makes the pair order-insensitive. All writes are
 * fire-and-forget; tracing never throws into the traced path.
 */

import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";

import { childDottedOrder } from "./dotted-order.js";
import { capPayload } from "./redact.js";
import { getTraceSink } from "./sink.js";
import {
  OBS_TRACES_SAMPLE_RATE_ENV,
  type FeedbackRow,
  type FeedbackSource,
  type RunRow,
  type RunSource,
  type RunType,
} from "./types.js";

export interface StartRunOptions {
  name: string;
  runType: RunType;
  source: RunSource;
  agentId: string;
  sessionKey?: string | null;
  inputs?: unknown;
  metadata?: Record<string, unknown>;
  tags?: string[];
  /** Golden example id when produced by an eval experiment. */
  referenceExampleId?: string;
  /** Override the ambient parent (default: none for startRun, ambient for withSpan). */
  parent?: RunHandle | null;
  /** Clock override for tests. */
  now?: () => Date;
}

export interface ChildRunOptions {
  name: string;
  runType: RunType;
  inputs?: unknown;
  metadata?: Record<string, unknown>;
  tags?: string[];
  source?: RunSource;
  now?: () => Date;
}

export interface EndRunOptions {
  outputs?: unknown;
  metadata?: Record<string, unknown>;
}

/**
 * Token counts for a model call. Field names follow the shape every major
 * provider reports, so a caller can pass its SDK's usage object through with
 * minimal mapping. All optional: record what you've got.
 */
export interface UsageTokens {
  input_tokens?: number;
  output_tokens?: number;
  cache_creation_input_tokens?: number;
  cache_read_input_tokens?: number;
}

export interface ModelUsage {
  model: string;
  provider?: string;
  tokens: UsageTokens;
  /**
   * Cost of this call, in whatever currency the caller accounts in. Pricing
   * is deliberately NOT built in: rate cards change faster than a library
   * ships, so cost is supplied by the caller or left null.
   */
  cost?: number;
}

const als = new AsyncLocalStorage<RunHandle | undefined>();

function sampleRate(): number {
  const raw = process.env[OBS_TRACES_SAMPLE_RATE_ENV];
  if (!raw) return 1;
  const parsed = Number.parseFloat(raw);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) return 1;
  return parsed;
}

/**
 * Handle over one span. `sampled=false` handles are inert but still propagate
 * (children of an unsampled root are unsampled too, the root-decision
 * semantic).
 */
export class RunHandle {
  readonly runId: string;
  readonly traceId: string;
  readonly parentRunId: string | null;
  readonly dottedOrder: string;
  readonly sampled: boolean;
  private readonly row: RunRow;
  private readonly now: () => Date;
  private ended = false;

  private constructor(row: RunRow, sampled: boolean, now: () => Date) {
    this.row = row;
    this.sampled = sampled;
    this.now = now;
    this.runId = row.run_id;
    this.traceId = row.trace_id;
    this.parentRunId = row.parent_run_id;
    this.dottedOrder = row.dotted_order;
  }

  /** Inert handle. Never emits, and its children stay inert too. */
  static inert(): RunHandle {
    const now = () => new Date();
    const startTime = now();
    const runId = randomUUID();
    const row: RunRow = {
      run_id: runId,
      trace_id: runId,
      parent_run_id: null,
      dotted_order: childDottedOrder(null, startTime, runId),
      agent_id: "untraced",
      session_key: null,
      source: "mcp",
      run_type: "chain",
      name: "untraced",
      status: "running",
      error: null,
      inputs: null,
      outputs: null,
      metadata: null,
      tags: null,
      model: null,
      provider: null,
      input_tokens: null,
      output_tokens: null,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: null,
      cost: null,
      start_time: startTime,
      end_time: null,
      latency_ms: null,
      reference_example_id: null,
    };
    return new RunHandle(row, false, now);
  }

  static start(options: StartRunOptions): RunHandle {
    const now = options.now ?? (() => new Date());
    const startTime = now();
    const runId = randomUUID();
    const parent = options.parent ?? null;
    const sampled = parent ? parent.sampled : Math.random() < sampleRate();
    const traceId = parent ? parent.traceId : randomUUID();
    const row: RunRow = {
      run_id: runId,
      trace_id: traceId,
      parent_run_id: parent ? parent.runId : null,
      dotted_order: childDottedOrder(parent?.dottedOrder ?? null, startTime, runId),
      agent_id: options.agentId,
      session_key: options.sessionKey ?? null,
      source: options.source,
      run_type: options.runType,
      name: options.name,
      status: "running",
      error: null,
      inputs: capPayload(options.inputs),
      outputs: null,
      metadata: options.metadata ?? null,
      tags: options.tags ?? null,
      model: null,
      provider: null,
      input_tokens: null,
      output_tokens: null,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: null,
      cost: null,
      start_time: startTime,
      end_time: null,
      latency_ms: null,
      reference_example_id: options.referenceExampleId ?? null,
    };
    const handle = new RunHandle(row, sampled, now);
    handle.emit();
    return handle;
  }

  child(options: ChildRunOptions): RunHandle {
    return RunHandle.start({
      name: options.name,
      runType: options.runType,
      source: options.source ?? this.row.source,
      agentId: this.row.agent_id,
      sessionKey: this.row.session_key,
      inputs: options.inputs,
      metadata: options.metadata,
      tags: options.tags,
      parent: this,
      now: options.now ?? this.now,
    });
  }

  /** Token counts + cost for run_type "llm" spans. */
  setModelUsage(usage: ModelUsage): this {
    this.row.model = usage.model;
    this.row.provider = usage.provider ?? null;
    this.row.input_tokens = usage.tokens.input_tokens ?? 0;
    this.row.output_tokens = usage.tokens.output_tokens ?? 0;
    this.row.cache_creation_input_tokens = usage.tokens.cache_creation_input_tokens ?? 0;
    this.row.cache_read_input_tokens = usage.tokens.cache_read_input_tokens ?? 0;
    this.row.cost = usage.cost === undefined ? null : Number(usage.cost.toFixed(6));
    return this;
  }

  addMetadata(patch: Record<string, unknown>): this {
    this.row.metadata = { ...(this.row.metadata ?? {}), ...patch };
    return this;
  }

  addTags(...tags: string[]): this {
    this.row.tags = [...new Set([...(this.row.tags ?? []), ...tags])];
    return this;
  }

  end(options: EndRunOptions = {}): void {
    if (this.ended) return;
    this.ended = true;
    const endTime = this.now();
    if (options.outputs !== undefined) this.row.outputs = capPayload(options.outputs);
    if (options.metadata) this.addMetadata(options.metadata);
    this.row.status = "ok";
    this.row.end_time = endTime;
    this.row.latency_ms = Math.max(0, endTime.getTime() - this.row.start_time.getTime());
    this.emit();
  }

  fail(error: unknown, options: EndRunOptions = {}): void {
    if (this.ended) return;
    this.ended = true;
    const endTime = this.now();
    if (options.outputs !== undefined) this.row.outputs = capPayload(options.outputs);
    if (options.metadata) this.addMetadata(options.metadata);
    this.row.status = "error";
    this.row.error = (error instanceof Error ? error.message : String(error)).slice(0, 2_000);
    this.row.end_time = endTime;
    this.row.latency_ms = Math.max(0, endTime.getTime() - this.row.start_time.getTime());
    this.emit();
  }

  /** Attach feedback to this span (evals judges use source "model"). */
  feedback(input: {
    key: string;
    score?: number | null;
    value?: string | null;
    comment?: string | null;
    correction?: unknown;
    source?: FeedbackSource;
    createdBy?: string | null;
  }): void {
    if (!this.sampled) return;
    const sink = getTraceSink();
    if (!sink) return;
    const row: FeedbackRow = {
      run_id: this.runId,
      trace_id: this.traceId,
      key: input.key,
      score: input.score ?? null,
      value: input.value ?? null,
      comment: input.comment ?? null,
      correction: input.correction ?? null,
      source: input.source ?? "api",
      created_by: input.createdBy ?? null,
    };
    try {
      sink.enqueueFeedback(row);
    } catch {
      /* tracing never throws into the traced path */
    }
  }

  /** Run `fn` with this span as the ambient parent; auto end/fail. */
  async run<T>(fn: () => Promise<T> | T): Promise<T> {
    try {
      const result = await als.run(this, fn);
      this.end();
      return result;
    } catch (err) {
      this.fail(err);
      throw err;
    }
  }

  /** Bind this handle as ambient parent without lifecycle management. */
  bind<T>(fn: () => Promise<T> | T): Promise<T> | T {
    return als.run(this, fn);
  }

  /** Current row snapshot (tests + eval runner reports). */
  snapshot(): Readonly<RunRow> {
    return { ...this.row };
  }

  private emit(): void {
    if (!this.sampled) return;
    const sink = getTraceSink();
    if (!sink) return;
    try {
      sink.enqueueRun({ ...this.row });
    } catch {
      /* tracing never throws into the traced path */
    }
  }
}

/** Open a root span (or an explicit-parent child). */
export function startRun(options: StartRunOptions): RunHandle {
  return RunHandle.start(options);
}

/** The ambient span, when inside `handle.run()`/`bind()`. */
export function currentRun(): RunHandle | undefined {
  return als.getStore();
}

/**
 * Convenience: child span under the ambient parent (or a fresh root when
 * there is none and `orphan` options are provided), auto end/fail around fn.
 */
export async function withSpan<T>(
  options: ChildRunOptions & {
    /** Used only when no ambient parent exists. */
    orphan?: Pick<StartRunOptions, "source" | "agentId" | "sessionKey">;
  },
  fn: (span: RunHandle) => Promise<T> | T,
): Promise<T> {
  const parent = currentRun();
  let span: RunHandle;
  if (parent) {
    span = parent.child(options);
  } else if (options.orphan) {
    span = RunHandle.start({
      name: options.name,
      runType: options.runType,
      source: options.orphan.source,
      agentId: options.orphan.agentId,
      sessionKey: options.orphan.sessionKey ?? null,
      inputs: options.inputs,
      metadata: options.metadata,
      tags: options.tags,
      now: options.now,
    });
  } else {
    // No ambient parent and no orphan spec, so run untraced on an inert handle
    // so callers can still call span methods unconditionally.
    span = RunHandle.inert();
  }
  try {
    const result = await als.run(span, () => fn(span));
    span.end();
    return result;
  } catch (err) {
    span.fail(err);
    throw err;
  }
}
