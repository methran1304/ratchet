/**
 * Sinks. Where RunRow snapshots actually go.
 *
 * PgTraceSink batches upserts into obs.runs on ANALYTICS_PG_CONN with the
 * same never-throw, bounded-queue posture as
 * core/src/email-engine/triage-telemetry.ts (telemetry must never break the
 * traced code path). Every op carries the COMPLETE row snapshot; a batch is
 * deduped keeping the LAST snapshot per run_id, and the multi-row
 * INSERT ... ON CONFLICT (run_id) DO UPDATE makes start/end patches
 * order-insensitive.
 *
 * InMemoryTraceSink backs unit tests and the eval runner's report mode.
 */

import { FEEDBACK_SOURCES, type FeedbackRow, type RunRow } from "./types.js";

export interface TraceSink {
  enqueueRun(row: RunRow): void;
  enqueueFeedback(row: FeedbackRow): void;
  /** Drain pending writes. Call this from a batch CLI before exit. */
  flush(): Promise<void>;
}

// ---------------------------------------------------------------------------
// In-memory sink (tests, dry inspection)
// ---------------------------------------------------------------------------

export class InMemoryTraceSink implements TraceSink {
  readonly runs: RunRow[] = [];
  readonly feedback: FeedbackRow[] = [];

  enqueueRun(row: RunRow): void {
    const existing = this.runs.findIndex((r) => r.run_id === row.run_id);
    if (existing >= 0) this.runs[existing] = row;
    else this.runs.push(row);
  }

  enqueueFeedback(row: FeedbackRow): void {
    this.feedback.push(row);
  }

  async flush(): Promise<void> {
    /* nothing queued */
  }

  /** Runs in depth-first execution order (what the dashboard renders). */
  get ordered(): RunRow[] {
    return [...this.runs].sort((a, b) => a.dotted_order.localeCompare(b.dotted_order));
  }
}

// ---------------------------------------------------------------------------
// Postgres sink
// ---------------------------------------------------------------------------

const RUN_COLUMNS = [
  "run_id",
  "trace_id",
  "parent_run_id",
  "dotted_order",
  "agent_id",
  "session_key",
  "source",
  "run_type",
  "name",
  "status",
  "error",
  "inputs",
  "outputs",
  "metadata",
  "tags",
  "model",
  "provider",
  "input_tokens",
  "output_tokens",
  "cache_creation_input_tokens",
  "cache_read_input_tokens",
  "cost",
  "start_time",
  "end_time",
  "latency_ms",
  "reference_example_id",
] as const;

/** Columns a later snapshot may change (start-immutable fields excluded). */
const RUN_UPDATE_COLUMNS = [
  "status",
  "error",
  "inputs",
  "outputs",
  "metadata",
  "tags",
  "model",
  "provider",
  "input_tokens",
  "output_tokens",
  "cache_creation_input_tokens",
  "cache_read_input_tokens",
  "cost",
  "end_time",
  "latency_ms",
  "reference_example_id",
] as const;

export function buildRunsUpsertSql(rowCount: number): string {
  const width = RUN_COLUMNS.length;
  const tuples: string[] = [];
  for (let r = 0; r < rowCount; r++) {
    const params: string[] = [];
    for (let c = 1; c <= width; c++) params.push(`$${r * width + c}`);
    tuples.push(`(${params.join(",")})`);
  }
  const updates = RUN_UPDATE_COLUMNS.map((c) => `${c} = EXCLUDED.${c}`).join(", ");
  return (
    `INSERT INTO obs.runs (${RUN_COLUMNS.join(", ")}) VALUES ${tuples.join(", ")} ` +
    `ON CONFLICT (run_id) DO UPDATE SET ${updates}`
  );
}

export function runRowParams(row: RunRow): unknown[] {
  return [
    row.run_id,
    row.trace_id,
    row.parent_run_id,
    row.dotted_order,
    row.agent_id,
    row.session_key,
    row.source,
    row.run_type,
    row.name,
    row.status,
    row.error,
    row.inputs === null ? null : JSON.stringify(row.inputs),
    row.outputs === null ? null : JSON.stringify(row.outputs),
    row.metadata === null ? null : JSON.stringify(row.metadata),
    row.tags,
    row.model,
    row.provider,
    row.input_tokens,
    row.output_tokens,
    row.cache_creation_input_tokens,
    row.cache_read_input_tokens,
    row.cost,
    row.start_time,
    row.end_time,
    row.latency_ms,
    row.reference_example_id,
  ];
}

const FEEDBACK_INSERT_SQL = `
  INSERT INTO obs.feedback
    (run_id, trace_id, key, score, value, comment, correction, source, created_by)
  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
`;

export function feedbackRowParams(row: FeedbackRow): unknown[] {
  const source = (FEEDBACK_SOURCES as readonly string[]).includes(row.source) ? row.source : "api";
  return [
    row.run_id,
    row.trace_id,
    row.key,
    row.score,
    row.value,
    row.comment,
    row.correction === null ? null : JSON.stringify(row.correction),
    source,
    row.created_by,
  ];
}

const MAX_QUEUED_ROWS = 1_000;
const MAX_BATCH_ROWS = 50;
const FLUSH_INTERVAL_MS = 1_000;
const CONNECTION_TIMEOUT_MS = 5_000;
const QUERY_TIMEOUT_MS = 10_000;
const IDLE_TIMEOUT_MS = 10_000;

type PgPool = import("pg").Pool;

export interface PgTraceSinkOptions {
  connectionString: string;
  /** Injectable for tests. */
  poolFactory?: () => Promise<PgPool>;
  flushIntervalMs?: number;
}

export class PgTraceSink implements TraceSink {
  private readonly conn: string;
  private readonly poolFactory: () => Promise<PgPool>;
  private readonly flushIntervalMs: number;
  private pool: Promise<PgPool> | undefined;
  private runQueue = new Map<string, RunRow>();
  private feedbackQueue: FeedbackRow[] = [];
  private timer: NodeJS.Timeout | undefined;
  private flushing: Promise<void> = Promise.resolve();
  private dropped = 0;

  constructor(options: PgTraceSinkOptions) {
    this.conn = options.connectionString;
    this.flushIntervalMs = options.flushIntervalMs ?? FLUSH_INTERVAL_MS;
    this.poolFactory =
      options.poolFactory ??
      (async () => {
        const { default: pg } = await import("pg");
        const pool = new pg.Pool({
          connectionString: this.conn,
          max: 2,
          connectionTimeoutMillis: CONNECTION_TIMEOUT_MS,
          query_timeout: QUERY_TIMEOUT_MS,
          idleTimeoutMillis: IDLE_TIMEOUT_MS,
        });
        pool.on("error", (err) => this.warn("obs_trace_sink_pool_error", err));
        return pool;
      });
  }

  private warn(msg: string, err?: unknown): void {
    console.error(
      JSON.stringify({
        level: "warn",
        msg,
        ...(err === undefined ? {} : { error: err instanceof Error ? err.message : String(err) }),
      }),
    );
  }

  enqueueRun(row: RunRow): void {
    if (this.runQueue.size >= MAX_QUEUED_ROWS && !this.runQueue.has(row.run_id)) {
      this.dropped += 1;
      if (this.dropped === 1 || this.dropped % 100 === 0) {
        this.warn("obs_trace_sink_queue_full_dropped");
      }
      return;
    }
    // Map preserves insertion order; a later snapshot for the same run
    // replaces the earlier one (end patch wins over start).
    this.runQueue.set(row.run_id, row);
    this.schedule();
  }

  enqueueFeedback(row: FeedbackRow): void {
    if (this.feedbackQueue.length >= MAX_QUEUED_ROWS) {
      this.warn("obs_feedback_sink_queue_full_dropped");
      return;
    }
    this.feedbackQueue.push(row);
    this.schedule();
  }

  private schedule(): void {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.drain();
    }, this.flushIntervalMs);
    // A batch writer must never keep a short-lived CLI process alive.
    this.timer.unref?.();
  }

  private async drain(): Promise<void> {
    // Serialize drains so batches never interleave on the pool.
    this.flushing = this.flushing.then(async () => {
      while (this.runQueue.size > 0 || this.feedbackQueue.length > 0) {
        const runs = [...this.runQueue.values()].slice(0, MAX_BATCH_ROWS);
        for (const row of runs) this.runQueue.delete(row.run_id);
        const feedback = this.feedbackQueue.splice(0, MAX_BATCH_ROWS);
        try {
          if (!this.pool) this.pool = this.poolFactory();
          const pool = await this.pool;
          if (runs.length > 0) {
            await pool.query(
              buildRunsUpsertSql(runs.length),
              runs.flatMap((r) => runRowParams(r)),
            );
          }
          for (const fb of feedback) {
            await pool.query(FEEDBACK_INSERT_SQL, feedbackRowParams(fb));
          }
        } catch (err) {
          this.warn("obs_trace_sink_write_failed", err);
          // Drop it. Re-queueing a poisoned batch just loops forever.
        }
      }
    });
    await this.flushing;
  }

  async flush(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    await this.drain();
  }

  /** Test + shutdown helper: flush, then close the pool. */
  async close(): Promise<void> {
    await this.flush();
    if (this.pool) {
      const pool = await this.pool.catch(() => undefined);
      this.pool = undefined;
      await pool?.end().catch(() => undefined);
    }
  }
}

// ---------------------------------------------------------------------------
// Env-driven, overridable for tests and batch tools.
// ---------------------------------------------------------------------------

let configuredSink: TraceSink | null | undefined;

/** Explicitly set (or, with null, disable) the process-wide sink. */
export function configureTraceSink(sink: TraceSink | null): void {
  configuredSink = sink;
}

/**
 * Resolve the process-wide sink: an explicitly configured one wins; otherwise
 * a PgTraceSink when ANALYTICS_PG_CONN is present and OBS_TRACES_DISABLED is
 * not set, otherwise null and tracing is off (same activation posture as
 * triage-telemetry).
 */
export function getTraceSink(): TraceSink | null {
  if (configuredSink !== undefined) return configuredSink;
  if (process.env.OBS_TRACES_DISABLED === "1") {
    configuredSink = null;
    return configuredSink;
  }
  const conn = process.env.ANALYTICS_PG_CONN;
  configuredSink = conn ? new PgTraceSink({ connectionString: conn }) : null;
  return configuredSink;
}

/** Reset module state between test cases. */
export async function resetTraceSinkForTests(): Promise<void> {
  const sink = configuredSink;
  configuredSink = undefined;
  if (sink instanceof PgTraceSink) await sink.close().catch(() => undefined);
}
