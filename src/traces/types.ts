/**
 * Span model. TypeScript mirror of `obs.runs` + `obs.feedback`
 * (schema/obs-db-init.sql). Keep the two in lockstep.
 *
 * The shape follows LangSmith's Run model where it is load-bearing
 * (dotted_order tree encoding, run_type taxonomy, cache-token detail,
 * feedback source taxonomy) and stays PG-native everywhere else.
 */

export const RUN_TYPES = ["turn", "llm", "tool", "chain", "pipeline", "step"] as const;
export type RunType = (typeof RUN_TYPES)[number];

export const RUN_SOURCES = [
  "gateway",
  "mail-engine",
  "mcp",
  "pipeline",
  "scheduler",
  "eval",
  "backfill",
] as const;
export type RunSource = (typeof RUN_SOURCES)[number];

export type RunStatus = "running" | "ok" | "error";

export const FEEDBACK_SOURCES = ["api", "model", "app"] as const;
export type FeedbackSource = (typeof FEEDBACK_SOURCES)[number];

/** Full row snapshot. Every sink op carries the whole thing. */
export interface RunRow {
  run_id: string;
  trace_id: string;
  parent_run_id: string | null;
  dotted_order: string;
  agent_id: string;
  session_key: string | null;
  source: RunSource;
  run_type: RunType;
  name: string;
  status: RunStatus;
  error: string | null;
  inputs: unknown | null;
  outputs: unknown | null;
  metadata: Record<string, unknown> | null;
  tags: string[] | null;
  model: string | null;
  provider: string | null;
  input_tokens: number | null;
  output_tokens: number | null;
  cache_creation_input_tokens: number | null;
  cache_read_input_tokens: number | null;
  cost: number | null;
  start_time: Date;
  end_time: Date | null;
  latency_ms: number | null;
  reference_example_id: string | null;
}

export interface FeedbackRow {
  run_id: string;
  trace_id: string;
  key: string;
  score: number | null;
  value: string | null;
  comment: string | null;
  correction: unknown | null;
  source: FeedbackSource;
  created_by: string | null;
}

/**
 * Payload cap. inputs/outputs get truncated to this many serialised bytes
 * BEFORE enqueueing (see redact.ts). 16 KB keeps a week of
 * spans in low tens of MB while preserving enough context to debug a turn.
 */
export const DEFAULT_PAYLOAD_CAP_BYTES = 16 * 1024;

/** Kill switch. Tracing is on whenever ANALYTICS_PG_CONN is set. */
export const OBS_TRACES_DISABLED_ENV = "OBS_TRACES_DISABLED";

/** Trace-level sampling rate env ([0,1], default 1). Root decision; children follow. */
export const OBS_TRACES_SAMPLE_RATE_ENV = "OBS_TRACES_SAMPLE_RATE";
