/**
 * Span emission (obs.runs / obs.feedback).
 *
 * Spans form a tree via `dotted_order`, so a flat `ORDER BY dotted_order`
 * reconstructs depth-first execution order with no recursive joins. Emission
 * is fire-and-forget: a failing sink must never fail the work being traced.
 */

export {
  DEFAULT_PAYLOAD_CAP_BYTES,
  FEEDBACK_SOURCES,
  OBS_TRACES_DISABLED_ENV,
  OBS_TRACES_SAMPLE_RATE_ENV,
  RUN_SOURCES,
  RUN_TYPES,
  type FeedbackRow,
  type FeedbackSource,
  type RunRow,
  type RunSource,
  type RunStatus,
  type RunType,
} from "./types.js";

export {
  DOTTED_ORDER_SEGMENT_LEN,
  DOTTED_ORDER_TIME_CHARS,
  childDottedOrder,
  dottedOrderDepth,
  dottedOrderSegment,
  dottedOrderTimestamp,
  parseDottedOrder,
  type DottedOrderSegment,
} from "./dotted-order.js";

export { capPayload, isRedactableKey, redactSensitiveKeys, type CappedPayload } from "./redact.js";

export {
  InMemoryTraceSink,
  PgTraceSink,
  buildRunsUpsertSql,
  configureTraceSink,
  feedbackRowParams,
  getTraceSink,
  resetTraceSinkForTests,
  runRowParams,
  type PgTraceSinkOptions,
  type TraceSink,
} from "./sink.js";

export {
  RunHandle,
  currentRun,
  startRun,
  withSpan,
  type ChildRunOptions,
  type EndRunOptions,
  type ModelUsage,
  type StartRunOptions,
} from "./tracer.js";
