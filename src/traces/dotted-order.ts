/**
 * LangSmith-style dotted-order encoding.
 *
 * Each span's `dotted_order` = its parent's dotted_order + "." + its own
 * segment; a segment is `<start_time %Y%m%dT%H%M%S%fZ><run uuid>` (23 chars of
 * UTC timestamp with microsecond padding + 36-char uuid). Properties the
 * dashboard and store rely on:
 *
 *   - lexicographic ORDER BY of a flat span set = depth-first execution order
 *     (tree rendering with zero recursive joins);
 *   - segment count = depth;
 *   - the last segment's uuid IS the run_id, the first segment's uuid IS the
 *     trace root's run_id. One string locates a span in its tree.
 *
 * JS Date gives millisecond precision; the final 3 microsecond digits are
 * zero-padded. Sibling spans created inside the same millisecond keep a
 * stable (uuid) tiebreak, which is fine for rendering.
 */

const SEGMENT_UUID_LEN = 36;

// Timestamp layout: YYYYMMDD (8) + "T" (1) + HHMMSS (6) + microseconds (6) +
// "Z" (1) = 22 chars, ie. strftime("%Y%m%dT%H%M%S%fZ") width.
const TIME_CHARS = 22;

function pad(n: number, width: number): string {
  return String(n).padStart(width, "0");
}

/** `20260704T081500123000Z`. UTC, microsecond field padded out from ms. */
export function dottedOrderTimestamp(time: Date, microSeq = 0): string {
  const micros = (time.getUTCMilliseconds() * 1000 + (microSeq % 1000)).toString().padStart(6, "0");
  return (
    `${pad(time.getUTCFullYear(), 4)}${pad(time.getUTCMonth() + 1, 2)}${pad(time.getUTCDate(), 2)}` +
    `T${pad(time.getUTCHours(), 2)}${pad(time.getUTCMinutes(), 2)}${pad(time.getUTCSeconds(), 2)}` +
    `${micros}Z`
  );
}

// JS clocks are millisecond-precision, so siblings created inside the same ms
// would tie on timestamp and sort by random uuid. The sub-ms digits therefore
// carry a per-process monotonic sequence. Real time survives to the ms, and
// same-ms siblings keep creation order (LangSmith gets this for free from
// microsecond clocks).
let microSeq = 0;

/** One segment: timestamp + run uuid. */
export function dottedOrderSegment(startTime: Date, runId: string): string {
  microSeq = (microSeq + 1) % 1000;
  return `${dottedOrderTimestamp(startTime, microSeq)}${runId}`;
}

/** Child dotted order under an (optional) parent dotted order. */
export function childDottedOrder(
  parentDottedOrder: string | null | undefined,
  startTime: Date,
  runId: string,
): string {
  const segment = dottedOrderSegment(startTime, runId);
  return parentDottedOrder ? `${parentDottedOrder}.${segment}` : segment;
}

/** Depth of a span (root = 1). */
export function dottedOrderDepth(dottedOrder: string): number {
  return dottedOrder.length === 0 ? 0 : dottedOrder.split(".").length;
}

export interface DottedOrderSegment {
  time: string;
  runId: string;
}

/** Parse segments; returns [] for a malformed string rather than throwing. */
export function parseDottedOrder(dottedOrder: string): DottedOrderSegment[] {
  if (!dottedOrder) return [];
  const parts = dottedOrder.split(".");
  const out: DottedOrderSegment[] = [];
  for (const part of parts) {
    if (part.length !== TIME_CHARS + SEGMENT_UUID_LEN) return [];
    out.push({
      time: part.slice(0, TIME_CHARS),
      runId: part.slice(TIME_CHARS),
    });
  }
  return out;
}

// Exported for tests.
export const DOTTED_ORDER_SEGMENT_LEN = TIME_CHARS + SEGMENT_UUID_LEN;
export { TIME_CHARS as DOTTED_ORDER_TIME_CHARS };
