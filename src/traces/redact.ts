/**
 * Payload capping and redaction for span inputs/outputs, applied BEFORE a row
 * is enqueued (client-side masking is the only acceptable posture for a shared
 * that reads tenants' email and documents; mirrors LangSmith's
 * hide_inputs/anonymizer hooks).
 *
 * Two layers:
 *   1. key redaction: any object key that looks like a credential is
 *      replaced with "[redacted]", recursively;
 *   2. byte cap: the serialised payload is capped at maxBytes, and an over-cap
 *      payload is replaced by `{ __truncated: true, bytes, preview }` so the
 *      dashboard can say "6.2 MB result, first 2 KB shown" honestly.
 */

import { DEFAULT_PAYLOAD_CAP_BYTES } from "./types.js";

/** Case-insensitive substrings that mark a key as credential-bearing. */
const REDACT_KEY_PATTERNS = [
  "authorization",
  "api_key",
  "apikey",
  "x-api-key",
  "token",
  "password",
  "secret",
  "cookie",
  "connection_string",
  "conn_string",
] as const;

const REDACTED = "[redacted]";
const PREVIEW_BYTES = 2 * 1024;
const MAX_DEPTH = 32;

export function isRedactableKey(key: string): boolean {
  const k = key.toLowerCase();
  return REDACT_KEY_PATTERNS.some((p) => k.includes(p));
}

/** Recursively replace credential-looking keys. Never throws; cycles cut at depth. */
export function redactSensitiveKeys(value: unknown, depth = 0): unknown {
  if (depth > MAX_DEPTH || value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map((v) => redactSensitiveKeys(v, depth + 1));
  const out: Record<string, unknown> = {};
  for (const [key, v] of Object.entries(value as Record<string, unknown>)) {
    out[key] = isRedactableKey(key) ? REDACTED : redactSensitiveKeys(v, depth + 1);
  }
  return out;
}

export interface CappedPayload {
  __truncated: true;
  bytes: number;
  preview: string;
}

/**
 * Redact + cap a payload for storage. Returns a JSON-serializable value that
 * is guaranteed to fit in maxBytes once serialized (up to the small envelope
 * of the truncation marker). Unserializable inputs degrade to a marker rather
 * rather than throwing. A span write must never break the code it's tracing.
 */
export function capPayload(
  value: unknown,
  maxBytes: number = DEFAULT_PAYLOAD_CAP_BYTES,
): unknown | null {
  if (value === undefined || value === null) return null;
  let redacted: unknown;
  try {
    redacted = redactSensitiveKeys(value);
  } catch {
    redacted = REDACTED;
  }
  let serialized: string;
  try {
    serialized = JSON.stringify(redacted) ?? "null";
  } catch {
    return { __truncated: true, bytes: 0, preview: "[unserializable]" } satisfies CappedPayload;
  }
  const bytes = Buffer.byteLength(serialized, "utf8");
  if (bytes <= maxBytes) return redacted;
  return {
    __truncated: true,
    bytes,
    preview: serialized.slice(0, PREVIEW_BYTES),
  } satisfies CappedPayload;
}
