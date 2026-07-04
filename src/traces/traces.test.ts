import { afterEach, describe, expect, it } from "vitest";

import {
  DOTTED_ORDER_SEGMENT_LEN,
  childDottedOrder,
  dottedOrderDepth,
  dottedOrderSegment,
  dottedOrderTimestamp,
  parseDottedOrder,
} from "./dotted-order.js";
import { capPayload, redactSensitiveKeys } from "./redact.js";
import {
  InMemoryTraceSink,
  buildRunsUpsertSql,
  configureTraceSink,
  resetTraceSinkForTests,
  runRowParams,
} from "./sink.js";
import { RunHandle, currentRun, startRun, withSpan } from "./tracer.js";

const UUID = "01234567-89ab-cdef-0123-456789abcdef";

afterEach(async () => {
  await resetTraceSinkForTests();
  delete process.env.OBS_TRACES_SAMPLE_RATE;
});

describe("dotted-order", () => {
  it("encodes the LangSmith timestamp layout (22 chars, microsecond padding)", () => {
    const ts = dottedOrderTimestamp(new Date(Date.UTC(2026, 6, 4, 8, 15, 0, 123)));
    expect(ts).toBe("20260704T081500123000Z");
    expect(ts).toHaveLength(22);
  });

  it("sorts lexicographically into depth-first execution order", () => {
    const t0 = new Date(Date.UTC(2026, 6, 4, 8, 0, 0, 0));
    const t1 = new Date(Date.UTC(2026, 6, 4, 8, 0, 1, 0));
    const t2 = new Date(Date.UTC(2026, 6, 4, 8, 0, 2, 0));
    const root = childDottedOrder(null, t0, "aaaaaaaa-0000-0000-0000-000000000000");
    const child1 = childDottedOrder(root, t1, "bbbbbbbb-0000-0000-0000-000000000000");
    const child2 = childDottedOrder(root, t2, "cccccccc-0000-0000-0000-000000000000");
    const sorted = [child2, root, child1].sort();
    expect(sorted).toEqual([root, child1, child2]);
    expect(dottedOrderDepth(root)).toBe(1);
    expect(dottedOrderDepth(child2)).toBe(2);
  });

  it("round-trips segments through parse", () => {
    const t = new Date(Date.UTC(2026, 0, 2, 3, 4, 5, 6));
    const segment = dottedOrderSegment(t, UUID);
    expect(segment).toHaveLength(DOTTED_ORDER_SEGMENT_LEN);
    const parsed = parseDottedOrder(`${segment}.${dottedOrderSegment(t, UUID)}`);
    expect(parsed).toHaveLength(2);
    expect(parsed[1].runId).toBe(UUID);
  });

  it("returns [] for malformed strings instead of throwing", () => {
    expect(parseDottedOrder("not-a-dotted-order")).toEqual([]);
    expect(parseDottedOrder("")).toEqual([]);
  });
});

describe("redact", () => {
  it("redacts credential-looking keys recursively", () => {
    const out = redactSensitiveKeys({
      body: "hello",
      headers: { Authorization: "Bearer xyz", "X-Api-Key": "k" },
      nested: [{ password: "p", ok: 1 }],
    }) as Record<string, unknown>;
    expect(out.body).toBe("hello");
    expect((out.headers as Record<string, unknown>).Authorization).toBe("[redacted]");
    expect((out.headers as Record<string, unknown>)["X-Api-Key"]).toBe("[redacted]");
    expect((out.nested as Array<Record<string, unknown>>)[0].password).toBe("[redacted]");
    expect((out.nested as Array<Record<string, unknown>>)[0].ok).toBe(1);
  });

  it("caps oversized payloads with an honest truncation marker", () => {
    const big = { text: "x".repeat(64 * 1024) };
    const capped = capPayload(big, 1024) as {
      __truncated: boolean;
      bytes: number;
      preview: string;
    };
    expect(capped.__truncated).toBe(true);
    expect(capped.bytes).toBeGreaterThan(64 * 1024);
    expect(capped.preview.length).toBeLessThanOrEqual(2 * 1024);
    // Under-cap payloads pass through (redacted but unwrapped).
    expect(capPayload({ a: 1 }, 1024)).toEqual({ a: 1 });
    expect(capPayload(undefined)).toBeNull();
  });
});

describe("tracer", () => {
  it("builds a parent/child tree with correct dotted_order and emits start+end snapshots", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);

    const root = startRun({
      name: "triage",
      runType: "turn",
      source: "mail-engine",
      agentId: "worker-1",
      sessionKey: "mail-triage:inbox",
      inputs: { scope: "inbox" },
    });
    await root.run(async () => {
      await withSpan({ name: "llm-1", runType: "llm" }, async (span) => {
        span.setModelUsage({
          model: "some-model-v1",
          tokens: {
            input_tokens: 1000,
            output_tokens: 50,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 4000,
          },
          cost: 0.0123,
        });
      });
      await withSpan({ name: "send", runType: "tool" }, async () => undefined);
    });

    const ordered = sink.ordered;
    expect(ordered.map((r) => r.name)).toEqual(["triage", "llm-1", "send"]);
    expect(ordered[0].status).toBe("ok");
    expect(ordered[1].parent_run_id).toBe(root.runId);
    expect(ordered[1].trace_id).toBe(root.traceId);
    expect(ordered[1].dotted_order.startsWith(`${root.dottedOrder}.`)).toBe(true);
    // Cost is caller-supplied - the tracer stores it, it never prices a call.
    expect(ordered[1].cost).toBe(0.0123);
    expect(ordered[1].cache_read_input_tokens).toBe(4000);
    expect(ordered[2].latency_ms).toBeGreaterThanOrEqual(0);
    // session/agent attribution flows to children.
    expect(ordered[2].agent_id).toBe("worker-1");
    expect(ordered[2].session_key).toBe("mail-triage:inbox");
  });

  it("marks failures with status=error and rethrows", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    const root = startRun({
      name: "boom",
      runType: "chain",
      source: "mcp",
      agentId: "worker-2",
    });
    await expect(
      root.run(async () => {
        throw new Error("kaboom");
      }),
    ).rejects.toThrow("kaboom");
    expect(sink.runs[0].status).toBe("error");
    expect(sink.runs[0].error).toContain("kaboom");
  });

  it("honours root-level sampling - unsampled roots emit nothing and children follow", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    process.env.OBS_TRACES_SAMPLE_RATE = "0";
    const root = startRun({ name: "r", runType: "turn", source: "gateway", agentId: "worker-3" });
    await root.run(async () => {
      await withSpan({ name: "child", runType: "tool" }, async () => undefined);
    });
    expect(root.sampled).toBe(false);
    expect(sink.runs).toHaveLength(0);
  });

  it("withSpan without ambient parent or orphan spec runs on an inert handle", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    expect(currentRun()).toBeUndefined();
    const result = await withSpan({ name: "loose", runType: "tool" }, async (span) => {
      expect(span.sampled).toBe(false);
      return 42;
    });
    expect(result).toBe(42);
    expect(sink.runs).toHaveLength(0);
  });

  it("redacts credentials in span inputs", () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    const run = startRun({
      name: "tool",
      runType: "tool",
      source: "mcp",
      agentId: "worker-4",
      inputs: { api_key: "sk-123", q: "ok" },
    });
    run.end();
    const inputs = sink.runs[0].inputs as Record<string, unknown>;
    expect(inputs.api_key).toBe("[redacted]");
    expect(inputs.q).toBe("ok");
  });

  it("attaches feedback rows to the span's trace", () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    const run = startRun({ name: "r", runType: "turn", source: "eval", agentId: "worker-1" });
    run.feedback({ key: "correctness", score: 1, source: "model", comment: "matches" });
    run.end();
    expect(sink.feedback).toHaveLength(1);
    expect(sink.feedback[0].trace_id).toBe(run.traceId);
    expect(sink.feedback[0].source).toBe("model");
  });

  it("inert handles never emit and produce inert children", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    const inert = RunHandle.inert();
    const child = inert.child({ name: "c", runType: "tool" });
    child.end();
    inert.end();
    expect(sink.runs).toHaveLength(0);
  });
});

describe("pg sink SQL", () => {
  it("builds a multi-row upsert with the full column list", () => {
    const sql = buildRunsUpsertSql(2);
    expect(sql).toContain("INSERT INTO obs.runs");
    expect(sql).toContain("ON CONFLICT (run_id) DO UPDATE SET");
    expect(sql).toContain("($1,");
    expect(sql).toContain("($27,"); // 26 columns per row
    expect(sql).toContain("status = EXCLUDED.status");
    expect(sql).toContain("end_time = EXCLUDED.end_time");
    // start-immutable identity fields are not updated
    expect(sql).not.toContain("dotted_order = EXCLUDED.dotted_order");
  });

  it("serialises jsonb params and passes arrays through", () => {
    const run = startRun({
      name: "n",
      runType: "llm",
      source: "gateway",
      agentId: "worker-5",
      tags: ["cron"],
      metadata: { cron_job: "morning-brief" },
    });
    run.end();
    const params = runRowParams(run.snapshot());
    expect(params).toHaveLength(26);
    expect(typeof params[13]).toBe("string"); // metadata serialized
    expect(Array.isArray(params[14])).toBe(true); // tags as pg array
  });
});
