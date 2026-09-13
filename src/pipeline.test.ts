import { afterEach, describe, expect, it } from "vitest";

import { InMemoryTraceSink, configureTraceSink, resetTraceSinkForTests } from "./traces/sink.js";
import {
  InMemoryPipelineStore,
  PipelineRejectedError,
  StepTimeoutError,
  definePipeline,
  resumePipeline,
  startPipeline,
} from "./index.js";

interface State {
  [key: string]: unknown;
  items: string[];
  approved?: boolean;
}

const deps = { sleep: async () => undefined, random: () => 0 };

afterEach(async () => {
  await resetTraceSinkForTests();
});

describe("pipeline runner", () => {
  it("runs steps in order, checkpointing state after every step", async () => {
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-happy",
      version: "1",
      steps: [
        { name: "collect", run: (s) => ({ ...s, items: [...s.items, "a"] }) },
        { name: "enrich", run: (s) => ({ ...s, items: [...s.items, "b"] }) },
        { name: "finish", run: () => undefined }, // void = state unchanged
      ],
    });
    const outcome = await startPipeline(pipeline, {
      store,
      initialState: { items: [] },
      agentId: "worker-1",
      ...deps,
    });
    expect(outcome.status).toBe("completed");
    expect(outcome.state.items).toEqual(["a", "b"]);

    const run = await store.loadRun(outcome.runId);
    expect(run?.status).toBe("completed");
    expect(run?.step_index).toBe(3);
    // Per-step checkpoint history (time travel/audit).
    expect(store.steps.map((s) => [s.step_name, s.status])).toEqual([
      ["collect", "ok"],
      ["enrich", "ok"],
      ["finish", "ok"],
    ]);
    expect(store.steps[0].state_after).toEqual({ items: ["a"] });
  });

  it("crash-resume re-executes only the failed step - completed steps never re-run", async () => {
    const store = new InMemoryPipelineStore();
    const executed: string[] = [];
    let crashOnce = true;
    const pipeline = definePipeline<State>({
      name: "test-resume",
      steps: [
        {
          name: "one",
          run: (s) => {
            executed.push("one");
            return { ...s, items: [...s.items, "one"] };
          },
        },
        {
          name: "two",
          run: (s) => {
            executed.push("two");
            if (crashOnce) {
              crashOnce = false;
              throw new Error("simulated crash");
            }
            return { ...s, items: [...s.items, "two"] };
          },
        },
        {
          name: "three",
          run: (s) => {
            executed.push("three");
            return { ...s, items: [...s.items, "three"] };
          },
        },
      ],
    });

    const first = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    expect(first.status).toBe("failed");
    expect(first.error).toContain("simulated crash");

    const second = await resumePipeline<State>(pipeline, first.runId, { store, ...deps });
    expect(second.status).toBe("completed");
    expect(second.state.items).toEqual(["one", "two", "three"]);
    // "one" ran exactly once across both drives.
    expect(executed).toEqual(["one", "two", "two", "three"]);
    expect((await store.loadRun(first.runId))?.resume_count).toBe(1);
  });

  it("parks on interrupt, resolves the operator's value on approval, and resumes", async () => {
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-hitl",
      steps: [
        { name: "draft", run: (s) => ({ ...s, items: ["draft"] }) },
        {
          name: "approval",
          run: async (s, ctx) => {
            const resolution = (await ctx.interrupt("send-external-email", {
              to: "client@example.com",
            })) as { note: string };
            return { ...s, approved: true, items: [...s.items, `approved:${resolution.note}`] };
          },
        },
        { name: "send", run: (s) => ({ ...s, items: [...s.items, "sent"] }) },
      ],
    });

    const parked = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    expect(parked.status).toBe("waiting_approval");
    expect(parked.pendingInterruptId).toBeDefined();
    expect((await store.loadRun(parked.runId))?.status).toBe("waiting_approval");

    // Days later, from any process: operator approves with a resolution value.
    const pending = await store.listPendingInterrupts(parked.runId);
    expect(pending).toHaveLength(1);
    expect(pending[0].payload).toEqual({ to: "client@example.com" });
    await store.resolveInterrupt(pending[0].id, {
      status: "approved",
      resolvedBy: "operator@example.com",
      value: { note: "ok to send" },
    });

    const resumed = await resumePipeline<State>(pipeline, parked.runId, { store, ...deps });
    expect(resumed.status).toBe("completed");
    expect(resumed.state.items).toEqual(["draft", "approved:ok to send", "sent"]);
    // Approval is deterministic on re-execution: the interrupt row stays
    // approved, so a crash-resume of the same step gets the same value.
    const again = await resumePipeline<State>(pipeline, parked.runId, { store, ...deps });
    expect(again.status).toBe("completed");
  });

  it("fails the run when the operator rejects the interrupt", async () => {
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-reject",
      steps: [
        {
          name: "approval",
          run: async (s, ctx) => {
            await ctx.interrupt("risky-action");
            return s;
          },
        },
      ],
    });
    const parked = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    const [pending] = await store.listPendingInterrupts(parked.runId);
    await store.resolveInterrupt(pending.id, { status: "rejected", resolvedBy: "marc" });

    const resumed = await resumePipeline<State>(pipeline, parked.runId, { store, ...deps });
    expect(resumed.status).toBe("failed");
    expect(resumed.error).toContain("rejected");
    // The rejection is typed for in-step handling too.
    expect(new PipelineRejectedError("id", "r").name).toBe("PipelineRejectedError");
  });

  it("applies retry policy with backoff and typed retryOn", async () => {
    const store = new InMemoryPipelineStore();
    const delays: number[] = [];
    let failures = 2;
    const pipeline = definePipeline<State>({
      name: "test-retry",
      steps: [
        {
          name: "flaky",
          retry: { maxAttempts: 3, initialDelayMs: 100, backoffFactor: 3, jitter: 0 },
          run: (s) => {
            if (failures > 0) {
              failures -= 1;
              throw new Error("upstream 503");
            }
            return { ...s, items: ["done"] };
          },
        },
      ],
    });
    const outcome = await startPipeline(pipeline, {
      store,
      initialState: { items: [] },
      sleep: async (ms) => {
        delays.push(ms);
      },
      random: () => 0,
    });
    expect(outcome.status).toBe("completed");
    expect(delays).toEqual([100, 300]); // exponential, no jitter
    expect(store.steps.filter((s) => s.step_name === "flaky")).toHaveLength(3); // 2 errors + 1 ok

    // Non-retryable predicate stops immediately.
    const store2 = new InMemoryPipelineStore();
    const pipeline2 = definePipeline<State>({
      name: "test-retry-typed",
      steps: [
        {
          name: "fatal",
          retry: { maxAttempts: 3, retryOn: (err) => !(err instanceof TypeError) },
          run: () => {
            throw new TypeError("bad input");
          },
        },
      ],
    });
    const out2 = await startPipeline(pipeline2, {
      store: store2,
      initialState: { items: [] },
      ...deps,
    });
    expect(out2.status).toBe("failed");
    expect(store2.steps).toHaveLength(1); // no retries on TypeError
  });

  it("enforces per-attempt timeouts (retryable) and skip/compensate policies", async () => {
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-policies",
      steps: [
        {
          name: "slow",
          timeoutMs: 20,
          onError: "skip",
          run: () => new Promise<State | void>((r) => setTimeout(() => r(undefined), 5_000)),
        },
        {
          name: "broken-but-compensated",
          onError: (state, err) => ({
            ...state,
            items: [...state.items, `compensated:${(err as Error).message}`],
          }),
          run: () => {
            throw new Error("hard failure");
          },
        },
        { name: "final", run: (s) => ({ ...s, items: [...s.items, "final"] }) },
      ],
    });
    const outcome = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    expect(outcome.status).toBe("completed");
    expect(outcome.state.items).toEqual(["compensated:hard failure", "final"]);
    const slow = store.steps.filter((s) => s.step_name === "slow");
    expect(slow[0].status).toBe("error");
    expect(slow[0].error).toContain("timed out");
    expect(new StepTimeoutError("x", 1).name).toBe("StepTimeoutError");
    const compensated = store.steps.find((s) => s.status === "compensated");
    expect(compensated?.step_name).toBe("broken-but-compensated");
  });

  it("skips steps via skipIf and validates definitions", async () => {
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-skip",
      steps: [
        { name: "always", run: (s) => ({ ...s, items: ["x"] }) },
        { name: "conditional", skipIf: (s) => s.items.includes("x"), run: (s) => s },
      ],
    });
    const outcome = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    expect(outcome.status).toBe("completed");
    expect(store.steps.find((s) => s.step_name === "conditional")?.status).toBe("skipped");

    expect(() =>
      definePipeline<State>({
        name: "dup",
        steps: [
          { name: "a", run: (s) => s },
          { name: "a", run: (s) => s },
        ],
      }),
    ).toThrow("duplicate step name");
    expect(() => definePipeline<State>({ name: "empty", steps: [] })).toThrow("no steps");
  });

  it("refuses to resume across a pipeline version drift", async () => {
    const store = new InMemoryPipelineStore();
    const v1 = definePipeline<State>({
      name: "test-drift",
      version: "v1",
      steps: [
        {
          name: "park",
          run: async (s, ctx) => {
            await ctx.interrupt("gate");
            return s;
          },
        },
      ],
    });
    const parked = await startPipeline(v1, { store, initialState: { items: [] }, ...deps });
    const v2 = definePipeline<State>({ ...v1, version: "v2" });
    await expect(resumePipeline<State>(v2, parked.runId, { store, ...deps })).rejects.toThrow(
      "version drift",
    );
  });

  it("emits pipeline + step spans into the trace sink", async () => {
    const sink = new InMemoryTraceSink();
    configureTraceSink(sink);
    const store = new InMemoryPipelineStore();
    const pipeline = definePipeline<State>({
      name: "test-traced",
      steps: [
        {
          name: "work",
          run: (s, ctx) => {
            ctx.emit("progress", { pct: 50 });
            return { ...s, items: ["done"] };
          },
        },
      ],
    });
    const outcome = await startPipeline(pipeline, { store, initialState: { items: [] }, ...deps });
    const ordered = sink.ordered;
    expect(ordered.map((r) => `${r.run_type}:${r.name}`)).toEqual([
      "pipeline:pipeline:test-traced",
      "step:step:work",
      "chain:event:progress",
    ]);
    expect(ordered[0].session_key).toBe(`pipeline:${outcome.runId}`);
    expect((await store.loadRun(outcome.runId))?.trace_id).toBe(ordered[0].trace_id);
  });
});

describe("checkpoint atomicity", () => {
  /**
   * Wraps the in-memory store and records which store methods the runner calls,
   * so a test can assert HOW the checkpoint is written rather than only what it
   * ends up saying.
   */
  class RecordingStore extends InMemoryPipelineStore {
    readonly calls: string[] = [];
    failCommitOnIndex: number | null = null;
    /** commitStep delegates to recordStep internally; only log the outer call. */
    private inCommit = false;

    override async recordStep(step: Parameters<InMemoryPipelineStore["recordStep"]>[0]) {
      if (!this.inCommit) this.calls.push(`recordStep:${step.status}:${step.step_index}`);
      return super.recordStep(step);
    }

    override async commitStep(
      step: Parameters<InMemoryPipelineStore["commitStep"]>[0],
      patch: Parameters<InMemoryPipelineStore["commitStep"]>[1],
    ) {
      if (this.failCommitOnIndex === step.step_index) {
        // Stand in for the process dying mid-write. Neither half may land.
        this.calls.push(`commitStep:CRASH:${step.step_index}`);
        throw new Error("connection terminated");
      }
      this.calls.push(`commitStep:${step.status}:${step.step_index}`);
      this.inCommit = true;
      try {
        return await super.commitStep(step, patch);
      } finally {
        this.inCommit = false;
      }
    }
  }

  const twoSteps = (ran: string[]) =>
    definePipeline<State>({
      name: "atomic",
      version: "1",
      steps: [
        {
          name: "one",
          run: (s) => {
            ran.push("one");
            return { ...s, items: [...s.items, "one"] };
          },
        },
        {
          name: "two",
          run: (s) => {
            ran.push("two");
            return { ...s, items: [...s.items, "two"] };
          },
        },
      ],
    });

  it("writes a completed step and its cursor advance as a single store call", async () => {
    // The gap this closes: recordStep("ok") followed by a separate updateRun
    // left a window where a crash between them meant the step read as done
    // while the cursor still pointed at it, so resume re-ran a side effect
    // that had already happened.
    const store = new RecordingStore();
    const ran: string[] = [];
    await startPipeline(twoSteps(ran), { store, initialState: { items: [] }, ...deps });

    expect(store.calls).toEqual(["commitStep:ok:0", "commitStep:ok:1"]);
    expect(store.calls.some((c) => c.startsWith("recordStep:ok"))).toBe(false);
  });

  it("leaves no half-applied checkpoint when the commit itself fails", async () => {
    const store = new RecordingStore();
    store.failCommitOnIndex = 0;
    const ran: string[] = [];

    const outcome = await startPipeline(twoSteps(ran), {
      store,
      initialState: { items: [] },
      ...deps,
    });

    expect(outcome.status).toBe("failed");
    // Step one's side effect ran, but nothing about it was recorded, so the
    // stored state is consistent: cursor still at 0, no "ok" row at 0.
    const run = await store.loadRun(outcome.runId);
    expect(run?.step_index).toBe(0);
    expect(store.steps.filter((s) => s.run_id === outcome.runId && s.status === "ok")).toHaveLength(
      0,
    );
    // And step two never started, because step one never committed.
    expect(ran).toEqual(["one"]);
  });
});
