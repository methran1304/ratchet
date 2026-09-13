/**
 * Checkpointer: pipeline.runs / pipeline.steps / pipeline.interrupts
 * (schema/pipeline-db-init.sql; keep column lists in lockstep).
 *
 * UNLIKE the telemetry sinks, this store is LOAD-BEARING: checkpoint writes
 * are awaited and failures propagate. Losing a checkpoint quietly would
 * corrupt the resume semantic the engine exists to provide.
 */

import { randomUUID } from "node:crypto";

import {
  MAX_STATE_BYTES,
  PIPELINE_PG_CONN_ENV,
  PIPELINE_PG_CONN_FALLBACK_ENV,
  type InterruptRecord,
  type InterruptStatus,
  type PipelineRunRecord,
  type PipelineRunStatus,
  type PipelineState,
  type StepRecord,
} from "./types.js";

export interface PipelineStore {
  createRun(run: PipelineRunRecord): Promise<void>;
  loadRun(runId: string): Promise<PipelineRunRecord | null>;
  updateRun(
    runId: string,
    patch: Partial<
      Pick<
        PipelineRunRecord,
        | "status"
        | "state"
        | "current_step"
        | "step_index"
        | "error"
        | "trace_id"
        | "resume_count"
        | "started_at"
        | "completed_at"
      >
    >,
  ): Promise<void>;
  recordStep(step: StepRecord): Promise<void>;
  /**
   * Record a completed step AND advance the run cursor as one durable write.
   *
   * These were two separate queries, which left a window: crash after the step
   * row said "ok" but before step_index moved, and resume re-ran a step whose
   * side effect had already happened. That is the exact thing a checkpointing
   * library exists to prevent, so it has to be one statement.
   */
  commitStep(
    step: StepRecord,
    runPatch: Parameters<PipelineStore["updateRun"]>[1],
  ): Promise<void>;
  createInterrupt(interrupt: InterruptRecord): Promise<void>;
  /** Latest interrupt for (runId, stepName, reason), any status. */
  findInterrupt(runId: string, stepName: string, reason: string): Promise<InterruptRecord | null>;
  resolveInterrupt(
    id: string,
    resolution: {
      status: Extract<InterruptStatus, "approved" | "rejected">;
      resolvedBy: string;
      value?: unknown;
    },
  ): Promise<InterruptRecord | null>;
  listPendingInterrupts(runId?: string): Promise<InterruptRecord[]>;
}

export function assertStateSize(state: PipelineState): string {
  const serialized = JSON.stringify(state) ?? "{}";
  const bytes = Buffer.byteLength(serialized, "utf8");
  if (bytes > MAX_STATE_BYTES) {
    throw new Error(
      `pipeline state is ${bytes} bytes, over the ${MAX_STATE_BYTES}-byte checkpoint cap. ` +
        "Keep large artefacts in blob/workspace storage and reference them from state.",
    );
  }
  return serialized;
}

// ---------------------------------------------------------------------------
// In-memory store (tests + ephemeral runs)
// ---------------------------------------------------------------------------

export class InMemoryPipelineStore implements PipelineStore {
  readonly runs = new Map<string, PipelineRunRecord>();
  readonly steps: StepRecord[] = [];
  readonly interrupts = new Map<string, InterruptRecord>();

  async createRun(run: PipelineRunRecord): Promise<void> {
    assertStateSize(run.state);
    this.runs.set(run.run_id, { ...run });
  }

  async loadRun(runId: string): Promise<PipelineRunRecord | null> {
    const run = this.runs.get(runId);
    return run ? { ...run } : null;
  }

  async updateRun(runId: string, patch: Parameters<PipelineStore["updateRun"]>[1]): Promise<void> {
    const run = this.runs.get(runId);
    if (!run) throw new Error(`pipeline run not found: ${runId}`);
    if (patch.state) assertStateSize(patch.state);
    Object.assign(run, patch, { updated_at: new Date() });
  }

  async recordStep(step: StepRecord): Promise<void> {
    if (step.state_after) assertStateSize(step.state_after);
    this.steps.push({ ...step });
  }

  async commitStep(
    step: StepRecord,
    runPatch: Parameters<PipelineStore["updateRun"]>[1],
  ): Promise<void> {
    // No crash window to close in-process: nothing can interleave between these
    // two synchronous mutations.
    await this.recordStep(step);
    await this.updateRun(step.run_id, runPatch);
  }

  async createInterrupt(interrupt: InterruptRecord): Promise<void> {
    this.interrupts.set(interrupt.id, { ...interrupt });
  }

  async findInterrupt(
    runId: string,
    stepName: string,
    reason: string,
  ): Promise<InterruptRecord | null> {
    const matches = [...this.interrupts.values()]
      .filter((i) => i.run_id === runId && i.step_name === stepName && i.reason === reason)
      .sort((a, b) => b.requested_at.getTime() - a.requested_at.getTime());
    return matches[0] ? { ...matches[0] } : null;
  }

  async resolveInterrupt(
    id: string,
    resolution: Parameters<PipelineStore["resolveInterrupt"]>[1],
  ): Promise<InterruptRecord | null> {
    const interrupt = this.interrupts.get(id);
    if (!interrupt || interrupt.status !== "pending") return interrupt ? { ...interrupt } : null;
    interrupt.status = resolution.status;
    interrupt.resolved_at = new Date();
    interrupt.resolved_by = resolution.resolvedBy;
    interrupt.resolution = resolution.value ?? null;
    return { ...interrupt };
  }

  async listPendingInterrupts(runId?: string): Promise<InterruptRecord[]> {
    return [...this.interrupts.values()]
      .filter((i) => i.status === "pending" && (!runId || i.run_id === runId))
      .map((i) => ({ ...i }));
  }
}

// ---------------------------------------------------------------------------
// Postgres store
// ---------------------------------------------------------------------------

interface PgClientLike {
  query(text: string, values?: unknown[]): Promise<{ rows: Array<Record<string, unknown>> }>;
}

interface PgPoolLike extends PgClientLike {
  end(): Promise<void>;
}

function rowToRun(r: Record<string, unknown>): PipelineRunRecord {
  return {
    run_id: String(r.run_id),
    pipeline: String(r.pipeline),
    pipeline_version: r.pipeline_version == null ? null : String(r.pipeline_version),
    agent_id: r.agent_id == null ? null : String(r.agent_id),
    status: String(r.status) as PipelineRunStatus,
    state: (r.state ?? {}) as PipelineState,
    current_step: r.current_step == null ? null : String(r.current_step),
    step_index: Number(r.step_index ?? 0),
    error: r.error == null ? null : String(r.error),
    trace_id: r.trace_id == null ? null : String(r.trace_id),
    metadata: (r.metadata ?? null) as Record<string, unknown> | null,
    resume_count: Number(r.resume_count ?? 0),
    created_at: new Date(String(r.created_at)),
    updated_at: new Date(String(r.updated_at)),
    started_at: r.started_at == null ? null : new Date(String(r.started_at)),
    completed_at: r.completed_at == null ? null : new Date(String(r.completed_at)),
  };
}

function rowToInterrupt(r: Record<string, unknown>): InterruptRecord {
  return {
    id: String(r.id),
    run_id: String(r.run_id),
    step_name: String(r.step_name),
    reason: String(r.reason),
    payload: r.payload ?? null,
    status: String(r.status) as InterruptStatus,
    requested_at: new Date(String(r.requested_at)),
    resolved_at: r.resolved_at == null ? null : new Date(String(r.resolved_at)),
    resolved_by: r.resolved_by == null ? null : String(r.resolved_by),
    resolution: r.resolution ?? null,
    expires_at: r.expires_at == null ? null : new Date(String(r.expires_at)),
  };
}

export class PgPipelineStore implements PipelineStore {
  private readonly connectionString: string;
  private readonly poolFactory: (conn: string) => Promise<PgPoolLike>;
  private poolPromise: Promise<PgPoolLike> | null = null;

  constructor(connectionString: string, poolFactory?: (conn: string) => Promise<PgPoolLike>) {
    this.connectionString = connectionString;
    this.poolFactory =
      poolFactory ??
      (async (conn) => {
        const { default: pg } = await import("pg");
        return new pg.Pool({ connectionString: conn, max: 2 }) as unknown as PgPoolLike;
      });
  }

  private async pool(): Promise<PgPoolLike> {
    if (!this.poolPromise) this.poolPromise = this.poolFactory(this.connectionString);
    return this.poolPromise;
  }

  async createRun(run: PipelineRunRecord): Promise<void> {
    const state = assertStateSize(run.state);
    const pool = await this.pool();
    await pool.query(
      `INSERT INTO pipeline.runs
         (run_id, pipeline, pipeline_version, agent_id, status, state, current_step,
          step_index, error, trace_id, metadata, resume_count, created_at, updated_at,
          started_at, completed_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
      [
        run.run_id,
        run.pipeline,
        run.pipeline_version,
        run.agent_id,
        run.status,
        state,
        run.current_step,
        run.step_index,
        run.error,
        run.trace_id,
        run.metadata === null ? null : JSON.stringify(run.metadata),
        run.resume_count,
        run.created_at,
        run.updated_at,
        run.started_at,
        run.completed_at,
      ],
    );
  }

  async loadRun(runId: string): Promise<PipelineRunRecord | null> {
    const pool = await this.pool();
    const res = await pool.query(`SELECT * FROM pipeline.runs WHERE run_id = $1`, [runId]);
    return res.rows[0] ? rowToRun(res.rows[0]) : null;
  }

  /**
   * Build the SET clause for a run patch. `values` starts with the run id at $1
   * and is appended to, so a caller can keep binding after it (see commitStep).
   */
  private runPatchSets(
    patch: Parameters<PipelineStore["updateRun"]>[1],
    values: unknown[],
  ): string[] {
    const sets: string[] = ["updated_at = NOW()"];
    const push = (fragment: string, value: unknown) => {
      values.push(value);
      sets.push(`${fragment} $${values.length}`);
    };
    if (patch.status !== undefined) push("status =", patch.status);
    if (patch.state !== undefined) push("state =", assertStateSize(patch.state));
    if (patch.current_step !== undefined) push("current_step =", patch.current_step);
    if (patch.step_index !== undefined) push("step_index =", patch.step_index);
    if (patch.error !== undefined) push("error =", patch.error);
    if (patch.trace_id !== undefined) push("trace_id =", patch.trace_id);
    if (patch.resume_count !== undefined) push("resume_count =", patch.resume_count);
    if (patch.started_at !== undefined) push("started_at =", patch.started_at);
    if (patch.completed_at !== undefined) push("completed_at =", patch.completed_at);
    return sets;
  }

  async updateRun(runId: string, patch: Parameters<PipelineStore["updateRun"]>[1]): Promise<void> {
    const values: unknown[] = [runId];
    const sets = this.runPatchSets(patch, values);
    const pool = await this.pool();
    await pool.query(`UPDATE pipeline.runs SET ${sets.join(", ")} WHERE run_id = $1`, values);
  }

  /**
   * One statement, so the step row and the cursor advance commit together.
   *
   * A CTE is enough here: Postgres runs a single statement in its own implicit
   * transaction, so there is no partial outcome to resume from. Doing it as two
   * queries left a window where a crash in between meant the step read as "ok"
   * while step_index still pointed at it, and resume re-ran a side effect that
   * had already happened.
   */
  async commitStep(
    step: StepRecord,
    patch: Parameters<PipelineStore["updateRun"]>[1],
  ): Promise<void> {
    const values: unknown[] = [step.run_id];
    const sets = this.runPatchSets(patch, values);
    const stepParams: string[] = [];
    for (const v of [
      step.step_name,
      step.step_index,
      step.attempt,
      step.status,
      step.state_after === null ? null : assertStateSize(step.state_after),
      step.error,
      step.started_at,
      step.completed_at,
      step.latency_ms,
    ]) {
      values.push(v);
      stepParams.push(`$${values.length}`);
    }
    const pool = await this.pool();
    await pool.query(
      `WITH recorded AS (
         INSERT INTO pipeline.steps
           (run_id, step_name, step_index, attempt, status, state_after, error,
            started_at, completed_at, latency_ms)
         VALUES ($1, ${stepParams.join(", ")})
       )
       UPDATE pipeline.runs SET ${sets.join(", ")} WHERE run_id = $1`,
      values,
    );
  }

  async recordStep(step: StepRecord): Promise<void> {
    const pool = await this.pool();
    await pool.query(
      `INSERT INTO pipeline.steps
         (run_id, step_name, step_index, attempt, status, state_after, error,
          started_at, completed_at, latency_ms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        step.run_id,
        step.step_name,
        step.step_index,
        step.attempt,
        step.status,
        step.state_after === null ? null : assertStateSize(step.state_after),
        step.error,
        step.started_at,
        step.completed_at,
        step.latency_ms,
      ],
    );
  }

  async createInterrupt(interrupt: InterruptRecord): Promise<void> {
    const pool = await this.pool();
    await pool.query(
      `INSERT INTO pipeline.interrupts
         (id, run_id, step_name, reason, payload, status, requested_at, expires_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [
        interrupt.id,
        interrupt.run_id,
        interrupt.step_name,
        interrupt.reason,
        interrupt.payload === null ? null : JSON.stringify(interrupt.payload),
        interrupt.status,
        interrupt.requested_at,
        interrupt.expires_at,
      ],
    );
  }

  async findInterrupt(
    runId: string,
    stepName: string,
    reason: string,
  ): Promise<InterruptRecord | null> {
    const pool = await this.pool();
    const res = await pool.query(
      `SELECT * FROM pipeline.interrupts
        WHERE run_id = $1 AND step_name = $2 AND reason = $3
        ORDER BY requested_at DESC LIMIT 1`,
      [runId, stepName, reason],
    );
    return res.rows[0] ? rowToInterrupt(res.rows[0]) : null;
  }

  async resolveInterrupt(
    id: string,
    resolution: Parameters<PipelineStore["resolveInterrupt"]>[1],
  ): Promise<InterruptRecord | null> {
    const pool = await this.pool();
    // Status-guarded so two operators cannot double-resolve.
    const res = await pool.query(
      `UPDATE pipeline.interrupts
          SET status = $2, resolved_at = NOW(), resolved_by = $3, resolution = $4
        WHERE id = $1 AND status = 'pending'
        RETURNING *`,
      [
        id,
        resolution.status,
        resolution.resolvedBy,
        resolution.value === undefined ? null : JSON.stringify(resolution.value),
      ],
    );
    return res.rows[0] ? rowToInterrupt(res.rows[0]) : null;
  }

  async listPendingInterrupts(runId?: string): Promise<InterruptRecord[]> {
    const pool = await this.pool();
    const res = runId
      ? await pool.query(
          `SELECT * FROM pipeline.interrupts WHERE status = 'pending' AND run_id = $1
            ORDER BY requested_at DESC`,
          [runId],
        )
      : await pool.query(
          `SELECT * FROM pipeline.interrupts WHERE status = 'pending'
            ORDER BY requested_at DESC`,
        );
    return res.rows.map(rowToInterrupt);
  }

  async close(): Promise<void> {
    if (this.poolPromise) {
      const pool = await this.poolPromise.catch(() => null);
      this.poolPromise = null;
      await pool?.end().catch(() => undefined);
    }
  }
}

/** Env-driven store resolution (PIPELINE_PG_CONN → APP_STATE_PG_CONN). */
export function resolvePipelineStore(env: NodeJS.ProcessEnv = process.env): PgPipelineStore | null {
  const conn = env[PIPELINE_PG_CONN_ENV]?.trim() || env[PIPELINE_PG_CONN_FALLBACK_ENV]?.trim();
  return conn ? new PgPipelineStore(conn) : null;
}

/** Fresh interrupt row helper (shared by runner + operator surfaces). */
export function newInterruptRecord(input: {
  runId: string;
  stepName: string;
  reason: string;
  payload?: unknown;
  expiresAt?: Date | null;
}): InterruptRecord {
  return {
    id: randomUUID(),
    run_id: input.runId,
    step_name: input.stepName,
    reason: input.reason,
    payload: input.payload ?? null,
    status: "pending",
    requested_at: new Date(),
    resolved_at: null,
    resolved_by: null,
    resolution: null,
    expires_at: input.expiresAt ?? null,
  };
}
