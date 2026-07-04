-- ---------------------------------------------------------------------------
-- pipeline schema, durable pipeline-run state.
--
-- Why this schema exists:
--   Multi-step pipelines had no durable execution: state lived in ad-hoc
--   per-process files, a crash meant restarting from step 0, and there was no
--   way to PARK a run waiting for human approval and resume it later. The
--   runner in src/ is the step engine; these tables are its checkpointer:
--
--     pipeline.runs        current state (checkpoint) + status per run
--     pipeline.steps       per-step completion history (time travel / audit)
--     pipeline.interrupts  parked human-in-the-loop approvals, a run in
--                          status 'waiting_approval' resumes when its pending
--                          interrupt row is approved or rejected, by a UI or
--                          by any process with database access
--
--   Crash-resume semantic: state is checkpointed after every completed step,
--   so resumePipeline(runId) re-executes only the failed/unstarted step -
--   completed steps are never re-run, so side effects need only be idempotent
--   at step granularity.
--
-- Connection: PIPELINE_PG_CONN (falls back to APP_STATE_PG_CONN).
--
-- Idempotent. Safe to re-run.
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS pipeline;

COMMENT ON SCHEMA pipeline IS
  'Durable pipeline execution: checkpointed run state, per-step history and '
  'parked human-approval interrupts.';

-- ---------------------------------------------------------------------------
-- pipeline.runs, one row per pipeline run; `state` is the latest checkpoint.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.runs (
  run_id           UUID PRIMARY KEY,
  pipeline         TEXT NOT NULL,
  pipeline_version TEXT,
  agent_id         TEXT,
  status           TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'running', 'waiting_approval', 'completed', 'failed', 'cancelled'
  )),
  -- Latest checkpoint (size-capped by the runner, default 256 KB). JSONB so
  -- the dashboard's state viewer can render diffs without deserialising blobs.
  state            JSONB NOT NULL DEFAULT '{}'::jsonb,
  current_step     TEXT,
  step_index       INTEGER NOT NULL DEFAULT 0,
  error            TEXT,
  trace_id         UUID,            -- obs.runs linkage (pipeline span tree)
  metadata         JSONB,
  resume_count     INTEGER NOT NULL DEFAULT 0,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  started_at       TIMESTAMPTZ,
  completed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS pipeline_runs_status_idx
  ON pipeline.runs (status, updated_at DESC);
CREATE INDEX IF NOT EXISTS pipeline_runs_pipeline_idx
  ON pipeline.runs (pipeline, created_at DESC);
CREATE INDEX IF NOT EXISTS pipeline_runs_agent_idx
  ON pipeline.runs (agent_id, created_at DESC)
  WHERE agent_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- pipeline.steps, append-only per-step attempt history.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.steps (
  id           BIGSERIAL PRIMARY KEY,
  run_id       UUID NOT NULL REFERENCES pipeline.runs (run_id) ON DELETE CASCADE,
  step_name    TEXT NOT NULL,
  step_index   INTEGER NOT NULL,
  attempt      INTEGER NOT NULL DEFAULT 1,
  status       TEXT NOT NULL CHECK (status IN (
    'running', 'ok', 'error', 'interrupted', 'skipped', 'compensated'
  )),
  -- State AFTER this step completed (the checkpoint it produced). NULL for
  -- non-ok attempts; capped like pipeline.runs.state.
  state_after  JSONB,
  error        TEXT,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  latency_ms   INTEGER
);

CREATE INDEX IF NOT EXISTS pipeline_steps_run_idx
  ON pipeline.steps (run_id, step_index, attempt);

-- ---------------------------------------------------------------------------
-- pipeline.interrupts, parked approvals. A pending row parks its run
-- indefinitely (status 'waiting_approval'); resolution becomes the
-- interrupt's return value inside the step on resume.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pipeline.interrupts (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_id       UUID NOT NULL REFERENCES pipeline.runs (run_id) ON DELETE CASCADE,
  step_name    TEXT NOT NULL,
  reason       TEXT NOT NULL,
  payload      JSONB,
  status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
    'pending', 'approved', 'rejected', 'expired'
  )),
  requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at  TIMESTAMPTZ,
  resolved_by  TEXT,               -- operator identity (Entra email), for audit
  resolution   JSONB,              -- value handed back to the paused step
  expires_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS pipeline_interrupts_pending_idx
  ON pipeline.interrupts (requested_at DESC)
  WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS pipeline_interrupts_run_idx
  ON pipeline.interrupts (run_id);

-- ---------------------------------------------------------------------------
-- Grants, same app_role bridge as the other schemas here.
-- UPDATE everywhere (status transitions); no DELETE (runs are an audit
-- ledger; CASCADE cleanup is an admin operation).
-- ---------------------------------------------------------------------------

SELECT set_config('app.role', :'app_role', false);

DO $$
DECLARE
  app_role TEXT := COALESCE(current_setting('app.role', true), 'cron_service');
BEGIN
  IF app_role IS NULL OR app_role = '' THEN
    RAISE NOTICE 'pipeline.app_role not set and no default, skipping grants.';
    RETURN;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app_role) THEN
    EXECUTE format('GRANT USAGE ON SCHEMA pipeline TO %I', app_role);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON pipeline.runs TO %I', app_role);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON pipeline.steps TO %I', app_role);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE ON pipeline.interrupts TO %I', app_role);
    EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE pipeline.steps_id_seq TO %I', app_role);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT SELECT, INSERT, UPDATE ON TABLES TO %I',
      app_role
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT USAGE, SELECT ON SEQUENCES TO %I',
      app_role
    );
    RAISE NOTICE 'Granted pipeline.* read/write to %', app_role;
  ELSE
    RAISE NOTICE 'Role % does not exist yet, skipping grants. Create it first, then re-run.', app_role;
  END IF;
END $$;
