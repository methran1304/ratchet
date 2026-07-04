-- ---------------------------------------------------------------------------
-- obs schema, run/span trees for pipeline and application tracing.
--
-- Why this schema exists:
--   Flat per-call metering tells you what something cost but not what it did.
--   Without a span TREE, tool spans, parent/child structure, per-run
--   inputs/outputs, debugging a multi-step run means reading process logs by
--   hand. obs.runs is that tree; obs.feedback attaches quality or operator
--   scores to any span (automated evaluators write source='model', humans
--   write source='app').
--
-- Every write is fire-and-forget and must never block the work being traced.
--
-- dotted_order is the encoding that makes this cheap to read: one
-- `<%Y%m%dT%H%M%S%fZ><uuid>` segment per ancestor joined with '.', so
-- `ORDER BY dotted_order` on a flat SELECT yields depth-first execution
-- order, a UI rebuilds the tree with zero recursive joins. Segment count is
-- the depth; the last segment's uuid is the run_id.
--
-- Retention: prune obs.runs + obs.feedback on your own window (the reference
-- deployment uses 90 days). Runs whose trace carries feedback are worth
-- exempting from pruning, those are the ones somebody looked at.
--
-- Idempotent. Safe to re-run.
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS obs;

COMMENT ON SCHEMA obs IS
  'Observability: run/span trees (obs.runs) + span-attached feedback '
  '(obs.feedback).';

-- ---------------------------------------------------------------------------
-- obs.runs, one row per span. Spans INSERT at start (status=running) and are
-- finalised via INSERT ... ON CONFLICT (run_id) DO UPDATE from the same
-- emitter (end patch), so a crashed process leaves an honest `running` row.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS obs.runs (
  run_id                      UUID PRIMARY KEY,
  trace_id                    UUID NOT NULL,
  parent_run_id               UUID,
  dotted_order                TEXT NOT NULL,
  agent_id                    TEXT NOT NULL,
  session_key                 TEXT,
  source                      TEXT NOT NULL,  -- free text; e.g. pipeline | scheduler | mcp | eval | backfill
  run_type                    TEXT NOT NULL,  -- turn | llm | tool | chain | pipeline | step
  name                        TEXT NOT NULL,
  status                      TEXT NOT NULL DEFAULT 'running'
                              CHECK (status IN ('running', 'ok', 'error')),
  error                       TEXT,
  -- Size-capped + redacted BEFORE write (see src/traces/redact.ts, default
  -- 16 KB per side). Spans on sensitive lanes may carry NO payloads at all -
  -- tool params and results are usually the most sensitive free text around.
  inputs                      JSONB,
  outputs                     JSONB,
  metadata                    JSONB,
  tags                        TEXT[],
  model                       TEXT,
  provider                    TEXT,
  input_tokens                INTEGER,
  output_tokens               INTEGER,
  cache_creation_input_tokens INTEGER,
  cache_read_input_tokens     INTEGER,
  cost                        NUMERIC(10, 6),
  start_time                  TIMESTAMPTZ NOT NULL,
  end_time                    TIMESTAMPTZ,
  latency_ms                  INTEGER,
  -- Golden-trace / dataset example id when this run was produced by an
  -- agent-eval experiment (evals.results.run_id points back the other way).
  reference_example_id        TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN obs.runs.dotted_order IS
  'LangSmith-style materialised path: per-ancestor <start_time %Y%m%dT%H%M%S%fZ><uuid> segments joined by ".". Lexicographic order = depth-first execution order.';

CREATE INDEX IF NOT EXISTS obs_runs_start_idx        ON obs.runs (start_time DESC);
CREATE INDEX IF NOT EXISTS obs_runs_agent_start_idx  ON obs.runs (agent_id, start_time DESC);
CREATE INDEX IF NOT EXISTS obs_runs_trace_idx        ON obs.runs (trace_id, dotted_order);
CREATE INDEX IF NOT EXISTS obs_runs_type_start_idx   ON obs.runs (run_type, start_time DESC);
CREATE INDEX IF NOT EXISTS obs_runs_session_idx      ON obs.runs (session_key, start_time DESC)
  WHERE session_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS obs_runs_error_start_idx  ON obs.runs (start_time DESC)
  WHERE status = 'error';
CREATE INDEX IF NOT EXISTS obs_runs_example_idx      ON obs.runs (reference_example_id)
  WHERE reference_example_id IS NOT NULL;
-- Roots-only listing (/traces default view) without scanning children.
CREATE INDEX IF NOT EXISTS obs_runs_roots_idx        ON obs.runs (start_time DESC)
  WHERE parent_run_id IS NULL;
-- Recovery analysis ("was there a later SUCCESSFUL tool call in this session
-- after the failed one?") is a common query shape. obs_runs_session_idx covers
-- session_key but not run_type/status, so that lookup degraded to a per-row
-- scan of every span in the session. This partial index targets the exact
-- filter shape (a session's successful tool spans only).
CREATE INDEX IF NOT EXISTS obs_runs_session_tool_ok_idx ON obs.runs (session_key, start_time DESC)
  WHERE run_type = 'tool' AND status = 'ok';

-- ---------------------------------------------------------------------------
-- obs.feedback, scores/comments attached to any span. source follows the
-- LangSmith taxonomy: 'api' (programmatic), 'model' (LLM judge / online
-- evaluator), 'app' (human via a dashboard).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS obs.feedback (
  id          BIGSERIAL PRIMARY KEY,
  run_id      UUID NOT NULL,
  trace_id    UUID NOT NULL,
  key         TEXT NOT NULL,
  score       NUMERIC,
  value       TEXT,
  comment     TEXT,
  correction  JSONB,
  source      TEXT NOT NULL CHECK (source IN ('api', 'model', 'app')),
  created_by  TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS obs_feedback_run_idx   ON obs.feedback (run_id);
CREATE INDEX IF NOT EXISTS obs_feedback_trace_idx ON obs.feedback (trace_id);
CREATE INDEX IF NOT EXISTS obs_feedback_key_idx   ON obs.feedback (key, created_at DESC);

-- ---------------------------------------------------------------------------
-- Grants, same app_role bridge as the other schemas here. UPDATE on obs.runs
-- is required for the end-patch upsert; DELETE on both tables is required by
-- the scheduler retention pass.
-- ---------------------------------------------------------------------------

SELECT set_config('app.role', :'app_role', false);

DO $$
DECLARE
  app_role TEXT := COALESCE(current_setting('app.role', true), 'cron_service');
BEGIN
  IF app_role IS NULL OR app_role = '' THEN
    RAISE NOTICE 'obs.app_role not set and no default, skipping grants.';
    RETURN;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app_role) THEN
    EXECUTE format('GRANT USAGE ON SCHEMA obs TO %I', app_role);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON obs.runs TO %I', app_role);
    EXECUTE format('GRANT SELECT, INSERT, DELETE ON obs.feedback TO %I', app_role);
    EXECUTE format('GRANT USAGE, SELECT ON SEQUENCE obs.feedback_id_seq TO %I', app_role);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA obs GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I',
      app_role
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA obs GRANT USAGE, SELECT ON SEQUENCES TO %I',
      app_role
    );
    RAISE NOTICE 'Granted obs.runs + obs.feedback read/write to %', app_role;
  ELSE
    RAISE NOTICE 'Role % does not exist yet, skipping grants. Create it first, then re-run.', app_role;
  END IF;
END $$;
