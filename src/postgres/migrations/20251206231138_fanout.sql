-- Add support for spawning subtasks from within tasks (fan-out pattern).
-- This migration adds:
--   1. parent_task_id column to track parent-child relationships
--   2. Modified spawn_task to accept parent_task_id
--   3. Modified complete_run to emit child completion events
--   4. Modified fail_run to emit events and cascade cancel children
--   5. cascade_cancel_children function for recursive cancellation
--   6. Modified cancel_task to cascade cancel and emit events

-- =============================================================================
-- 1. Modify ensure_queue_tables to add parent_task_id column
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.ensure_queue_tables (p_queue_name text)
  RETURNS void
  LANGUAGE plpgsql
AS $$
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS durable.%I (
        task_id uuid PRIMARY KEY,
        task_name text NOT NULL,
        params jsonb NOT NULL,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        parent_task_id uuid,
        enqueue_at timestamptz NOT NULL DEFAULT durable.current_time(),
        first_started_at timestamptz,
        state text NOT NULL CHECK (state IN (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer NOT NULL DEFAULT 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz
     ) WITH (fillfactor=70)',
    't_' || p_queue_name
  );

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS durable.%I (
        run_id uuid PRIMARY KEY,
        task_id uuid NOT NULL,
        attempt integer NOT NULL,
        state text NOT NULL CHECK (state IN (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        claimed_by text,
        claim_expires_at timestamptz,
        available_at timestamptz NOT NULL,
        wake_event text,
        event_payload jsonb,
        started_at timestamptz,
        completed_at timestamptz,
        failed_at timestamptz,
        result jsonb,
        failure_reason jsonb,
        created_at timestamptz NOT NULL DEFAULT durable.current_time()
     ) WITH (fillfactor=70)',
    'r_' || p_queue_name
  );

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS durable.%I (
        task_id uuid NOT NULL,
        checkpoint_name text NOT NULL,
        state jsonb,
        status text NOT NULL DEFAULT ''committed'',
        owner_run_id uuid,
        updated_at timestamptz NOT NULL DEFAULT durable.current_time(),
        PRIMARY KEY (task_id, checkpoint_name)
     ) WITH (fillfactor=70)',
    'c_' || p_queue_name
  );

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS durable.%I (
        event_name text PRIMARY KEY,
        payload jsonb,
        emitted_at timestamptz NOT NULL DEFAULT durable.current_time()
     )',
    'e_' || p_queue_name
  );

  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS durable.%I (
        task_id uuid NOT NULL,
        run_id uuid NOT NULL,
        step_name text NOT NULL,
        event_name text NOT NULL,
        timeout_at timestamptz,
        created_at timestamptz NOT NULL DEFAULT durable.current_time(),
        PRIMARY KEY (run_id, step_name)
     )',
    'w_' || p_queue_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON durable.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON durable.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON durable.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  -- Index for finding children of a parent task (for cascade cancellation)
  EXECUTE format(
    'CREATE INDEX IF NOT EXISTS %I ON durable.%I (parent_task_id) WHERE parent_task_id IS NOT NULL',
    ('t_' || p_queue_name) || '_pti',
    't_' || p_queue_name
  );
END;
$$;

-- =============================================================================
-- 2. Modify spawn_task to accept parent_task_id from options
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb DEFAULT '{}'::jsonb
)
  RETURNS TABLE (
    task_id uuid,
    run_id uuid,
    attempt integer
  )
  LANGUAGE plpgsql
AS $$
DECLARE
  v_task_id uuid := durable.portable_uuidv7();
  v_run_id uuid := durable.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_parent_task_id uuid;
  v_now timestamptz := durable.current_time();
  v_params jsonb := COALESCE(p_params, 'null'::jsonb);
BEGIN
  IF p_task_name IS NULL OR length(trim(p_task_name)) = 0 THEN
    RAISE EXCEPTION 'task_name must be provided';
  END IF;

  IF p_options IS NOT NULL THEN
    v_headers := p_options->'headers';
    v_retry_strategy := p_options->'retry_strategy';
    IF p_options ? 'max_attempts' THEN
      v_max_attempts := (p_options->>'max_attempts')::int;
      IF v_max_attempts IS NOT NULL AND v_max_attempts < 1 THEN
        RAISE EXCEPTION 'max_attempts must be >= 1';
      END IF;
    END IF;
    v_cancellation := p_options->'cancellation';
    -- Extract parent_task_id for subtask tracking
    v_parent_task_id := (p_options->>'parent_task_id')::uuid;
  END IF;

  EXECUTE format(
    'INSERT INTO durable.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, parent_task_id, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, ''pending'', $10, $11, NULL, NULL)',
    't_' || p_queue_name
  )
  USING v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_parent_task_id, v_now, v_attempt, v_run_id;

  EXECUTE format(
    'INSERT INTO durable.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     VALUES ($1, $2, $3, ''pending'', $4, NULL, NULL, NULL, NULL)',
    'r_' || p_queue_name
  )
  USING v_run_id, v_task_id, v_attempt, v_now;

  RETURN QUERY SELECT v_task_id, v_run_id, v_attempt;
END;
$$;

-- =============================================================================
-- 3. Modify complete_run to emit child completion event
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.complete_run (
  p_queue_name text,
  p_run_id uuid,
  p_state jsonb DEFAULT NULL
)
  RETURNS void
  LANGUAGE plpgsql
AS $$
DECLARE
  v_task_id uuid;
  v_state text;
  v_parent_task_id uuid;
  v_now timestamptz := durable.current_time();
BEGIN
  EXECUTE format(
    'SELECT task_id, state
       FROM durable.%I
      WHERE run_id = $1
      FOR UPDATE',
    'r_' || p_queue_name
  )
  INTO v_task_id, v_state
  USING p_run_id;

  IF v_task_id IS NULL THEN
    RAISE EXCEPTION 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  END IF;

  IF v_state <> 'running' THEN
    RAISE EXCEPTION 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  END IF;

  EXECUTE format(
    'UPDATE durable.%I
        SET state = ''completed'',
            completed_at = $2,
            result = $3
      WHERE run_id = $1',
    'r_' || p_queue_name
  ) USING p_run_id, v_now, p_state;

  -- Get parent_task_id to check if this is a subtask
  EXECUTE format(
    'UPDATE durable.%I
        SET state = ''completed'',
            completed_payload = $2,
            last_attempt_run = $3
      WHERE task_id = $1
      RETURNING parent_task_id',
    't_' || p_queue_name
  )
  INTO v_parent_task_id
  USING v_task_id, p_state, p_run_id;

  EXECUTE format(
    'DELETE FROM durable.%I WHERE run_id = $1',
    'w_' || p_queue_name
  ) USING p_run_id;

  -- Emit completion event for parent to join on (only if this is a subtask)
  IF v_parent_task_id IS NOT NULL THEN
    PERFORM durable.emit_event(
      p_queue_name,
      '$child:' || v_task_id::text,
      jsonb_build_object('status', 'completed', 'result', p_state)
    );
  END IF;
END;
$$;

-- =============================================================================
-- 4. Add cascade_cancel_children function (must be defined before fail_run)
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.cascade_cancel_children (
  p_queue_name text,
  p_parent_task_id uuid
)
  RETURNS void
  LANGUAGE plpgsql
AS $$
DECLARE
  v_child_id uuid;
  v_child_state text;
  v_now timestamptz := durable.current_time();
BEGIN
  -- Find all children of this parent that are not in terminal state
  FOR v_child_id, v_child_state IN
    EXECUTE format(
      'SELECT task_id, state
         FROM durable.%I
        WHERE parent_task_id = $1
          AND state NOT IN (''completed'', ''failed'', ''cancelled'')
        FOR UPDATE',
      't_' || p_queue_name
    )
    USING p_parent_task_id
  LOOP
    -- Cancel the child task
    EXECUTE format(
      'UPDATE durable.%I
          SET state = ''cancelled'',
              cancelled_at = COALESCE(cancelled_at, $2)
        WHERE task_id = $1',
      't_' || p_queue_name
    ) USING v_child_id, v_now;

    -- Cancel all runs of this child
    EXECUTE format(
      'UPDATE durable.%I
          SET state = ''cancelled'',
              claimed_by = NULL,
              claim_expires_at = NULL
        WHERE task_id = $1
          AND state NOT IN (''completed'', ''failed'', ''cancelled'')',
      'r_' || p_queue_name
    ) USING v_child_id;

    -- Delete wait registrations
    EXECUTE format(
      'DELETE FROM durable.%I WHERE task_id = $1',
      'w_' || p_queue_name
    ) USING v_child_id;

    -- Emit cancellation event so parent's join() can receive it
    PERFORM durable.emit_event(
      p_queue_name,
      '$child:' || v_child_id::text,
      jsonb_build_object('status', 'cancelled')
    );

    -- Recursively cancel grandchildren
    PERFORM durable.cascade_cancel_children(p_queue_name, v_child_id);
  END LOOP;
END;
$$;

-- =============================================================================
-- 5. Modify fail_run to emit event and cascade cancel children
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.fail_run (
  p_queue_name text,
  p_run_id uuid,
  p_reason jsonb,
  p_retry_at timestamptz DEFAULT NULL
)
  RETURNS void
  LANGUAGE plpgsql
AS $$
DECLARE
  v_task_id uuid;
  v_attempt integer;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_now timestamptz := durable.current_time();
  v_next_attempt integer;
  v_delay_seconds double precision := 0;
  v_next_available timestamptz;
  v_retry_kind text;
  v_base double precision;
  v_factor double precision;
  v_max_seconds double precision;
  v_first_started timestamptz;
  v_cancellation jsonb;
  v_max_duration bigint;
  v_task_state text;
  v_task_cancel boolean := false;
  v_new_run_id uuid;
  v_task_state_after text;
  v_recorded_attempt integer;
  v_last_attempt_run uuid := p_run_id;
  v_cancelled_at timestamptz := NULL;
  v_parent_task_id uuid;
BEGIN
  EXECUTE format(
    'SELECT r.task_id, r.attempt
       FROM durable.%I r
      WHERE r.run_id = $1
        AND r.state IN (''running'', ''sleeping'')
      FOR UPDATE',
    'r_' || p_queue_name
  )
  INTO v_task_id, v_attempt
  USING p_run_id;

  IF v_task_id IS NULL THEN
    RAISE EXCEPTION 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  END IF;

  EXECUTE format(
    'SELECT retry_strategy, max_attempts, first_started_at, cancellation, state, parent_task_id
       FROM durable.%I
      WHERE task_id = $1
      FOR UPDATE',
    't_' || p_queue_name
  )
  INTO v_retry_strategy, v_max_attempts, v_first_started, v_cancellation, v_task_state, v_parent_task_id
  USING v_task_id;

  EXECUTE format(
    'UPDATE durable.%I
        SET state = ''failed'',
            wake_event = NULL,
            failed_at = $2,
            failure_reason = $3
      WHERE run_id = $1',
    'r_' || p_queue_name
  ) USING p_run_id, v_now, p_reason;

  v_next_attempt := v_attempt + 1;
  v_task_state_after := 'failed';
  v_recorded_attempt := v_attempt;

  IF v_max_attempts IS NULL OR v_next_attempt <= v_max_attempts THEN
    IF p_retry_at IS NOT NULL THEN
      v_next_available := p_retry_at;
    ELSE
      v_retry_kind := COALESCE(v_retry_strategy->>'kind', 'none');
      IF v_retry_kind = 'fixed' THEN
        v_base := COALESCE((v_retry_strategy->>'base_seconds')::double precision, 60);
        v_delay_seconds := v_base;
      ELSIF v_retry_kind = 'exponential' THEN
        v_base := COALESCE((v_retry_strategy->>'base_seconds')::double precision, 30);
        v_factor := COALESCE((v_retry_strategy->>'factor')::double precision, 2);
        v_delay_seconds := v_base * power(v_factor, greatest(v_attempt - 1, 0));
        v_max_seconds := (v_retry_strategy->>'max_seconds')::double precision;
        IF v_max_seconds IS NOT NULL THEN
          v_delay_seconds := least(v_delay_seconds, v_max_seconds);
        END IF;
      ELSE
        v_delay_seconds := 0;
      END IF;
      v_next_available := v_now + (v_delay_seconds * interval '1 second');
    END IF;

    IF v_next_available < v_now THEN
      v_next_available := v_now;
    END IF;

    IF v_cancellation IS NOT NULL THEN
      v_max_duration := (v_cancellation->>'max_duration')::bigint;
      IF v_max_duration IS NOT NULL AND v_first_started IS NOT NULL THEN
        IF extract(epoch FROM (v_next_available - v_first_started)) >= v_max_duration THEN
          v_task_cancel := true;
        END IF;
      END IF;
    END IF;

    IF NOT v_task_cancel THEN
      v_task_state_after := CASE WHEN v_next_available > v_now THEN 'sleeping' ELSE 'pending' END;
      v_new_run_id := durable.portable_uuidv7();
      v_recorded_attempt := v_next_attempt;
      v_last_attempt_run := v_new_run_id;
      EXECUTE format(
        'INSERT INTO durable.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
         VALUES ($1, $2, $3, %L, $4, NULL, NULL, NULL, NULL)',
        'r_' || p_queue_name,
        v_task_state_after
      )
      USING v_new_run_id, v_task_id, v_next_attempt, v_next_available;
    END IF;
  END IF;

  IF v_task_cancel THEN
    v_task_state_after := 'cancelled';
    v_cancelled_at := v_now;
    v_recorded_attempt := greatest(v_recorded_attempt, v_attempt);
    v_last_attempt_run := p_run_id;
  END IF;

  EXECUTE format(
    'UPDATE durable.%I
        SET state = %L,
            attempts = greatest(attempts, $3),
            last_attempt_run = $4,
            cancelled_at = COALESCE(cancelled_at, $5)
      WHERE task_id = $1',
    't_' || p_queue_name,
    v_task_state_after
  ) USING v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  EXECUTE format(
    'DELETE FROM durable.%I WHERE run_id = $1',
    'w_' || p_queue_name
  ) USING p_run_id;

  -- If task reached terminal failure state (failed or cancelled), emit event and cascade cancel
  IF v_task_state_after IN ('failed', 'cancelled') THEN
    -- Cascade cancel all children
    PERFORM durable.cascade_cancel_children(p_queue_name, v_task_id);

    -- Emit completion event for parent to join on (only if this is a subtask)
    IF v_parent_task_id IS NOT NULL THEN
      PERFORM durable.emit_event(
        p_queue_name,
        '$child:' || v_task_id::text,
        jsonb_build_object('status', v_task_state_after, 'error', p_reason)
      );
    END IF;
  END IF;
END;
$$;

-- =============================================================================
-- 6. Modify cancel_task to cascade cancel and emit event
-- =============================================================================

CREATE OR REPLACE FUNCTION durable.cancel_task (
  p_queue_name text,
  p_task_id uuid
)
  RETURNS void
  LANGUAGE plpgsql
AS $$
DECLARE
  v_now timestamptz := durable.current_time();
  v_task_state text;
  v_parent_task_id uuid;
BEGIN
  EXECUTE format(
    'SELECT state, parent_task_id
       FROM durable.%I
      WHERE task_id = $1
      FOR UPDATE',
    't_' || p_queue_name
  )
  INTO v_task_state, v_parent_task_id
  USING p_task_id;

  IF v_task_state IS NULL THEN
    RAISE EXCEPTION 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  END IF;

  IF v_task_state IN ('completed', 'failed', 'cancelled') THEN
    RETURN;
  END IF;

  EXECUTE format(
    'UPDATE durable.%I
        SET state = ''cancelled'',
            cancelled_at = COALESCE(cancelled_at, $2)
      WHERE task_id = $1',
    't_' || p_queue_name
  ) USING p_task_id, v_now;

  EXECUTE format(
    'UPDATE durable.%I
        SET state = ''cancelled'',
            claimed_by = NULL,
            claim_expires_at = NULL
      WHERE task_id = $1
        AND state NOT IN (''completed'', ''failed'', ''cancelled'')',
    'r_' || p_queue_name
  ) USING p_task_id;

  EXECUTE format(
    'DELETE FROM durable.%I WHERE task_id = $1',
    'w_' || p_queue_name
  ) USING p_task_id;

  -- Cascade cancel all children
  PERFORM durable.cascade_cancel_children(p_queue_name, p_task_id);

  -- Emit cancellation event for parent to join on (only if this is a subtask)
  IF v_parent_task_id IS NOT NULL THEN
    PERFORM durable.emit_event(
      p_queue_name,
      '$child:' || p_task_id::text,
      jsonb_build_object('status', 'cancelled')
    );
  END IF;
END;
$$;
