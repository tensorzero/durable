-- Add indexes to reduce claim_task scans and update ensure_queue_tables to include them.

create or replace function durable.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  execute format(
    'create table if not exists durable.%I (
        task_id uuid primary key,
        task_name text not null,
        params jsonb not null,
        headers jsonb,
        retry_strategy jsonb,
        max_attempts integer,
        cancellation jsonb,
        parent_task_id uuid,
        enqueue_at timestamptz not null default durable.current_time(),
        first_started_at timestamptz,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        attempts integer not null default 0,
        last_attempt_run uuid,
        completed_payload jsonb,
        cancelled_at timestamptz
     ) with (fillfactor=70)',
    't_' || p_queue_name
  );

  execute format('comment on column durable.%I.params is %L', 't_' || p_queue_name, 'User-defined. Task input parameters. Schema depends on Task::Params type.');
  execute format('comment on column durable.%I.headers is %L', 't_' || p_queue_name, 'User-defined. Optional key-value metadata as {"key": <any JSON value>}.');
  execute format('comment on column durable.%I.retry_strategy is %L', 't_' || p_queue_name, '{"kind": "none"} | {"kind": "fixed", "base_seconds": <u64>} | {"kind": "exponential", "base_seconds": <u64>, "factor": <f64>, "max_seconds": <u64>}');
  execute format('comment on column durable.%I.cancellation is %L', 't_' || p_queue_name, '{"max_delay": <seconds>, "max_duration": <seconds>} - both optional. max_delay: cancel if not started within N seconds of enqueue. max_duration: cancel if not completed within N seconds of first start.');
  execute format('comment on column durable.%I.completed_payload is %L', 't_' || p_queue_name, 'User-defined. Task return value. Schema depends on Task::Output type.');

  execute format(
    'create table if not exists durable.%I (
        run_id uuid primary key,
        task_id uuid not null,
        attempt integer not null,
        state text not null check (state in (''pending'', ''running'', ''sleeping'', ''completed'', ''failed'', ''cancelled'')),
        claimed_by text,
        claim_expires_at timestamptz,
        available_at timestamptz not null,
        wake_event text,
        event_payload jsonb,
        started_at timestamptz,
        completed_at timestamptz,
        failed_at timestamptz,
        result jsonb,
        failure_reason jsonb,
        created_at timestamptz not null default durable.current_time()
     ) with (fillfactor=70)',
    'r_' || p_queue_name
  );

  execute format('comment on column durable.%I.wake_event is %L', 'r_' || p_queue_name, 'Event name this run is waiting for while sleeping. Set by await_event when suspending, cleared when the event fires or timeout expires.');
  execute format('comment on column durable.%I.event_payload is %L', 'r_' || p_queue_name, 'Payload delivered by emit_event when waking this run. Consumed by await_event on the next claim to return the value to the caller.');
  execute format('comment on column durable.%I.result is %L', 'r_' || p_queue_name, 'User-defined. Serialized task output. Schema depends on Task::Output type.');
  execute format('comment on column durable.%I.failure_reason is %L', 'r_' || p_queue_name, '{"name": "<error type>", "message": "<string>", "backtrace": "<string>"}');

  execute format(
    'create table if not exists durable.%I (
        task_id uuid not null,
        checkpoint_name text not null,
        state jsonb,
        owner_run_id uuid,
        updated_at timestamptz not null default durable.current_time(),
        primary key (task_id, checkpoint_name)
     ) with (fillfactor=70)',
    'c_' || p_queue_name
  );

  execute format('comment on column durable.%I.state is %L', 'c_' || p_queue_name, 'User-defined. Checkpoint value from ctx.step(). Any JSON-serializable value.');

  execute format(
    'create table if not exists durable.%I (
        event_name text primary key,
        payload jsonb,
        emitted_at timestamptz not null default durable.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format('comment on column durable.%I.payload is %L', 'e_' || p_queue_name, 'User-defined. Event payload. Internal child events use: {"status": "completed"|"failed"|"cancelled", "result"?: <json>, "error"?: <json>}');

  execute format(
    'create table if not exists durable.%I (
        task_id uuid not null,
        run_id uuid not null,
        step_name text not null,
        event_name text not null,
        timeout_at timestamptz,
        created_at timestamptz not null default durable.current_time(),
        primary key (run_id, step_name)
     )',
    'w_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on durable.%I (state, available_at)',
    ('r_' || p_queue_name) || '_sai',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on durable.%I (task_id)',
    ('r_' || p_queue_name) || '_ti',
    'r_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on durable.%I (event_name)',
    ('w_' || p_queue_name) || '_eni',
    'w_' || p_queue_name
  );

  -- Index for finding children of a parent task (for cascade cancellation)
  execute format(
    'create index if not exists %I on durable.%I (parent_task_id) where parent_task_id is not null',
    ('t_' || p_queue_name) || '_pti',
    't_' || p_queue_name
  );

  -- Speed up claim timeout scans.
  execute format(
    'create index if not exists %I on durable.%I (claim_expires_at)
     where state = ''running'' and claim_expires_at is not null',
    ('r_' || p_queue_name) || '_cei',
    'r_' || p_queue_name
  );

  -- Speed up cancellation and cancelled-task sync scans.
  execute format(
    'create index if not exists %I on durable.%I (state)
     where state in (''pending'', ''sleeping'', ''running'')',
    ('t_' || p_queue_name) || '_state_active',
    't_' || p_queue_name
  );

  execute format(
    'create index if not exists %I on durable.%I (state)
     where state = ''cancelled''',
    ('t_' || p_queue_name) || '_state_cancelled',
    't_' || p_queue_name
  );
end;
$$;

-- Apply updated indexes to existing queues.
do $$
declare
  v_queue text;
begin
  for v_queue in select queue_name from durable.queues loop
    perform durable.ensure_queue_tables(v_queue);
  end loop;
end;
$$;
