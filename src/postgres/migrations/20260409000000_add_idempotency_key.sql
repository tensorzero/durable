-- Add idempotency key support for task deduplication.
-- When set, only the first spawn with a given key creates a task.
-- Subsequent spawns with the same key (for non-terminal tasks) are no-ops.

-- 1. Add column and index to all existing queue task tables
do $$
declare
  q record;
begin
  for q in select queue_name from durable.queues loop
    execute format(
      'alter table durable.%I add column if not exists idempotency_key text',
      't_' || q.queue_name
    );
    execute format(
      'create unique index if not exists %I on durable.%I (idempotency_key) where idempotency_key is not null',
      ('t_' || q.queue_name) || '_ik',
      't_' || q.queue_name
    );
  end loop;
end;
$$;

-- 2. Update ensure_queue_tables so new queues also get the column + index
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
        idempotency_key text,
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

  -- Idempotency might be added after the table was created; handle both cases
  execute format(
    'alter table durable.%I add column if not exists idempotency_key text',
    't_' || p_queue_name
  );

  execute format('comment on column durable.%I.params is %L', 't_' || p_queue_name, 'User-defined. Task input parameters. Schema depends on Task::Params type.');
  execute format('comment on column durable.%I.headers is %L', 't_' || p_queue_name, 'User-defined. Optional key-value metadata as {"key": <any JSON value>}.');
  execute format('comment on column durable.%I.retry_strategy is %L', 't_' || p_queue_name, '{"kind": "none"} | {"kind": "fixed", "base_seconds": <u64>} | {"kind": "exponential", "base_seconds": <u64>, "factor": <f64>, "max_seconds": <u64>}');
  execute format('comment on column durable.%I.cancellation is %L', 't_' || p_queue_name, '{"max_delay": <seconds>, "max_duration": <seconds>} - both optional. max_delay: cancel if not started within N seconds of enqueue. max_duration: cancel if not completed within N seconds of first start.');
  execute format('comment on column durable.%I.idempotency_key is %L', 't_' || p_queue_name, 'Optional dedup key. When set, only one non-terminal task with this key can exist. Set via SpawnOptions.idempotency_key.');
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

  -- Partial index for claim candidate ORDER BY (available_at, run_id).
  -- Matches the exact ordering used in the claim query for ready runs.
  execute format(
    'create index if not exists %I on durable.%I (available_at, run_id) include (task_id)
     where state in (''pending'', ''sleeping'')',
    ('r_' || p_queue_name) || '_ready',
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

  -- Speed up cleanup_task_terminal wait deletion by task_id.
  execute format(
    'create index if not exists %I on durable.%I (task_id)',
    ('w_' || p_queue_name) || '_ti',
    'w_' || p_queue_name
  );

  -- Index for finding children of a parent task (for cascade cancellation)
  execute format(
    'create index if not exists %I on durable.%I (parent_task_id) where parent_task_id is not null',
    ('t_' || p_queue_name) || '_pti',
    't_' || p_queue_name
  );

  -- Idempotency key unique index (partial: only non-null keys)
  execute format(
    'create unique index if not exists %I on durable.%I (idempotency_key) where idempotency_key is not null',
    ('t_' || p_queue_name) || '_ik',
    't_' || p_queue_name
  );

  -- Speed up claim timeout scans.
  execute format(
    'create index if not exists %I on durable.%I (claim_expires_at)
     where state = ''running'' and claim_expires_at is not null',
    ('r_' || p_queue_name) || '_cei',
    'r_' || p_queue_name
  );

  -- Speed up cancellation sweep: only index tasks that have cancellation policies.
  execute format(
    'create index if not exists %I on durable.%I (task_id)
     where state in (''pending'', ''sleeping'', ''running'')
       and cancellation is not null
       and (cancellation ? ''max_delay'' or cancellation ? ''max_duration'')',
    ('t_' || p_queue_name) || '_cxlpol',
    't_' || p_queue_name
  );

  -- Composite index for active task state lookups.
  -- Enables Index Only Scans for claim_task join, emit_event, and cancel propagation.
  execute format(
    'create index if not exists %I on durable.%I (state, task_id)
     where state in (''pending'', ''sleeping'', ''running'', ''cancelled'')',
    ('t_' || p_queue_name) || '_state_tid',
    't_' || p_queue_name
  );
end;
$$;

-- 3. Update spawn_task to handle idempotency key
create or replace function durable.spawn_task (
  p_queue_name text,
  p_task_name text,
  p_params jsonb,
  p_options jsonb default '{}'::jsonb
)
  returns table (
    task_id uuid,
    run_id uuid,
    attempt integer
  )
  language plpgsql
as $$
declare
  v_task_id uuid := durable.portable_uuidv7();
  v_run_id uuid := durable.portable_uuidv7();
  v_attempt integer := 1;
  v_headers jsonb;
  v_retry_strategy jsonb;
  v_max_attempts integer;
  v_cancellation jsonb;
  v_parent_task_id uuid;
  v_idempotency_key text;
  v_now timestamptz := durable.current_time();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
  v_existing_task_id uuid;
begin
  if p_task_name is null or length(trim(p_task_name)) = 0 then
    raise exception 'task_name must be provided';
  end if;

  if p_options is not null then
    v_headers := p_options->'headers';
    v_retry_strategy := p_options->'retry_strategy';
    if p_options ? 'max_attempts' then
      v_max_attempts := (p_options->>'max_attempts')::int;
      if v_max_attempts is not null and v_max_attempts < 1 then
        raise exception 'max_attempts must be >= 1';
      end if;
    end if;
    v_cancellation := p_options->'cancellation';
    v_parent_task_id := (p_options->>'parent_task_id')::uuid;
    v_idempotency_key := p_options->>'idempotency_key';
  end if;

  -- Idempotency check: return existing non-terminal task if key matches
  if v_idempotency_key is not null then
    execute format(
      'select t.task_id from durable.%I t
       where t.idempotency_key = $1
         and t.state not in (''completed'', ''failed'', ''cancelled'')
       limit 1',
      't_' || p_queue_name
    )
    into v_existing_task_id
    using v_idempotency_key;

    if v_existing_task_id is not null then
      return query
        execute format(
          'select t.task_id, r.run_id, r.attempt
           from durable.%I t
           join durable.%I r on r.task_id = t.task_id and r.run_id = t.last_attempt_run
           where t.task_id = $1',
          't_' || p_queue_name,
          'r_' || p_queue_name
        )
        using v_existing_task_id;
      return;
    end if;
  end if;

  execute format(
    'insert into durable.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, parent_task_id, idempotency_key, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, null, ''pending'', $11, $12, null, null)',
    't_' || p_queue_name
  )
  using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_parent_task_id, v_idempotency_key, v_now, v_attempt, v_run_id;

  execute format(
    'insert into durable.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  using v_run_id, v_task_id, v_attempt, v_now;

  return query select v_task_id, v_run_id, v_attempt;
end;
$$;
