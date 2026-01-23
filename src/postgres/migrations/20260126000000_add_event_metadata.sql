-- Add metadata column to event tables for storing trace context and other system metadata.
-- This allows separating user payload from system metadata (like OpenTelemetry trace context).

-- Drop the existing await_event function since we're changing its return type
drop function if exists durable.await_event(text, uuid, uuid, text, text, integer);

-- Drop the existing emit_event function since we're adding a new parameter
drop function if exists durable.emit_event(text, text, jsonb);

-- Add metadata column to all existing event tables
do $$
declare
  tbl record;
begin
  for tbl in
    select tablename from pg_tables
    where schemaname = 'durable' and tablename like 'e\_%'
  loop
    execute format('alter table durable.%I add column if not exists metadata jsonb', tbl.tablename);
  end loop;
end;
$$;

-- Replace emit_event to accept metadata parameter
create or replace function durable.emit_event (
  p_queue_name text,
  p_event_name text,
  p_payload jsonb default null,
  p_metadata jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_payload jsonb := coalesce(p_payload, 'null'::jsonb);
  v_inserted_count integer;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  -- Insert the event into the events table (first-writer-wins).
  -- Subsequent emits for the same event are no-ops.
  -- We use DO UPDATE WHERE payload IS NULL to handle the case where await_event
  -- created a placeholder row before emit_event ran.
  execute format(
    'insert into durable.%I (event_name, payload, metadata, emitted_at)
     values ($1, $2, $3, $4)
     on conflict (event_name) do update
        set payload = excluded.payload,
            metadata = excluded.metadata,
            emitted_at = excluded.emitted_at
      where durable.%I.payload is null',
    'e_' || p_queue_name,
    'e_' || p_queue_name
  ) using p_event_name, v_payload, p_metadata, v_now;

  get diagnostics v_inserted_count = row_count;

  -- Only wake waiters if we actually inserted (first emit).
  -- Subsequent emits are no-ops to maintain consistency.
  if v_inserted_count = 0 then
    return;
  end if;

  execute format(
    'with expired_waits as (
        delete from durable.%1$I w
         where w.event_name = $1
           and w.timeout_at is not null
           and w.timeout_at <= $2
         returning w.run_id
     ),
     affected as (
        select run_id, task_id, step_name
          from durable.%1$I
         where event_name = $1
           and (timeout_at is null or timeout_at > $2)
     ),
     -- Lock tasks before updating runs to prevent waking cancelled tasks.
     -- Only lock sleeping tasks to avoid interfering with other operations.
     -- This prevents waking cancelled tasks (e.g., when cascade_cancel_children
     -- is running concurrently).
     locked_tasks as (
        select t.task_id
          from durable.%4$I t
         where t.task_id in (select task_id from affected)
           and t.state = ''sleeping''
         for update
     ),
     -- update the run table for all waiting runs so they are pending again
     updated_runs as (
        update durable.%2$I r
           set state = ''pending'',
               available_at = $2,
               wake_event = null,
               event_payload = $3,
               claimed_by = null,
               claim_expires_at = null
         where r.run_id in (select run_id from affected)
           and r.state = ''sleeping''
           and r.task_id in (select task_id from locked_tasks)
         returning r.run_id, r.task_id
     ),
     -- update checkpoints for all affected tasks/steps so they contain the event payload
     checkpoint_upd as (
        insert into durable.%3$I (task_id, checkpoint_name, state, owner_run_id, updated_at)
        select a.task_id, a.step_name, $3, a.run_id, $2
          from affected a
          join updated_runs ur on ur.run_id = a.run_id
        on conflict (task_id, checkpoint_name)
        do update set state = excluded.state,
                      owner_run_id = excluded.owner_run_id,
                      updated_at = excluded.updated_at
     ),
     -- update the task table to set to pending
     updated_tasks as (
        update durable.%4$I t
           set state = ''pending''
         where t.task_id in (select task_id from updated_runs)
         returning task_id
     )
     -- delete the wait registrations that were satisfied
     delete from durable.%5$I w
      where w.event_name = $1
        and w.run_id in (select run_id from updated_runs)',
    'w_' || p_queue_name,
    'r_' || p_queue_name,
    'c_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name
  ) using p_event_name, v_now, v_payload;
end;
$$;

-- Create await_event with metadata in return type
create function durable.await_event (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_step_name text,
  p_event_name text,
  p_timeout integer default null -- seconds
)
  returns table (
    should_suspend boolean,
    payload jsonb,
    metadata jsonb
  )
  language plpgsql
as $$
declare
  v_run_state text;
  v_run_task_id uuid;
  v_existing_payload jsonb;
  v_event_payload jsonb;
  v_event_metadata jsonb;
  v_checkpoint_payload jsonb;
  v_resolved_payload jsonb;
  v_timeout_at timestamptz;
  v_available_at timestamptz;
  v_now timestamptz := durable.current_time();
  v_task_state text;
  v_wake_event text;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  if p_timeout is not null then
    if p_timeout < 0 then
      raise exception 'timeout must be non-negative';
    end if;
    v_timeout_at := v_now + (p_timeout::double precision * interval '1 second');
  end if;

  v_available_at := coalesce(v_timeout_at, 'infinity'::timestamptz);

  -- if there is already a checkpoint for this step just use it
  execute format(
    'select state
       from durable.%I
      where task_id = $1
        and checkpoint_name = $2',
    'c_' || p_queue_name
  )
  into v_checkpoint_payload
  using p_task_id, p_step_name;

  if v_checkpoint_payload is not null then
    -- Return from checkpoint without metadata (trace linking already done)
    return query select false, v_checkpoint_payload, null::jsonb;
    return;
  end if;

  -- Ensure a row exists for this event so we can take a row-level lock.
  --
  -- We use payload IS NULL as the sentinel for "not emitted yet".  emit_event
  -- always writes a non-NULL payload (at minimum JSON null).
  --
  -- Lock ordering is important to avoid deadlocks: await_event locks the event
  -- row first (FOR SHARE) and then the run row (FOR UPDATE).  emit_event
  -- naturally locks the event row via its UPSERT before touching waits/runs.
  execute format(
    'insert into durable.%I (event_name, payload, emitted_at)
     values ($1, null, ''epoch''::timestamptz)
     on conflict (event_name) do nothing',
    'e_' || p_queue_name
  ) using p_event_name;

  execute format(
    'select 1
       from durable.%I
      where event_name = $1
      for share',
    'e_' || p_queue_name
  ) using p_event_name;

  -- Lock task first to keep a consistent task -> run lock order.
  execute format(
    'select state from durable.%I where task_id = $1 for update',
    't_' || p_queue_name
  )
  into v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- Lock run after task
  execute format(
    'select task_id, state, event_payload, wake_event
       from durable.%I
      where run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_run_task_id, v_run_state, v_existing_payload, v_wake_event
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_run_task_id <> p_task_id then
    raise exception 'Run "%" does not belong to task "%"', p_run_id, p_task_id;
  end if;

  -- Fetch event payload and metadata
  execute format(
    'select payload, metadata
       from durable.%I
      where event_name = $1',
    'e_' || p_queue_name
  )
  into v_event_payload, v_event_metadata
  using p_event_name;

  if v_existing_payload is not null then
    execute format(
      'update durable.%I
          set event_payload = null
        where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;

    if v_event_payload is not null and v_event_payload = v_existing_payload then
      v_resolved_payload := v_existing_payload;
    end if;
  end if;

  if v_run_state <> 'running' then
    raise exception 'Run "%" must be running to await events', p_run_id;
  end if;

  if v_resolved_payload is null and v_event_payload is not null then
    v_resolved_payload := v_event_payload;
  end if;

  -- last write wins if there is an existing checkpoint with this name for this task
  if v_resolved_payload is not null then
    execute format(
      'insert into durable.%I (task_id, checkpoint_name, state, owner_run_id, updated_at)
       values ($1, $2, $3, $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, v_resolved_payload, p_run_id, v_now;
    return query select false, v_resolved_payload, v_event_metadata;
    return;
  end if;

  -- Detect if we resumed due to timeout: wake_event matches and payload is null
  if v_resolved_payload is null and v_wake_event = p_event_name and v_existing_payload is null then
    -- Resumed due to timeout; don't re-sleep and don't create a new wait
    -- unset the wake event before returning
    execute format(
      'update durable.%I set wake_event = null where run_id = $1',
      'r_' || p_queue_name
    ) using p_run_id;
    return query select false, null::jsonb, null::jsonb;
    return;
  end if;

  -- otherwise we must set up a waiter
  execute format(
    'insert into durable.%I (task_id, run_id, step_name, event_name, timeout_at, created_at)
     values ($1, $2, $3, $4, $5, $6)
     on conflict (run_id, step_name)
     do update set event_name = excluded.event_name,
                   timeout_at = excluded.timeout_at,
                   created_at = excluded.created_at',
    'w_' || p_queue_name
  ) using p_task_id, p_run_id, p_step_name, p_event_name, v_timeout_at, v_now;

  execute format(
    'update durable.%I
        set state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $3,
            wake_event = $2,
            event_payload = null
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, p_event_name, v_available_at;

  execute format(
    'update durable.%I
        set state = ''sleeping''
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id;

  return query select true, null::jsonb, null::jsonb;
  return;
end;
$$;

-- Update ensure_queue_tables to include metadata column when creating new event tables
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

  -- Event table WITH metadata column
  execute format(
    'create table if not exists durable.%I (
        event_name text primary key,
        payload jsonb,
        metadata jsonb,
        emitted_at timestamptz not null default durable.current_time()
     )',
    'e_' || p_queue_name
  );

  execute format('comment on column durable.%I.payload is %L', 'e_' || p_queue_name, 'User-defined. Event payload. Internal child events use: {"status": "completed"|"failed"|"cancelled", "result"?: <json>, "error"?: <json>}');
  execute format('comment on column durable.%I.metadata is %L', 'e_' || p_queue_name, 'System metadata (e.g., trace context for distributed tracing). Format: {"durable::otel_context": {...}}');

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
