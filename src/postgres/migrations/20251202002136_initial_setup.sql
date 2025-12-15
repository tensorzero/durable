-- Note: this is taken from `absurd` (https://github.com/earendil-works/absurd)
-- durable installs a Postgres-native durable workflow system that can be dropped
-- into an existing database.
--
-- It bootstraps the `durable` schema and required extensions so that jobs, runs,
-- checkpoints, and workflow events all live alongside application data without
-- external services.
--
-- Each queue is materialized as its own set of tables that share a prefix:
-- * `t_` for tasks (what is to be run)
-- * `r_` for runs (attempts to run a task)
-- * `c_` for checkpoints (saved states)
-- * `e_` for emitted events
-- * `w_` for wait registrations
--
-- `create_queue`, `drop_queue`, and `list_queues` provide the management
-- surface for provisioning queues safely.
--
-- Task execution flows through `spawn_task`, which records the logical task and
-- its first run, and `claim_task`, which hands work to workers with leasing
-- semantics, state transitions, and cancellation checks.  Runtime routines
-- such as `complete_run`, `sleep_for`, and `fail_run` advance or retry work,
-- enforce attempt accounting, and keep the task and run tables synchronized.
--
-- Long-running or event-driven workflows rely on lightweight persistence
-- primitives.  Checkpoint helpers (`set_task_checkpoint_state`,
-- `get_task_checkpoint_state`, `get_task_checkpoint_states`) write arbitrary
-- JSON payloads keyed by task and step, while `await_event` and `emit_event`
-- coordinate sleepers and external signals so that tasks can suspend and resume
-- without losing context.  Events are uniquely indexed and can only be fired
-- once per name.

create extension if not exists "uuid-ossp";

create schema if not exists durable;

-- Returns either the actual current timestamp or a fake one if
-- the session sets `durable.fake_now`.  This lets tests control time.
create function durable.current_time ()
  returns timestamptz
  language plpgsql
  volatile
as $$
declare
  v_fake text;
begin
  v_fake := current_setting('durable.fake_now', true);
  if v_fake is not null and length(trim(v_fake)) > 0 then
    return v_fake::timestamptz;
  end if;

  return clock_timestamp();
end;
$$;

create table if not exists durable.queues (
  queue_name text primary key,
  created_at timestamptz not null default durable.current_time()
);

create function durable.ensure_queue_tables (p_queue_name text)
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
end;
$$;

-- Creates the queue with the given name.
--
-- If the table already exists, the function returns silently.
create function durable.create_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  if p_queue_name is null or length(trim(p_queue_name)) = 0 then
    raise exception 'Queue name must be provided';
  end if;

  if length(p_queue_name) + 2 > 50 then
    raise exception 'Queue name "%" is too long', p_queue_name;
  end if;

  begin
    insert into durable.queues (queue_name)
    values (p_queue_name);
  exception when unique_violation then
    return;
  end;

  perform durable.ensure_queue_tables(p_queue_name);
end;
$$;

-- Drop a queue if it exists.
create function durable.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_existing_queue text;
begin
  select queue_name into v_existing_queue
  from durable.queues
  where queue_name = p_queue_name;

  if v_existing_queue is null then
    return;
  end if;

  execute format('drop table if exists durable.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 't_' || p_queue_name);

  delete from durable.queues where queue_name = p_queue_name;
end;
$$;

-- Lists all queues that currently exist.
create function durable.list_queues ()
  returns table (queue_name text)
  language sql
as $$
  select queue_name from durable.queues order by queue_name;
$$;

-- Spawns a given task in a queue.
create function durable.spawn_task (
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
  v_now timestamptz := durable.current_time();
  v_params jsonb := coalesce(p_params, 'null'::jsonb);
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
    -- Extract parent_task_id for subtask tracking
    v_parent_task_id := (p_options->>'parent_task_id')::uuid;
  end if;

  execute format(
    'insert into durable.%I (task_id, task_name, params, headers, retry_strategy, max_attempts, cancellation, parent_task_id, enqueue_at, first_started_at, state, attempts, last_attempt_run, completed_payload, cancelled_at)
     values ($1, $2, $3, $4, $5, $6, $7, $8, $9, null, ''pending'', $10, $11, null, null)',
    't_' || p_queue_name
  )
  using v_task_id, p_task_name, v_params, v_headers, v_retry_strategy, v_max_attempts, v_cancellation, v_parent_task_id, v_now, v_attempt, v_run_id;

  execute format(
    'insert into durable.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
     values ($1, $2, $3, ''pending'', $4, null, null, null, null)',
    'r_' || p_queue_name
  )
  using v_run_id, v_task_id, v_attempt, v_now;

  return query select v_task_id, v_run_id, v_attempt;
end;
$$;

-- Workers call this to reserve a task from a given queue
-- for a given reservation period in seconds.
create function durable.claim_task (
  p_queue_name text,
  p_worker_id text,
  p_claim_timeout integer default 30,
  p_qty integer default 1
)
  returns table (
    run_id uuid,
    task_id uuid,
    attempt integer,
    task_name text,
    params jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    headers jsonb,
    wake_event text,
    event_payload jsonb
  )
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_claim_timeout integer := coalesce(p_claim_timeout, 30);
  v_worker_id text := coalesce(nullif(p_worker_id, ''), 'worker');
  v_qty integer := greatest(coalesce(p_qty, 1), 1);
  v_claim_until timestamptz;
  v_sql text;
  v_expired_run record;
begin
  if v_claim_timeout <= 0 then
    raise exception 'claim_timeout must be greater than zero';
  end if;

  v_claim_until := v_now + make_interval(secs => v_claim_timeout);

  -- Apply cancellation rules before claiming.
  -- These are max_delay (delay before starting) and
  -- max_duration (duration from created to finished)
  execute format(
    'with limits as (
        select task_id,
               (cancellation->>''max_delay'')::bigint as max_delay,
               (cancellation->>''max_duration'')::bigint as max_duration,
               enqueue_at,
               first_started_at,
               state
          from durable.%I
        where state in (''pending'', ''sleeping'', ''running'')
     ),
     to_cancel as (
        select task_id
          from limits
         where
           (
             max_delay is not null
             and first_started_at is null
             and extract(epoch from ($1 - enqueue_at)) >= max_delay
           )
           or
           (
             max_duration is not null
             and first_started_at is not null
             and extract(epoch from ($1 - first_started_at)) >= max_duration
           )
     )
     update durable.%I t
        set state = ''cancelled'',
            cancelled_at = coalesce(t.cancelled_at, $1)
      where t.task_id in (select task_id from to_cancel)',
    't_' || p_queue_name,
    't_' || p_queue_name
  ) using v_now;

  -- Fail any run claims that have timed out
  for v_expired_run in
    execute format(
      'select run_id,
              claimed_by,
              claim_expires_at,
              attempt
         from durable.%I
        where state = ''running''
          and claim_expires_at is not null
          and claim_expires_at <= $1
        for update skip locked',
      'r_' || p_queue_name
    )
  using v_now
  loop
    perform durable.fail_run(
      p_queue_name,
      v_expired_run.run_id,
      jsonb_strip_nulls(jsonb_build_object(
        'name', '$ClaimTimeout',
        'message', 'worker did not finish task within claim interval',
        'workerId', v_expired_run.claimed_by,
        'claimExpiredAt', v_expired_run.claim_expires_at,
        'attempt', v_expired_run.attempt
      )),
      null
    );
  end loop;

  -- Find all tasks where state is cancelled,
  -- then update all the runs to be cancelled as well.
  execute format(
    'update durable.%I r
        set state = ''cancelled'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $1,
            wake_event = null
      where task_id in (select task_id from durable.%I where state = ''cancelled'')
        and r.state <> ''cancelled''',
    'r_' || p_queue_name,
    't_' || p_queue_name
  ) using v_now;

  -- Actually claim tasks
  v_sql := format(
    -- Grab unique pending / sleeping runs that are available now
    'with candidate as (
        select r.run_id
          from durable.%1$I r
          join durable.%2$I t on t.task_id = r.task_id
         where r.state in (''pending'', ''sleeping'')
           and t.state in (''pending'', ''sleeping'', ''running'')
           and r.available_at <= $1
         order by r.available_at, r.run_id
         limit $2
         for update skip locked
     ),
     -- update the runs to be running and set claim info
     updated as (
        update durable.%1$I r
           set state = ''running'',
               claimed_by = $3,
               claim_expires_at = $4,
               started_at = $1,
               available_at = $1
         where run_id in (select run_id from candidate)
         returning r.run_id, r.task_id, r.attempt
     ),
     -- update the task to also be running and handle attempt / time bookkeeping
     task_upd as (
        update durable.%2$I t
           set state = ''running'',
               attempts = greatest(t.attempts, u.attempt),
               first_started_at = coalesce(t.first_started_at, $1),
               last_attempt_run = u.run_id
          from updated u
         where t.task_id = u.task_id
         returning t.task_id
     ),
     -- clean up any wait registrations that timed out
     -- that are subsumed by the claim
     -- e.g. a wait times out so the run becomes available and now
     -- it is claimed but we do not want this wait to linger in DB
     wait_cleanup as (
        delete from durable.%3$I w
         using updated u
        where w.run_id = u.run_id
          and w.timeout_at is not null
          and w.timeout_at <= $1
        returning w.run_id
     )
     select
       u.run_id,
       u.task_id,
       u.attempt,
       t.task_name,
       t.params,
       t.retry_strategy,
       t.max_attempts,
       t.headers,
       r.wake_event,
       r.event_payload
     from updated u
     join durable.%1$I r on r.run_id = u.run_id
     join durable.%2$I t on t.task_id = u.task_id
     order by r.available_at, u.run_id',
    'r_' || p_queue_name,
    't_' || p_queue_name,
    'w_' || p_queue_name
  );

  return query execute v_sql using v_now, v_qty, v_worker_id, v_claim_until;
end;
$$;

-- Marks a run as completed
create function durable.complete_run (
  p_queue_name text,
  p_run_id uuid,
  p_state jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_task_id uuid;
  v_state text;
  v_parent_task_id uuid;
  v_now timestamptz := durable.current_time();
begin
  execute format(
    'select task_id, state
       from durable.%I
      where run_id = $1
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_state
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_state <> 'running' then
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  execute format(
    'update durable.%I
        set state = ''completed'',
            completed_at = $2,
            result = $3
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_now, p_state;

  -- Get parent_task_id to check if this is a subtask
  execute format(
    'update durable.%I
        set state = ''completed'',
            completed_payload = $2,
            last_attempt_run = $3
      where task_id = $1
      returning parent_task_id',
    't_' || p_queue_name
  )
  into v_parent_task_id
  using v_task_id, p_state, p_run_id;

  -- Clean up any wait registrations for this run
  execute format(
    'delete from durable.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;

  -- Emit completion event for parent to join on (only if this is a subtask)
  if v_parent_task_id is not null then
    perform durable.emit_event(
      p_queue_name,
      '$child:' || v_task_id::text,
      jsonb_build_object('status', 'completed', 'result', p_state)
    );
  end if;
end;
$$;

create function durable.sleep_for(
  p_queue_name text,
  p_run_id uuid,
  p_checkpoint_name text,
  p_duration_ms bigint
)
  returns boolean  -- true = suspended, false = wake time already passed
  language plpgsql
as $$
declare
  v_wake_at timestamptz;
  v_existing_state jsonb;
  v_now timestamptz := durable.current_time();
  v_task_id uuid;
begin
  -- Get task_id from run (needed for checkpoint table key)
  execute format(
    'select task_id from durable.%I where run_id = $1 and state = ''running'' for update',
    'r_' || p_queue_name
  ) into v_task_id using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" is not currently running in queue "%"', p_run_id, p_queue_name;
  end if;

  -- Check for existing checkpoint, else compute and store wake time
  execute format(
    'select state from durable.%I where task_id = $1 and checkpoint_name = $2',
    'c_' || p_queue_name
  ) into v_existing_state using v_task_id, p_checkpoint_name;

  if v_existing_state is not null then
    v_wake_at := (v_existing_state #>> '{}')::timestamptz;
  else
    v_wake_at := v_now + (p_duration_ms || ' milliseconds')::interval;
    execute format(
      'insert into durable.%I (task_id, checkpoint_name, state, owner_run_id, updated_at)
       values ($1, $2, $3, $4, $5)',
      'c_' || p_queue_name
    ) using v_task_id, p_checkpoint_name, to_jsonb(v_wake_at::text), p_run_id, v_now;
  end if;

  -- If wake time passed, return false (no suspend needed)
  if v_now >= v_wake_at then
    return false;
  end if;

  -- Schedule the run to wake at v_wake_at
  execute format(
    'update durable.%I
        set state = ''sleeping'',
            claimed_by = null,
            claim_expires_at = null,
            available_at = $2,
            wake_event = null
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_wake_at;

  execute format(
    'update durable.%I
        set state = ''sleeping''
      where task_id = $1',
    't_' || p_queue_name
  ) using v_task_id;

  return true;
end;
$$;

-- Recursively cancels all children of a parent task.
-- Used when a parent task fails or is cancelled to cascade the cancellation.
create function durable.cascade_cancel_children (
  p_queue_name text,
  p_parent_task_id uuid
)
  returns void
  language plpgsql
as $$
declare
  v_child_id uuid;
  v_child_state text;
  v_now timestamptz := durable.current_time();
begin
  -- Find all children of this parent that are not in terminal state
  for v_child_id, v_child_state in
    execute format(
      'select task_id, state
         from durable.%I
        where parent_task_id = $1
          and state not in (''completed'', ''failed'', ''cancelled'')
        for update',
      't_' || p_queue_name
    )
    using p_parent_task_id
  loop
    -- Cancel the child task
    execute format(
      'update durable.%I
          set state = ''cancelled'',
              cancelled_at = coalesce(cancelled_at, $2)
        where task_id = $1',
      't_' || p_queue_name
    ) using v_child_id, v_now;

    -- Cancel all runs of this child
    execute format(
      'update durable.%I
          set state = ''cancelled'',
              claimed_by = null,
              claim_expires_at = null
        where task_id = $1
          and state not in (''completed'', ''failed'', ''cancelled'')',
      'r_' || p_queue_name
    ) using v_child_id;

    -- Delete wait registrations
    execute format(
      'delete from durable.%I where task_id = $1',
      'w_' || p_queue_name
    ) using v_child_id;

    -- Emit cancellation event so parent's join() can receive it
    perform durable.emit_event(
      p_queue_name,
      '$child:' || v_child_id::text,
      jsonb_build_object('status', 'cancelled')
    );

    -- Recursively cancel grandchildren
    perform durable.cascade_cancel_children(p_queue_name, v_child_id);
  end loop;
end;
$$;

create function durable.fail_run (
  p_queue_name text,
  p_run_id uuid,
  p_reason jsonb,
  p_retry_at timestamptz default null
)
  returns void
  language plpgsql
as $$
declare
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
  v_cancelled_at timestamptz := null;
  v_parent_task_id uuid;
begin
  -- find the run to fail
  execute format(
    'select r.task_id, r.attempt
       from durable.%I r
      where r.run_id = $1
        and r.state in (''running'', ''sleeping'')
      for update',
    'r_' || p_queue_name
  )
  into v_task_id, v_attempt
  using p_run_id;

  if v_task_id is null then
    raise exception 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  -- get the retry strategy and metadata about task
  execute format(
    'select retry_strategy, max_attempts, first_started_at, cancellation, state, parent_task_id
       from durable.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_retry_strategy, v_max_attempts, v_first_started, v_cancellation, v_task_state, v_parent_task_id
  using v_task_id;

  -- actually fail the run
  execute format(
    'update durable.%I
        set state = ''failed'',
            wake_event = null,
            failed_at = $2,
            failure_reason = $3
      where run_id = $1',
    'r_' || p_queue_name
  ) using p_run_id, v_now, p_reason;

  v_next_attempt := v_attempt + 1;
  v_task_state_after := 'failed';
  v_recorded_attempt := v_attempt;

  -- compute the next retry time
  if v_max_attempts is null or v_next_attempt <= v_max_attempts then
    if p_retry_at is not null then
      v_next_available := p_retry_at;
    else
      v_retry_kind := coalesce(v_retry_strategy->>'kind', 'none');
      if v_retry_kind = 'fixed' then
        v_base := coalesce((v_retry_strategy->>'base_seconds')::double precision, 60);
        v_delay_seconds := v_base;
      elsif v_retry_kind = 'exponential' then
        v_base := coalesce((v_retry_strategy->>'base_seconds')::double precision, 30);
        v_factor := coalesce((v_retry_strategy->>'factor')::double precision, 2);
        v_delay_seconds := v_base * power(v_factor, greatest(v_attempt - 1, 0));
        v_max_seconds := (v_retry_strategy->>'max_seconds')::double precision;
        if v_max_seconds is not null then
          v_delay_seconds := least(v_delay_seconds, v_max_seconds);
        end if;
      else
        v_delay_seconds := 0;
      end if;
      v_next_available := v_now + (v_delay_seconds * interval '1 second');
    end if;

    if v_next_available < v_now then
      v_next_available := v_now;
    end if;

    if v_cancellation is not null then
      v_max_duration := (v_cancellation->>'max_duration')::bigint;
      if v_max_duration is not null and v_first_started is not null then
        if extract(epoch from (v_next_available - v_first_started)) >= v_max_duration then
          v_task_cancel := true;
        end if;
      end if;
    end if;

    -- set up the new run if not cancelling
    if not v_task_cancel then
      v_task_state_after := case when v_next_available > v_now then 'sleeping' else 'pending' end;
      v_new_run_id := durable.portable_uuidv7();
      v_recorded_attempt := v_next_attempt;
      v_last_attempt_run := v_new_run_id;
      execute format(
        'insert into durable.%I (run_id, task_id, attempt, state, available_at, wake_event, event_payload, result, failure_reason)
         values ($1, $2, $3, %L, $4, null, null, null, null)',
        'r_' || p_queue_name,
        v_task_state_after
      )
      using v_new_run_id, v_task_id, v_next_attempt, v_next_available;
    end if;
  end if;

  if v_task_cancel then
    v_task_state_after := 'cancelled';
    v_cancelled_at := v_now;
    v_recorded_attempt := greatest(v_recorded_attempt, v_attempt);
    v_last_attempt_run := p_run_id;
  end if;

  execute format(
    'update durable.%I
        set state = %L,
            attempts = greatest(attempts, $3),
            last_attempt_run = $4,
            cancelled_at = coalesce(cancelled_at, $5)
      where task_id = $1',
    't_' || p_queue_name,
    v_task_state_after
  ) using v_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  execute format(
    'delete from durable.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;

  -- If task reached terminal failure state (failed or cancelled), emit event and cascade cancel
  if v_task_state_after in ('failed', 'cancelled') then
    -- Cascade cancel all children
    perform durable.cascade_cancel_children(p_queue_name, v_task_id);

    -- Emit completion event for parent to join on (only if this is a subtask)
    if v_parent_task_id is not null then
      perform durable.emit_event(
        p_queue_name,
        '$child:' || v_task_id::text,
        jsonb_build_object('status', v_task_state_after, 'error', p_reason)
      );
    end if;
  end if;
end;
$$;

-- sets the checkpoint state for a given task and step name.
-- only updates if the owner_run's attempt is >= existing owner's attempt.
-- if the task is cancelled, this throws error AB001.
-- if extend_claim_by is provided, extends the claim on the owner_run by that many seconds.
create function durable.set_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text,
  p_state jsonb,
  p_owner_run uuid,
  p_extend_claim_by integer default null -- seconds
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_new_attempt integer;
  v_existing_attempt integer;
  v_existing_owner uuid;
  v_task_state text;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  -- get the current attempt number and task state
  execute format(
    'select r.attempt, t.state
       from durable.%I r
       join durable.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_new_attempt, v_task_state
  using p_owner_run;

  if v_new_attempt is null then
    raise exception 'Run "%" not found for checkpoint', p_owner_run;
  end if;

  -- if the task was cancelled raise a special error the caller can catch to terminate
  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- Extend the claim if requested
  if p_extend_claim_by is not null and p_extend_claim_by > 0 then
    execute format(
      'update durable.%I
          set claim_expires_at = $2 + make_interval(secs => $3)
        where run_id = $1
          and state = ''running''
          and claim_expires_at is not null',
      'r_' || p_queue_name
    )
    using p_owner_run, v_now, p_extend_claim_by;
  end if;

  execute format(
    'select c.owner_run_id,
            r.attempt
       from durable.%I c
       left join durable.%I r on r.run_id = c.owner_run_id
      where c.task_id = $1
        and c.checkpoint_name = $2',
    'c_' || p_queue_name,
    'r_' || p_queue_name
  )
  into v_existing_owner, v_existing_attempt
  using p_task_id, p_step_name;

  if v_existing_owner is null or v_existing_attempt is null or v_new_attempt >= v_existing_attempt then
    execute format(
      'insert into durable.%I (task_id, checkpoint_name, state, owner_run_id, updated_at)
       values ($1, $2, $3, $4, $5)
       on conflict (task_id, checkpoint_name)
       do update set state = excluded.state,
                     owner_run_id = excluded.owner_run_id,
                     updated_at = excluded.updated_at',
      'c_' || p_queue_name
    ) using p_task_id, p_step_name, p_state, p_owner_run, v_now;
  end if;
end;
$$;

-- extends the claim on a run by that many seconds
create function durable.extend_claim (
  p_queue_name text,
  p_run_id uuid,
  p_extend_by integer
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_extend_by integer;
  v_claim_timeout integer;
  v_rows_updated integer;
  v_task_state text;
begin
  execute format(
    'select t.state
       from durable.%I r
       join durable.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_state
  using p_run_id;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'update durable.%I
        set claim_expires_at = $2 + make_interval(secs => $3)
      where run_id = $1
        and state = ''running''
        and claim_expires_at is not null',
    'r_' || p_queue_name
  )
  using p_run_id, v_now, p_extend_by;
end;
$$;

create function durable.get_task_checkpoint_state (
  p_queue_name text,
  p_task_id uuid,
  p_step_name text
)
  returns table (
    checkpoint_name text,
    state jsonb,
    owner_run_id uuid,
    updated_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select checkpoint_name, state, owner_run_id, updated_at
       from durable.%I
      where task_id = $1
        and checkpoint_name = $2',
    'c_' || p_queue_name
  ) using p_task_id, p_step_name;
end;
$$;

create function durable.get_task_checkpoint_states (
  p_queue_name text,
  p_task_id uuid
)
  returns table (
    checkpoint_name text,
    state jsonb,
    owner_run_id uuid,
    updated_at timestamptz
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select checkpoint_name, state, owner_run_id, updated_at
       from durable.%I
      where task_id = $1
      order by updated_at asc',
    'c_' || p_queue_name
  ) using p_task_id;
end;
$$;

-- awaits an event for a given task's run and step name.
-- this will immediately return if it the event has already returned
-- it will also time out if the event has taken too long
-- if a timeout is set it will return without a payload after that much time
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
    payload jsonb
  )
  language plpgsql
as $$
declare
  v_run_state text;
  v_existing_payload jsonb;
  v_event_payload jsonb;
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
    return query select false, v_checkpoint_payload;
    return;
  end if;

  -- let's get the run state, any existing event payload and wake event name
  execute format(
    'select r.state, r.event_payload, r.wake_event, t.state
       from durable.%I r
       join durable.%I t on t.task_id = r.task_id
      where r.run_id = $1
      for update',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_run_state, v_existing_payload, v_wake_event, v_task_state
  using p_run_id;

  if v_run_state is null then
    raise exception 'Run "%" not found while awaiting event', p_run_id;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'select payload
       from durable.%I
      where event_name = $1',
    'e_' || p_queue_name
  )
  into v_event_payload
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
    return query select false, v_resolved_payload;
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
    return query select false, null::jsonb;
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

  return query select true, null::jsonb;
  return;
end;
$$;

-- emit an event and wake up waiters
create function durable.emit_event (
  p_queue_name text,
  p_event_name text,
  p_payload jsonb default null
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_payload jsonb := coalesce(p_payload, 'null'::jsonb);
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  -- insert the event into the events table
  execute format(
    'insert into durable.%I (event_name, payload, emitted_at)
     values ($1, $2, $3)
     on conflict (event_name)
     do update set payload = excluded.payload,
                   emitted_at = excluded.emitted_at',
    'e_' || p_queue_name
  ) using p_event_name, v_payload, v_now;

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

-- Manually cancels a task by its task_id.
-- Sets the task state to 'cancelled' and prevents any future runs.
-- Currently running code will detect cancellation at the next checkpoint or heartbeat.
create function durable.cancel_task (
  p_queue_name text,
  p_task_id uuid
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_task_state text;
  v_parent_task_id uuid;
begin
  execute format(
    'select state, parent_task_id
       from durable.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_task_state, v_parent_task_id
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  if v_task_state in ('completed', 'failed', 'cancelled') then
    return;
  end if;

  execute format(
    'update durable.%I
        set state = ''cancelled'',
            cancelled_at = coalesce(cancelled_at, $2)
      where task_id = $1',
    't_' || p_queue_name
  ) using p_task_id, v_now;

  execute format(
    'update durable.%I
        set state = ''cancelled'',
            claimed_by = null,
            claim_expires_at = null
      where task_id = $1
        and state not in (''completed'', ''failed'', ''cancelled'')',
    'r_' || p_queue_name
  ) using p_task_id;

  execute format(
    'delete from durable.%I where task_id = $1',
    'w_' || p_queue_name
  ) using p_task_id;

  -- Cascade cancel all children
  perform durable.cascade_cancel_children(p_queue_name, p_task_id);

  -- Emit cancellation event for parent to join on (only if this is a subtask)
  if v_parent_task_id is not null then
    perform durable.emit_event(
      p_queue_name,
      '$child:' || p_task_id::text,
      jsonb_build_object('status', 'cancelled')
    );
  end if;
end;
$$;

-- Checks if a task and all its recursive children are in a "blocked" state.
-- Blocked states: sleeping, completed, failed, cancelled
-- Not blocked states: pending, running
--
-- Returns true if the entire task tree is blocked (no pending/running tasks).
-- Returns false if any task in the tree is pending or running.
-- Returns NULL if the task does not exist.
create function durable.is_task_tree_blocked (
  p_queue_name text,
  p_task_id uuid
)
  returns boolean
  language plpgsql
  stable
as $$
declare
  v_root_state text;
  v_has_unblocked boolean;
begin
  -- Check if root task exists and get its state
  execute format(
    'select state from durable.%I where task_id = $1',
    't_' || p_queue_name
  )
  into v_root_state
  using p_task_id;

  -- Return NULL if task doesn't exist
  if v_root_state is null then
    return null;
  end if;

  -- Quick check: if root is not blocked, return false immediately
  if v_root_state in ('pending', 'running') then
    return false;
  end if;

  -- Use recursive CTE to check all descendants
  execute format(
    'with recursive task_tree as (
        select task_id, state
          from durable.%1$I
         where parent_task_id = $1

        union all

        select t.task_id, t.state
          from durable.%1$I t
          join task_tree tt on t.parent_task_id = tt.task_id
     )
     select exists(
        select 1 from task_tree where state in (''pending'', ''running'')
     )',
    't_' || p_queue_name
  )
  into v_has_unblocked
  using p_task_id;

  return not v_has_unblocked;
end;
$$;

-- Cleans up old completed, failed, or cancelled tasks and their related data.
-- Deletes tasks whose terminal timestamp (completed_at, failed_at, or cancelled_at)
-- is older than the specified TTL in seconds.
--
-- Returns the number of tasks deleted.
create function durable.cleanup_tasks (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  -- Delete in order: wait registrations, checkpoints, runs, then tasks
  -- Use a CTE to find eligible tasks and delete their related data
  execute format(
    'with eligible_tasks as (
        select t.task_id,
               case
                 when t.state = ''completed'' then r.completed_at
                 when t.state = ''failed'' then r.failed_at
                 when t.state = ''cancelled'' then t.cancelled_at
                 else null
               end as terminal_at
          from durable.%1$I t
          left join durable.%2$I r on r.run_id = t.last_attempt_run
         where t.state in (''completed'', ''failed'', ''cancelled'')
     ),
     to_delete as (
        select task_id
          from eligible_tasks
         where terminal_at is not null
           and terminal_at < $1
         order by terminal_at
         limit $2
     ),
     del_waits as (
        delete from durable.%3$I w
         where w.task_id in (select task_id from to_delete)
     ),
     del_checkpoints as (
        delete from durable.%4$I c
         where c.task_id in (select task_id from to_delete)
     ),
     del_runs as (
        delete from durable.%2$I r
         where r.task_id in (select task_id from to_delete)
     ),
     del_tasks as (
        delete from durable.%1$I t
         where t.task_id in (select task_id from to_delete)
         returning 1
     )
     select count(*) from del_tasks',
    't_' || p_queue_name,
    'r_' || p_queue_name,
    'w_' || p_queue_name,
    'c_' || p_queue_name
  )
  into v_deleted_count
  using v_cutoff, p_limit;

  return v_deleted_count;
end;
$$;

-- Cleans up old emitted events.
-- Deletes events whose emitted_at timestamp is older than the specified TTL in seconds.
--
-- Returns the number of events deleted.
create function durable.cleanup_events (
  p_queue_name text,
  p_ttl_seconds integer,
  p_limit integer default 1000
)
  returns integer
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_cutoff timestamptz;
  v_deleted_count integer;
begin
  if p_ttl_seconds is null or p_ttl_seconds < 0 then
    raise exception 'TTL must be a non-negative number of seconds';
  end if;

  v_cutoff := v_now - (p_ttl_seconds * interval '1 second');

  execute format(
    'with to_delete as (
        select event_name
          from durable.%I
         where emitted_at < $1
         order by emitted_at
         limit $2
     ),
     del_events as (
        delete from durable.%I e
         where e.event_name in (select event_name from to_delete)
         returning 1
     )
     select count(*) from del_events',
    'e_' || p_queue_name,
    'e_' || p_queue_name
  )
  into v_deleted_count
  using v_cutoff, p_limit;

  return v_deleted_count;
end;
$$;

-- utility function to generate a uuidv7 even for older postgres versions.
create function durable.portable_uuidv7 ()
  returns uuid
  language plpgsql
  volatile
as $$
declare
  v_server_num integer := current_setting('server_version_num')::int;
  ts_ms bigint;
  b bytea;
  rnd bytea;
  i int;
begin
  if v_server_num >= 180000 then
    return uuidv7 ();
  end if;
  ts_ms := floor(extract(epoch from durable.current_time()) * 1000)::bigint;
  rnd := uuid_send(uuid_generate_v4 ());
  b := repeat(E'\\000', 16)::bytea;
  for i in 0..5 loop
    b := set_byte(b, i, ((ts_ms >> ((5 - i) * 8)) & 255)::int);
  end loop;
  for i in 6..15 loop
    b := set_byte(b, i, get_byte(rnd, i));
  end loop;
  b := set_byte(b, 6, ((get_byte(b, 6) & 15) | (7 << 4)));
  b := set_byte(b, 8, ((get_byte(b, 8) & 63) | 128));
  return encode(b, 'hex')::uuid;
end;
$$;
