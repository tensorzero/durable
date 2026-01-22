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

-- Update claim_task to only scan tasks that have cancellation policies.
create or replace function durable.claim_task (
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
  v_cancelled_task uuid;
begin
  if v_claim_timeout <= 0 then
    raise exception 'claim_timeout must be greater than zero';
  end if;

  v_claim_until := v_now + make_interval(secs => v_claim_timeout);

  -- Apply cancellation rules before claiming.
  -- These are max_delay (delay before starting) and
  -- max_duration (duration from created to finished)
  -- Use a loop so we can cleanup each cancelled task properly.
  -- Only scan tasks that actually have cancellation policies set.
  for v_cancelled_task in
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
            and cancellation is not null
            and (cancellation ? ''max_delay'' or cancellation ? ''max_duration'')
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
        where t.task_id in (select task_id from to_cancel)
        returning t.task_id',
      't_' || p_queue_name,
      't_' || p_queue_name
    ) using v_now
  loop
    -- Cancel all runs for this task
    execute format(
      'update durable.%I
          set state = ''cancelled'',
              claimed_by = null,
              claim_expires_at = null
        where task_id = $1
          and state not in (''completed'', ''failed'', ''cancelled'')',
      'r_' || p_queue_name
    ) using v_cancelled_task;

    -- Cleanup: delete waiters, emit event, cascade cancel children
    perform durable.cleanup_task_terminal(p_queue_name, v_cancelled_task, 'cancelled', null, true);
  end loop;

  -- Fail any run claims that have timed out.
  -- Lock tasks first to keep a consistent task -> run lock order.
  for v_expired_run in
    execute format(
      'select r.run_id,
              r.task_id,
              r.claimed_by,
              r.claim_expires_at,
              r.attempt
         from durable.%I r
         join durable.%I t on t.task_id = r.task_id
        where r.state = ''running''
          and r.claim_expires_at is not null
          and r.claim_expires_at <= $1
        for update of t skip locked',
      'r_' || p_queue_name,
      't_' || p_queue_name
    )
  using v_now
  loop
    perform durable.fail_run(
      p_queue_name,
      v_expired_run.task_id,
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
