-- Enforce lease validity on checkpoint/heartbeat by raising AB002 when expired.

create or replace function durable.set_task_checkpoint_state (
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
  v_task_state text;
  v_attempt integer;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  -- Lock task row (consistent task -> run lock order)
  execute format(
    'select state from durable.%I where task_id = $1 for update',
    't_' || p_queue_name
  )
  into v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  -- if the task was cancelled raise a special error the caller can catch to terminate
  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- Validate lease and lock run row by conditionally updating it.
  if p_extend_claim_by is not null and p_extend_claim_by > 0 then
    execute format(
      'update durable.%I
          set claim_expires_at = $3 + make_interval(secs => $4)
        where run_id = $1
          and task_id = $2
          and state = ''running''
          and claim_expires_at is not null
          and claim_expires_at > $3
        returning attempt',
      'r_' || p_queue_name
    )
    into v_attempt
    using p_owner_run, p_task_id, v_now, p_extend_claim_by;
  else
    -- Touch row to lock it + validate lease even when not extending.
    -- If the run has been cancelled then this row's state will be set to
    -- 'failed' and this check will return null
    execute format(
      'update durable.%I
          set claim_expires_at = claim_expires_at
        where run_id = $1
          and task_id = $2
          and state = ''running''
          and claim_expires_at is not null
          and claim_expires_at > $3
        returning attempt',
      'r_' || p_queue_name
    )
    into v_attempt
    using p_owner_run, p_task_id, v_now;
  end if;

  -- If the check above returns null then we shouldn't be running it anymore.
  if v_attempt is null then
    raise exception sqlstate 'AB002' using message = 'Task lease expired';
  end if;

  execute format(
    'insert into durable.%I (task_id, checkpoint_name, state, owner_run_id, updated_at)
     values ($1, $2, $3, $4, $5)
     on conflict (task_id, checkpoint_name)
     do update set state = excluded.state,
                   owner_run_id = excluded.owner_run_id,
                   updated_at = excluded.updated_at
     where $6 >= coalesce(
       (select r.attempt
          from durable.%I r
         where r.run_id = durable.%I.owner_run_id),
       $6
     )',
    'c_' || p_queue_name,
    'r_' || p_queue_name,
    'c_' || p_queue_name
  )
  using p_task_id, p_step_name, p_state, p_owner_run, v_now, v_attempt;
end;
$$;

create or replace function durable.extend_claim (
  p_queue_name text,
  p_run_id uuid,
  p_extend_by integer
)
  returns void
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_task_id uuid;
  v_task_state text;
  v_attempt integer;
begin
  -- Lock task row first (consistent task -> run lock order)
  execute format(
    'select task_id, state
       from durable.%I
      where task_id = (select task_id from durable.%I where run_id = $1)
      for update',
    't_' || p_queue_name,
    'r_' || p_queue_name
  )
  into v_task_id, v_task_state
  using p_run_id;

  if v_task_state is null then
    raise exception 'Run "%" not found in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  execute format(
    'update durable.%I
        set claim_expires_at = $2 + make_interval(secs => $3)
      where run_id = $1
        and task_id = $4
        and state = ''running''
        and claim_expires_at is not null
        and claim_expires_at > $2
      returning attempt',
    'r_' || p_queue_name
  )
  into v_attempt
  using p_run_id, v_now, p_extend_by, v_task_id;

  if v_attempt is null then
    raise exception sqlstate 'AB002' using message = 'Task lease expired';
  end if;
end;
$$;
