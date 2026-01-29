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
  v_new_attempt integer;
  v_task_state text;
  v_run_state text;
  v_claim_expires_at timestamptz;
begin
  if p_step_name is null or length(trim(p_step_name)) = 0 then
    raise exception 'step_name must be provided';
  end if;

  -- get the current attempt number and task/run state
  execute format(
    'select r.attempt, t.state, r.state, r.claim_expires_at
       from durable.%I r
       join durable.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_new_attempt, v_task_state, v_run_state, v_claim_expires_at
  using p_owner_run;

  if v_new_attempt is null then
    raise exception 'Run "%" not found for checkpoint', p_owner_run;
  end if;

  -- if the task was cancelled raise a special error the caller can catch to terminate
  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- if the lease is expired or run is no longer running, terminate
  if v_run_state <> 'running'
     or v_claim_expires_at is null
     or v_claim_expires_at <= v_now then
    raise exception sqlstate 'AB002' using message = 'Task lease expired';
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
  ) using p_task_id, p_step_name, p_state, p_owner_run, v_now, v_new_attempt;
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
  v_task_state text;
  v_run_state text;
  v_claim_expires_at timestamptz;
begin
  execute format(
    'select t.state, r.state, r.claim_expires_at
       from durable.%I r
       join durable.%I t on t.task_id = r.task_id
      where r.run_id = $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  )
  into v_task_state, v_run_state, v_claim_expires_at
  using p_run_id;

  if v_task_state = 'cancelled' then
    raise exception sqlstate 'AB001' using message = 'Task has been cancelled';
  end if;

  -- if the lease is expired or run is no longer running, terminate
  if v_run_state <> 'running'
     or v_claim_expires_at is null
     or v_claim_expires_at <= v_now then
    raise exception sqlstate 'AB002' using message = 'Task lease expired';
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
