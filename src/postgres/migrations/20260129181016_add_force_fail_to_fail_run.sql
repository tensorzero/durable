-- Add p_force_fail parameter to durable.fail_run function
-- When p_force_fail is true, the retry policy and p_retry_at are ignored,
-- and the task is immediately failed (as though it had reached the max retries)

drop function if exists durable.fail_run(text, uuid, uuid, jsonb, timestamptz);

create function durable.fail_run (
  p_queue_name text,
  p_task_id uuid,
  p_run_id uuid,
  p_reason jsonb,
  p_retry_at timestamptz default null,
  p_force_fail boolean default false
)
  returns void
  language plpgsql
as $$
declare
  v_run_task_id uuid;
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
begin
  -- Lock task first to keep a consistent task -> run lock order.
  execute format(
    'select retry_strategy, max_attempts, first_started_at, cancellation, state
       from durable.%I
      where task_id = $1
      for update',
    't_' || p_queue_name
  )
  into v_retry_strategy, v_max_attempts, v_first_started, v_cancellation, v_task_state
  using p_task_id;

  if v_task_state is null then
    raise exception 'Task "%" not found in queue "%"', p_task_id, p_queue_name;
  end if;

  -- Lock run after task and ensure it's still eligible
  execute format(
    'select task_id, attempt
       from durable.%I
      where run_id = $1
        and state in (''running'', ''sleeping'')
      for update',
    'r_' || p_queue_name
  )
  into v_run_task_id, v_attempt
  using p_run_id;

  if v_run_task_id is null then
    raise exception 'Run "%" cannot be failed in queue "%"', p_run_id, p_queue_name;
  end if;

  if v_run_task_id <> p_task_id then
    raise exception 'Run "%" does not belong to task "%"', p_run_id, p_task_id;
  end if;

  -- Actually fail the run
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

  -- Compute the next retry time, unless we're forcing a failure
  if (not p_force_fail) and (v_max_attempts is null or v_next_attempt <= v_max_attempts) then
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

    -- Set up the new run if not cancelling
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
      using v_new_run_id, p_task_id, v_next_attempt, v_next_available;
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
  ) using p_task_id, v_task_state_after, v_recorded_attempt, v_last_attempt_run, v_cancelled_at;

  -- Delete wait registrations for this run
  execute format(
    'delete from durable.%I where run_id = $1',
    'w_' || p_queue_name
  ) using p_run_id;

  -- If task reached terminal state, cleanup (emit event, cascade cancel)
  if v_task_state_after in ('failed', 'cancelled') then
    perform durable.cleanup_task_terminal(
      p_queue_name,
      p_task_id,
      v_task_state_after,
      jsonb_build_object('error', p_reason),
      true  -- cascade cancel children
    );
  end if;
end;
$$;
