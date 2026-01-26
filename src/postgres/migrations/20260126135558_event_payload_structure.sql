-- Migration to enforce structured event payload format with 'inner' and 'metadata' keys

-- Update durable.cleanup_task_terminal to wrap the status in an 'inner' object
create or replace function durable.cleanup_task_terminal (
  p_queue_name text,
  p_task_id uuid,
  p_status text,             -- 'completed', 'failed', 'cancelled'
  p_payload jsonb default null,
  p_cascade_children boolean default false
)
  returns void
  language plpgsql
as $$
declare
  v_parent_task_id uuid;
begin
  -- Get parent_task_id for event emission
  execute format(
    'select parent_task_id from durable.%I where task_id = $1',
    't_' || p_queue_name
  ) into v_parent_task_id using p_task_id;

  -- Delete wait registrations for this task
  execute format(
    'delete from durable.%I where task_id = $1',
    'w_' || p_queue_name
  ) using p_task_id;

  -- Emit completion event for parent (if subtask)
  if v_parent_task_id is not null then
    perform durable.emit_event(
      p_queue_name,
      '$child:' || p_task_id::text,
      jsonb_build_object(
        'inner', jsonb_build_object('status', p_status) || coalesce(p_payload, '{}'::jsonb),
        'metadata', '{}'::jsonb
      )
    );
  end if;

  -- Cascade cancel children if requested
  if p_cascade_children then
    perform durable.cascade_cancel_children(p_queue_name, p_task_id);
  end if;
end;
$$;

-- Update durable.emit_event to validate payload structure
create or replace function durable.emit_event (
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
  v_inserted_count integer;
begin
  if p_event_name is null or length(trim(p_event_name)) = 0 then
    raise exception 'event_name must be provided';
  end if;

  -- Validate that p_payload only contains allowed keys ('inner' and 'metadata')
  if p_payload is not null and jsonb_typeof(p_payload) = 'object' then
    if exists (
      select 1
        from jsonb_object_keys(p_payload) as k
       where k not in ('inner', 'metadata')
    ) then
      raise exception 'p_payload may only contain ''inner'' and ''metadata'' keys';
    end if;
  end if;

  -- Insert the event into the events table (first-writer-wins).
  -- Subsequent emits for the same event are no-ops.
  -- We use DO UPDATE WHERE payload IS NULL to handle the case where await_event
  -- created a placeholder row before emit_event ran.
  execute format(
    'insert into durable.%I (event_name, payload, emitted_at)
     values ($1, $2, $3)
     on conflict (event_name) do update
        set payload = excluded.payload, emitted_at = excluded.emitted_at
      where durable.%I.payload is null',
    'e_' || p_queue_name,
    'e_' || p_queue_name
  ) using p_event_name, v_payload, v_now;

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
