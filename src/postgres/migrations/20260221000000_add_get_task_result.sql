-- Returns the current status and result of a task.
-- For completed tasks, includes the output payload.
-- For failed tasks, includes the error from the latest run via last_attempt_run.
-- Returns NULL (empty result set) if the task doesn't exist.
create function durable.get_task_result (
  p_queue_name text,
  p_task_id uuid
)
  returns table (
    task_id uuid,
    state text,
    completed_payload jsonb,
    failure_reason jsonb
  )
  language plpgsql
as $$
begin
  return query execute format(
    'select t.task_id,
            t.state,
            t.completed_payload,
            r.failure_reason
       from durable.%I t
       left join durable.%I r on r.run_id = t.last_attempt_run
      where t.task_id = $1',
    't_' || p_queue_name,
    'r_' || p_queue_name
  ) using p_task_id;
end;
$$;
