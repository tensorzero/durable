-- Add function to count unclaimed runs that are ready to be claimed.
-- Uses the same candidate logic as claim_task but without locking or updating.
create or replace function durable.count_unclaimed_ready_tasks (
  p_queue_name text
)
  returns bigint
  language plpgsql
as $$
declare
  v_now timestamptz := durable.current_time();
  v_count bigint;
begin
  execute format(
    'select count(*)
       from durable.%1$I r
       join durable.%2$I t on t.task_id = r.task_id
      where r.state in (''pending'', ''sleeping'')
        and t.state in (''pending'', ''sleeping'', ''running'')
        and r.available_at <= $1',
    'r_' || p_queue_name,
    't_' || p_queue_name
  ) into v_count using v_now;

  return v_count;
end;
$$;
