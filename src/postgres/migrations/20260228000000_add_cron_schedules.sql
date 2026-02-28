-- Cron schedule registry table.
-- Stores metadata for schedules managed by pg_cron via the Durable client API.
-- This table always exists (even without pg_cron installed), so list_schedules() works regardless.

CREATE TABLE IF NOT EXISTS durable.cron_schedules (
  schedule_name TEXT NOT NULL,
  queue_name TEXT NOT NULL,
  task_name TEXT NOT NULL,
  cron_expression TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  spawn_options JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  pgcron_job_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_name, schedule_name)
);

CREATE INDEX IF NOT EXISTS idx_cron_schedules_metadata
  ON durable.cron_schedules USING gin (metadata);

CREATE INDEX IF NOT EXISTS idx_cron_schedules_task_name
  ON durable.cron_schedules (queue_name, task_name);

-- Override drop_queue to clean up cron schedules and their pg_cron jobs.
CREATE OR REPLACE FUNCTION durable.drop_queue (p_queue_name text)
  returns void
  language plpgsql
as $$
declare
  v_existing_queue text;
  v_rec record;
  v_jobid bigint;
begin
  select queue_name into v_existing_queue
  from durable.queues
  where queue_name = p_queue_name;

  if v_existing_queue is null then
    return;
  end if;

  -- Clean up any cron schedules associated with this queue
  for v_rec in
    select pgcron_job_name
    from durable.cron_schedules
    where queue_name = p_queue_name
  loop
    begin
      select jobid into v_jobid
      from cron.job
      where jobname = v_rec.pgcron_job_name;

      if v_jobid is not null then
        perform cron.unschedule(v_jobid);
      end if;
    exception when others then
      -- pg_cron may not be installed; ignore errors
      null;
    end;
  end loop;

  delete from durable.cron_schedules where queue_name = p_queue_name;

  -- Existing drop logic
  execute format('drop table if exists durable.%I cascade', 'w_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'e_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'c_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 'r_' || p_queue_name);
  execute format('drop table if exists durable.%I cascade', 't_' || p_queue_name);

  delete from durable.queues where queue_name = p_queue_name;
end;
$$;
