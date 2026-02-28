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
