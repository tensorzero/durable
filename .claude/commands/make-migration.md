# Create Migration

Generate a new migration file based on changes to `sql/schema.sql`.

## Arguments

- `$ARGUMENTS` - The migration name (e.g., "add_user_table", "update_claim_indexes")

## Workflow

1. **Read the current schema**: Read `sql/schema.sql` to understand the current desired state.

2. **Read existing migrations**: Read all files in `src/postgres/migrations/` to understand what's already been migrated.

3. **Determine the changes**: Compare the schema.sql against what the migrations would produce. Identify:
   - New tables, columns, indexes, or constraints to add
   - Modified functions or triggers
   - Any DROP statements needed (be careful with these)

4. **Generate the migration SQL**: Create SQL that transforms the database from the current migrated state to the new schema.sql state.
   - For new tables/indexes: Use `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`
   - For function updates: Use `CREATE OR REPLACE FUNCTION`
   - For existing queues that need new indexes: Include a `DO $$ ... END $$` block that applies changes to existing queue tables

5. **Create the migration file**: Generate a timestamped migration file:
   - Filename format: `YYYYMMDDHHMMSS_<name>.sql`
   - Place in: `src/postgres/migrations/`
   - Use current UTC time for the timestamp

6. **Run validation**: Execute `./scripts/validate-schema` to verify the migration produces the correct schema.

## Example

If the user has added a new index to `ensure_queue_tables` in schema.sql:

```sql
-- New migration: 20260115143022_add_new_index.sql

-- Update ensure_queue_tables to include the new index for future queues
create or replace function durable.ensure_queue_tables (p_queue_name text)
  returns void
  language plpgsql
as $$
begin
  -- ... (full function with new index)
end;
$$;

-- Apply the new index to existing queues
do $$
declare
  v_queue text;
begin
  for v_queue in select queue_name from durable.queues loop
    execute format(
      'create index if not exists %I on durable.%I (...)',
      ('t_' || v_queue) || '_new_idx',
      't_' || v_queue
    );
  end loop;
end;
$$;
```

## Important Notes

- Always use `IF NOT EXISTS` for idempotent migrations
- For function changes, the full function must be included (not just the diff)
- The `DO $$ ... END $$` block for existing queues should NOT be in schema.sql (it's migration-only logic)
- Run validation after creating the migration to ensure schema.sql matches
