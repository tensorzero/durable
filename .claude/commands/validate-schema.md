# Validate Schema

Run the schema validation script to verify that `sql/schema.sql` matches the result of applying all migrations.

## How It Works

The validation script (`scripts/validate-schema`) uses testcontainers to:

1. Start two PostgreSQL 16 containers
2. Apply `sql/schema.sql` directly to container A
3. Apply all migrations in `src/postgres/migrations/` to container B
4. Dump both schemas using `pg_dump --schema-only --schema=durable`
5. Compare the normalized dumps
6. Report pass/fail with a diff on failure

## Running Validation

```bash
./scripts/validate-schema
```

## Requirements

- Docker must be running
- `uv` must be installed (the script uses inline dependencies)

## When to Run

- After creating a new migration with `/make-migration`
- Before committing schema changes
- CI runs this automatically on pull requests

## Troubleshooting

If validation fails, the output will show a unified diff between:
- `schema.sql` - What the schema file defines
- `migrations` - What applying all migrations produces

Common causes of failure:
- Forgot to update schema.sql after adding a migration
- Migration has different SQL than what's in schema.sql
- Migration includes logic that shouldn't be in schema.sql (like `DO $$ ... END $$` blocks for existing queues)
