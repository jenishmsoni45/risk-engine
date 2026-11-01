# Migrations

Numbered SQL files applied in order by `python -m risk.migrate` (the one-shot `migrate` Compose
service, D40w). Name each file `NNNN_short_name.sql`, starting at `0001`, with a version no other
file uses. Each file runs in one transaction with its row in `schema_migrations`. A merged
migration is never edited: a change is a new file.

The first migration arrives in Phase 2 (the `decisions` table).
