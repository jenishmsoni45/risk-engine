-- The ledger stub's own storage (design.md section 2, D40l): its Idempotency-Keys with their
-- saved responses, and its outbox. Like tally, only a posted transfer saves a key.
create schema stub;

create table stub.transfers (
    idempotency_key text primary key,
    request_hash text not null,
    response_status int not null,
    response_body jsonb not null,
    entry_id uuid not null unique,
    decision_id uuid,
    risk_check text not null,
    created_at timestamptz not null,
    -- The stub posts only transfers that asked the decision API: decided or fallback (D7).
    constraint transfers_risk_check check (risk_check in ('decided', 'fallback')),
    constraint transfers_decision_id check ((risk_check = 'decided') = (decision_id is not null))
);

create table stub.outbox (
    id bigserial primary key,
    entry_id uuid not null,
    payload jsonb not null,
    created_at timestamptz not null default now(),
    published_at timestamptz
);

-- The relay's query reads only unpublished rows, oldest first.
create index outbox_unpublished on stub.outbox (id) where published_at is null;

-- Line ids, numbered like tally's journal_lines ids.
create sequence stub.line_ids;
