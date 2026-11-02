-- The decision log and attempt log (design.md section 2). The constraints are I5's schema half:
-- a row without its rule set's version and hash, its fired rules or its features is refused.
-- The check that features holds all 31 names comes with the features, in Phase 3 (D43).
create table decisions (
    decision_id uuid primary key,
    request_id text not null unique,
    from_account_id uuid not null,
    to_account_id uuid not null,
    amount bigint not null,
    currency text not null,
    requested_at timestamptz not null,
    -- The service's clock, for latency and audit only; never a feature input (D14).
    decided_at timestamptz not null,
    decision text not null,
    rule_set_version int not null,
    rule_set_hash text not null,
    fired jsonb not null,
    features jsonb not null,
    feature_lag_ms bigint not null,
    constraint decisions_accounts_differ check (from_account_id <> to_account_id),
    constraint decisions_amount_positive check (amount > 0),
    constraint decisions_currency_code check (currency ~ '^[A-Z]{3}$'),
    constraint decisions_decision_value check (decision in ('allow', 'hold', 'block')),
    constraint decisions_rule_set_version check (rule_set_version >= 1),
    constraint decisions_rule_set_hash check (rule_set_hash ~ '^[0-9a-f]{64}$'),
    constraint decisions_fired_strings check (
        jsonb_typeof(fired) = 'array'
        and not jsonb_path_exists(fired, '$[*] ? (@.type() != "string")')
    ),
    constraint decisions_features_object check (jsonb_typeof(features) = 'object')
);

-- Serves rebuilding a sender's attempts from the log (design.md 3.5).
create index decisions_sender_requested_at on decisions (from_account_id, requested_at);
