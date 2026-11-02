-- Messages on ledger.entries that fail parsing or validation (design.md 1.2 and section 2, D40i).
-- The consumer stores each one, counts it and commits past it, so one bad message never stalls a
-- partition. Its position is unique, so a message read again after a crash is stored once.
create table bad_events (
    id bigserial primary key,
    topic text not null,
    kafka_partition int not null,
    kafka_offset bigint not null,
    -- The raw bytes: a bad message may not be text, and a tombstone has no value.
    message_key bytea,
    message_value bytea,
    error text not null,
    -- The consumer's clock, for investigation only.
    received_at timestamptz not null default now(),
    constraint bad_events_position unique (topic, kafka_partition, kafka_offset)
);
