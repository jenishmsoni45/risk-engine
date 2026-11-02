-- I5: every new decision row's snapshot holds exactly the 31 features of design.md 1.5, each a
-- whole number (requirements.md US5.2). `not valid` leaves Phase 2's rows, which hold only the
-- amount, as they were (D43); every row inserted or updated from here on is checked.
-- The list is risk/features/compute.py's FEATURE_NAMES; tests/unit/test_migration_files.py checks
-- that the two agree.
alter table decisions add constraint decisions_feature_names check (
    features ?& array[
        'out_count_1m', 'out_sum_1m', 'out_count_1h', 'out_sum_1h', 'out_count_24h', 'out_sum_24h',
        'in_count_1m', 'in_sum_1m', 'in_count_1h', 'in_sum_1h', 'in_count_24h', 'in_sum_24h',
        'dest_in_count_1m', 'dest_in_sum_1m', 'dest_in_count_1h', 'dest_in_sum_1h',
        'dest_in_count_24h', 'dest_in_sum_24h',
        'dest_out_count_1m', 'dest_out_sum_1m', 'dest_out_count_1h', 'dest_out_sum_1h',
        'dest_out_count_24h', 'dest_out_sum_24h',
        'attempt_count_1m', 'attempt_sum_1m', 'attempt_count_1h', 'attempt_sum_1h',
        'attempt_count_24h', 'attempt_sum_24h',
        'amount'
    ]
    and features - array[
        'out_count_1m', 'out_sum_1m', 'out_count_1h', 'out_sum_1h', 'out_count_24h', 'out_sum_24h',
        'in_count_1m', 'in_sum_1m', 'in_count_1h', 'in_sum_1h', 'in_count_24h', 'in_sum_24h',
        'dest_in_count_1m', 'dest_in_sum_1m', 'dest_in_count_1h', 'dest_in_sum_1h',
        'dest_in_count_24h', 'dest_in_sum_24h',
        'dest_out_count_1m', 'dest_out_sum_1m', 'dest_out_count_1h', 'dest_out_sum_1h',
        'dest_out_count_24h', 'dest_out_sum_24h',
        'attempt_count_1m', 'attempt_sum_1m', 'attempt_count_1h', 'attempt_sum_1h',
        'attempt_count_24h', 'attempt_sum_24h',
        'amount'
    ] = '{}'::jsonb
    and not jsonb_path_exists(features, 'strict $.* ? (@.type() != "number" || @.floor() != @)')
) not valid;
