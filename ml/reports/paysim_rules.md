# The PaySim rule set

Written by `make paysim-rules` (`python -m ml.paysim_rules`) together with the rule file; every number below comes from that script (D29). D54 sets what it does: D20's rules with every threshold on a cent feature multiplied by the smallest integer factor at which the set holds or blocks at most 1% of the training days' transfers. Only the backtest reads the file (D40q).

- Data: `PS_20174392719_1491204439457_log.csv`, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`
- Rule file: `ml/rules/paysim.yaml`, v2, SHA-256 `621f8ade95700cdcfc65935f372c0f2a6af88941b7b0b38f1ad0502ca27b865e`
- Days 0 to 30; the training days are 0 to 21 (D38), with 2667886 transfers, of which fraud 5871
- Factor: 265. The set may stop at most 26678 training transfers (1%); at 264 it would stop 26852.

## Thresholds

| rule | action | condition | v1 (D20) | PaySim (v2) |
|---|---|---|---|---|
| velocity_1m | hold | `out_count_1m >=` | 5 | 5 |
| attempt_burst_1m | block | `attempt_count_1m >=` | 10 | 10 |
| daily_outflow_24h | hold | `out_sum_24h >=` | 2500000 | 662500000 |
| large_transfer | hold | `amount >=` | 1000000 | 265000000 |
| dest_fan_in_1h | hold | `dest_in_count_1h >=` | 20 | 20 |
| pass_through_1h | hold | `in_sum_1h >=` | 200000 | 53000000 |
| pass_through_1h | hold | `amount >=` | 200000 | 53000000 |

## On the training days

The test days aren't evaluated here, so they stay unseen until the backtest (D38).

| decision | transfers | share | of fraud | of not fraud |
|---|---|---|---|---|
| allow | 2641214 | 99.00% | 83.68% | 99.03% |
| hold | 26672 | 1.00% | 16.32% | 0.97% |
| block | 0 | 0.00% | 0.00% | 0.00% |

| rule | fired on | share | of fraud | of not fraud |
|---|---|---|---|---|
| velocity_1m | 0 | 0.00% | 0.00% | 0.00% |
| attempt_burst_1m | 0 | 0.00% | 0.00% | 0.00% |
| daily_outflow_24h | 2 | 0.00% | 0.00% | 0.00% |
| large_transfer | 26669 | 1.00% | 16.32% | 0.97% |
| dest_fan_in_1h | 1 | 0.00% | 0.00% | 0.00% |
| pass_through_1h | 0 | 0.00% | 0.00% | 0.00% |
