# PaySim profile

Written by `make profile` (`python -m ml.profile`); every number below comes from that script (D29). It confirms the sample before Phase 7 (D32).

- File: `PS_20174392719_1491204439457_log.csv`, 493534783 bytes, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`
- Rows: 6362620; steps 1 to 743
- Step 1 starts at 2026-01-01T00:00:00Z (D51.2)

## Rows by type

| type | rows | isFraud | isFlaggedFraud | amount rounds to 0 cents |
|---|---|---|---|---|
| CASH_IN | 1399284 | 0 | 0 | 0 |
| CASH_OUT | 2237500 | 4116 | 0 | 16 |
| DEBIT | 41432 | 0 | 0 | 0 |
| PAYMENT | 2151495 | 0 | 0 | 0 |
| TRANSFER | 532909 | 4097 | 16 | 0 |

Rows of the loaded types (D32: TRANSFER, CASH_OUT) that send to the same account, which the loader refuses (D52e): 0.

## The sample

- Transfers loaded: 2770393, of which fraud 8197 (0.30%)
- Skipped because the amount rounds to 0 cents (D52f): 16

## How often accounts repeat

Transfers per account over the whole sample.

| side | accounts | with 1 | with 2 | with 3 to 9 | with 10+ |
|---|---|---|---|---|---|
| senders | 2768614 | 2766838 accounts, 2766838 transfers | 1773 accounts, 3546 transfers | 3 accounts, 9 transfers | 0 accounts, 0 transfers |
| receivers | 509561 | 128330 accounts, 128330 transfers | 79448 accounts, 158896 transfers | 215006 accounts, 1102783 transfers | 86777 accounts, 1380384 transfers |

- Fraud transfers whose sender sends more than once: 16 (0.20%)
- Fraud transfers whose receiver receives more than once: 5099 (62.21%)

## Which feature families have members

Share of transfers whose window held at least one member at the transfer's time, from the offline pipeline (D19).

| family | 1m, all | 1h, all | 24h, all | 24h, fraud | 24h, not fraud |
|---|---|---|---|---|---|
| out | 0.00% | 0.00% | 0.01% | 0.02% | 0.01% |
| in | 0.00% | 0.00% | 0.01% | 0.00% | 0.01% |
| dest_in | 0.36% | 10.11% | 48.00% | 9.72% | 48.11% |
| dest_out | 0.00% | 0.00% | 0.01% | 0.00% | 0.01% |
| attempt | 0.00% | 0.00% | 0.01% | 0.02% | 0.01% |

## Amounts, in cents

|  | p50 | p90 | p99 | max | ≥ 200,000 | ≥ 1,000,000 |
|---|---|---|---|---|---|---|
| all transfers | 17126244 | 54486979 | 265015479 | 9244551664 | 99.44% | 97.15% |

200,000 and 1,000,000 are the starter rules' amount thresholds (D20).

## Hour of day (D51.2)

| hour (UTC) | transfers | fraud |
|---|---|---|
| 0 | 6005 | 358 |
| 1 | 1921 | 371 |
| 2 | 779 | 325 |
| 3 | 512 | 274 |
| 4 | 632 | 366 |
| 5 | 903 | 357 |
| 6 | 2542 | 328 |
| 7 | 10302 | 368 |
| 8 | 120028 | 341 |
| 9 | 187020 | 371 |
| 10 | 198240 | 324 |
| 11 | 218561 | 339 |
| 12 | 213906 | 346 |
| 13 | 199362 | 353 |
| 14 | 188135 | 340 |
| 15 | 200906 | 345 |
| 16 | 202803 | 350 |
| 17 | 270681 | 343 |
| 18 | 299933 | 342 |
| 19 | 238888 | 339 |
| 20 | 90925 | 346 |
| 21 | 61162 | 349 |
| 22 | 38033 | 322 |
| 23 | 18214 | 300 |

## The starter rules on PaySim (rule set v1)

What the starter rule set (D20) decides for every loaded transfer, from its offline features. Its thresholds assume CAD cents; PaySim's currency is unnamed. Over every day of the sample, not only Phase 7's test days.

| decision | transfers | share | of fraud | of not fraud |
|---|---|---|---|---|
| allow | 78822 | 2.85% | 3.20% | 2.84% |
| hold | 2691571 | 97.15% | 96.80% | 97.16% |
| block | 0 | 0.00% | 0.00% | 0.00% |

| rule | fired on | share | of fraud | of not fraud |
|---|---|---|---|---|
| velocity_1m | 0 | 0.00% | 0.00% | 0.00% |
| attempt_burst_1m | 0 | 0.00% | 0.00% | 0.00% |
| daily_outflow_24h | 223 | 0.01% | 0.01% | 0.01% |
| large_transfer | 2691568 | 97.15% | 96.80% | 97.16% |
| dest_fan_in_1h | 1 | 0.00% | 0.00% | 0.00% |
| pass_through_1h | 16 | 0.00% | 0.00% | 0.00% |
