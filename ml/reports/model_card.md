# Model card: lgbm-22f566b6643e

Written by `make model` (`python -m ml.train`) together with the model files; every number below comes from that script (D29). The model is scored in shadow next to the rules and never changes a decision (Phase 8, I8).

## Model

- Name: `lgbm-22f566b6643e`, from the first 12 hex digits of the model file's SHA-256 (D56b)
- Files: `ml/model/model.txt` (LightGBM's text format), SHA-256 `22f566b6643eb0130f7376738d80f50864db773902e46ee8715eabf99ac4865d`, and `ml/model/model.json` (name, inputs, threshold)
- LightGBM 4.7.0, trained in this repo's image only (D51.4), with fixed settings and no search (D56c): 300 rounds, `objective=binary`, `learning_rate=0.05`, `num_leaves=31`, `min_data_in_leaf=100`, `seed=7`, `num_threads=1`, `deterministic=True`, `force_row_wise=True`, `verbosity=-1`. No early stopping and no class weights.

## Data

- `PS_20174392719_1491204439457_log.csv`, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`: PaySim, a simulated month of mobile-money transactions, which stays in the git-ignored `data/` (CLAUDE.md)
- Loaded as ledger transfers (D32, D33, D40p): the TRANSFER and CASH_OUT rows of at least 1 cent, 2770393 transfers, of which fraud 8197 (0.30%)
- Label: PaySim's `isFraud`. `isFlaggedFraud` and the balance columns are never read (D35).

## Split (D38)

By day of the transfer's time, from 2026-01-01 UTC (D51.2). The model trains on the training days only; the threshold days only set the threshold, and the test days are scored once, for this card.

| set | days | transfers | per day | fraud | fraud share |
|---|---|---|---|---|---|
| train | 0 to 21 | 2667886 | 121267 | 5871 | 0.22% |
| threshold | 22 to 23 | 34563 | 17281 | 495 | 1.43% |
| test | 24 to 30 | 67944 | 9706 | 1831 | 2.69% |

## Inputs (D35, D54)

The 31 features of D19, computed by the shared feature module (`risk/features/`) from strictly earlier transfers, as the live guard computes them, in this order:

`out_count_1m`, `out_sum_1m`, `out_count_1h`, `out_sum_1h`, `out_count_24h`, `out_sum_24h`, `in_count_1m`, `in_sum_1m`, `in_count_1h`, `in_sum_1h`, `in_count_24h`, `in_sum_24h`, `dest_in_count_1m`, `dest_in_sum_1m`, `dest_in_count_1h`, `dest_in_sum_1h`, `dest_in_count_24h`, `dest_in_sum_24h`, `dest_out_count_1m`, `dest_out_sum_1m`, `dest_out_count_1h`, `dest_out_sum_1h`, `dest_out_count_24h`, `dest_out_sum_24h`, `attempt_count_1m`, `attempt_sum_1m`, `attempt_count_1h`, `attempt_sum_1h`, `attempt_count_24h`, `attempt_sum_24h`, `amount`

Not inputs: the hour of day (D54), the transaction type, the balance columns, the account names and `isFlaggedFraud` (D35).

## Threshold (D34, D55.3)

- Review capacity: at most 1% of the threshold days' 34563 transfers, that is 345.
- Threshold: a transfer is held when its score is at least `0.03012039931478348`. On the threshold days it holds 312 transfers (0.90%).

## On the test days

- PR-AUC, as average precision: 0.1685. A model that ranks at random scores about the test days' fraud share, 2.69%.
- At the threshold, which holds at most 1% of the threshold days' transfers:

| measure | value | from |
|---|---|---|
| transfers held | 1.26% | 858 of 67944 |
| recall | 17.80% | 326 of 1831 fraud transfers |
| fraud value caught | 72.73% | 215751833957 of 296664583364 cents |
| false-positive rate | 0.80% | 532 of 66113 other transfers |

## Limitations

- PaySim is simulated. Its fraud follows the simulator's pattern, so these numbers say how well the model finds that pattern, not how it would do on real fraud.
- PaySim's senders almost never repeat (`ml/reports/paysim_profile.md`, D54), so the sender-side features are nearly always empty and the model leans on the amount and the receiver's history. Where senders repeat, as in live traffic, it is untested.
- It sees only what the live guard sees: no balance columns, transaction type or hour of day (D35, D54). That makes it weaker on PaySim than a model that reads them, by design: those columns make PaySim artificially easy.
- Amounts are PaySim's unnamed currency read as CAD cents (D40p), so its splits on amounts and sums mean nothing for real CAD transfers.
- The split is by day (D38), and PaySim's days differ: the split table gives each set's transfers per day and fraud share. Where those differ from the training days', the test days measure the model on traffic unlike most of what it learned from, and the threshold, set on two days, holds a different share of the test days than of its own (above).
- Its scores rank transfers; they are not calibrated probabilities, and only their order is used.
- Fixed settings and one training run, with no search, retraining or drift handling (out of scope): a tuned or retrained model may do better.
