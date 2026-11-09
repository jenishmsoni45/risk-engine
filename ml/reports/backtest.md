# Backtest: the PaySim rules against the model

Written by `make backtest` (`python -m ml.backtest`) together with `ml/reports/backtest.json`; every number below comes from that script (D29). design.md section 8 and D40q set what it compares, and D57.3 and D59 set the model's lines.

- Data: `PS_20174392719_1491204439457_log.csv`, SHA-256 `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`
- Test days: days 24 to 30 (D38), 67944 transfers, of which fraud 1831. Neither the rules nor the model saw them.
- Rules: `ml/rules/paysim.yaml`, v2, SHA-256 `621f8ade95700cdcfc65935f372c0f2a6af88941b7b0b38f1ad0502ca27b865e`, through the service's own evaluation function. A hold and a block both count as held (D54).
- Model: `lgbm-22f566b6643e` from `ml/model/`, checked as the decision API checks it (D58b).

## On the test days

|  | held | recall | fraud value caught | false-positive rate |
|---|---|---|---|---|
| PaySim rules (v2), at their own hold rate | 1.16% (786) | 16.71% (306 of 1831) | 71.36% | 0.73% (480) |
| model `lgbm-22f566b6643e`, at the rules' hold rate and at 1%: above a tie of 199 | 0.97% (659) | 14.69% (269 of 1831) | 67.50% | 0.59% (390) |
| model `lgbm-22f566b6643e`, at the rules' hold rate and at 1%: with the tie of 199 | 1.26% (858) | 17.80% (326 of 1831) | 72.73% | 0.80% (532) |

- The rules hold what they hold; the model then holds as many of the test days' transfers, those it scores highest, and then 1% of them. Where transfers sharing the model's score at a line straddle that count, no line holds exactly that many: the model is shown above the tie, holding fewer, and with it, holding more, so the count it is compared at lies between the two. No tie is broken (D59).
- Recall is fraud held over all fraud; fraud value caught is the fraud amounts held over all fraud amounts; the false-positive rate is other transfers held over all other transfers.
- The model's lines are drawn on the test days' own scores, so that both sides hold the same number. The threshold the model ships with, set on the threshold days, and what it holds on the test days are in `ml/reports/model_card.md`.
- PaySim is simulated, and its amounts are read as CAD cents (D40p); the model card's limitations apply here too.
