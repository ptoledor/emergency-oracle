# Temporal candidate v4

## Decision

The candidate passes the current temporal promotion gate, but remains experimental and is not activated automatically.

## Evaluation protocol

- Direct rolling-origin evaluation for horizons H1 through H6.
- Candidate design and residual-model selection use development data and an inner temporal validation segment.
- Final reported metrics use the last 25% of evaluable origins: 269 origins and 1,614 origin-horizon pairs, starting on 2025-06-20.
- Risk remains the 90-day empirical probability because alternative classifiers were less well calibrated.

## Selected candidate

- Count base: 42-day rolling median.
- Residual correction: `GradientBoostingRegressor(loss="absolute_error")`.
- Correction scale: 1.0.
- Predictors: 58 origin-safe history, calendar, and forecast-weather variables.

## Holdout results

| Metric | Candidate | Baseline |
|---|---:|---:|
| MAE | 2.0795 | 2.1487 (rolling 28d) |
| RMSE | 2.5844 | 2.6185 |
| R2 | -0.0259 | -0.0532 |
| Brier | 0.1268 | 0.1268 (risk 90d) |
| ROC-AUC | 0.5677 | 0.5677 |

The candidate improves count MAE by 3.22%, improves all six horizons, and does not worsen Brier calibration. Therefore the configured gate passes.

## Important limitation

Several model families were iterated while observing aggregate performance on the same final historical period. Although v4 performs its parameter selection without using the holdout targets directly, the broader research direction was informed by earlier holdout results. Before operational promotion, confirm the advantage with future unseen observations or a fully nested walk-forward evaluation.

`active_models.json` was not modified.
