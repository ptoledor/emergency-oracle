# Experimental model benchmark

This directory is isolated from `03_model/saved_models`. It reads production
metadata only to reuse the current feature list and never writes or replaces a
production model.

## Models

Count benchmark:

- `GradientBoostingRegressor(loss="absolute_error")`
- `HistGradientBoostingRegressor(loss="poisson")`
- `PoissonRegressor`
- `TweedieRegressor(power=1.5)`
- NB2 Negative Binomial GLM implemented with SciPy

Risk benchmark:

- Current Random Forest configuration, uncalibrated
- Random Forest with sigmoid calibration
- Random Forest with isotonic calibration
- Probability of `EVENTOS > critical_threshold` derived from Negative Binomial

All reported predictions are expanding-window `TimeSeriesSplit` predictions.
For calibrated classifiers, the calibrator is fitted on a second temporal OOF
loop contained entirely inside each outer training block.

## Zero-count days

The default is `--zero-policy include`. A verified day with zero emergencies is
valid information and Poisson, Tweedie, HistGradientBoosting Poisson and
Negative Binomial all support it. A missing or failed scrape must remain
missing upstream; it must not be silently converted to zero.

Use `--zero-policy exclude` only as a sensitivity experiment. Removing genuine
zeros changes the estimand to "count conditional on at least one emergency".

## Run

From the repository root:

```powershell
.\.venv\Scripts\python.exe 05_research\models\benchmark.py
```

Use a different dataset and result directory:

```powershell
.\.venv\Scripts\python.exe 05_research\models\benchmark.py `
  --dataset 02_data\augmented_emergency_data.csv `
  --output-dir 05_research\models\results\run_01 `
  --critical-threshold 7 `
  --zero-policy include
```

Feature sources:

- `metadata`: current production metadata feature list, default.
- `climatic`: numeric weather features without target-derived columns.
- `calendar_lags`: safe numeric fields including calendar and historical lags.
- `all_safe`: numeric fields except target and same-day category targets.
- Explicit list: `--feature-source "TEMP_MAX,TEMP_MIN,LLUVIA"`.

## Outputs

- `predictions.csv`: temporal OOF predictions and probabilities by date.
- `count_metrics.csv`: aggregate count metrics.
- `classification_metrics.csv`: aggregate discrimination and calibration metrics.
- `fold_metrics.csv`: metrics for each temporal fold.
- `run_config.json`: complete configuration, selected features and data counts.

Primary count metrics are MAE, RMSE, R2, Poisson deviance and MASE. Primary
risk metrics are average precision, ROC-AUC, Brier score, log loss, ECE,
precision, recall and F1. Accuracy should not be interpreted alone because
critical days are uncommon.

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover `
  -s 05_research\models\tests -p "test_*.py" -v
```

