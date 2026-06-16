# Temporal Backtesting

Framework experimental aislado para evaluar predicciones diarias sin modificar
el pipeline de producción.

## Principios

- El holdout final queda bloqueado por fechas y no entra en folds de desarrollo.
- Los folds externos usan rolling-origin. Cada fold externo contiene folds internos
  construidos solo con su periodo de entrenamiento.
- Filas con target `NaN`, vacío o no numérico no entrenan ni puntúan. Se conservan
  en el plan como `unknown_target_indices`.
- Un target `0` sí es una observación válida y participa en entrenamiento y métricas.
- Se exige una fila por fecha; primero deben agregarse fechas duplicadas.
- Fechas posteriores a `holdout_end` quedan registradas como ignoradas.
- `evaluate_cv()` sirve para iterar. `evaluate_holdout()` abre el test final de forma
  explícita cuando configuración y modelo ya están congelados.

## Uso rápido

Desde raíz del repositorio:

```powershell
python -m 05_research.backtesting.cli `
  02_data/augmented_emergency_data.csv `
  --sep ';' `
  --date-column FECHA_DIA `
  --target-column EVENTOS `
  --holdout-start 2025-01-01 `
  --output 05_research/backtesting/results
```

Agregar `--evaluate-holdout` solo para evaluación final.

## API

```python
import sys
sys.path.insert(0, "05_research")

from backtesting import (
    BacktestEvaluator,
    PredictionBundle,
    TemporalBacktestConfig,
    build_backtest_plan,
)

config = TemporalBacktestConfig(
    date_column="FECHA_DIA",
    target_column="EVENTOS",
    holdout_start="2025-01-01",
    outer_min_train_size=365,
    outer_test_size=28,
    inner_min_train_size=180,
    inner_test_size=28,
)
plan = build_backtest_plan(data, config)

def model_predictor(train, test):
    model.fit(train[features], train["EVENTOS"])
    count = model.predict(test[features])
    probability = classifier.predict_proba(test[features])[:, 1]
    return PredictionBundle(count=count, probability=probability)

result = BacktestEvaluator(plan).evaluate_cv(
    predictors={"candidate": model_predictor}
)
result.export("05_research/backtesting/results", plan)
```

`plan.outer_folds[i].inner_folds` entrega los folds internos para selección de
variables e hiperparámetros dentro de cada fold externo.

## Baselines

- `historical_median`: mediana del entrenamiento disponible.
- `moving_average_28d`: media móvil de 28 observaciones conocidas anteriores.

La media móvil se actualiza con targets observados anteriores dentro del bloque de
prueba. Esto representa evaluación walk-forward diaria. Para un pronóstico de bloque
completo, usar `MovingAverageBaseline(update_with_observed=False)`.

Ambos baselines generan probabilidad de día crítico usando frecuencia histórica del
evento `target > critical_threshold`.

## Salidas

- `cv_predictions.csv`
- `cv_fold_metrics.csv`
- `cv_summary.csv`
- `cv_results.json`
- `backtest_plan.json`
- Equivalentes `holdout_*` al abrir el holdout.

Métricas de regresión: MAE, RMSE, R2, MASE y Poisson deviance.

Métricas de clasificación: ROC-AUC, PR-AUC, Brier, log-loss, precision, recall,
F1 y alertas por semana. ROC-AUC queda vacío si un fold contiene una sola clase.

## Tests

```powershell
python -m unittest discover -s 05_research/backtesting/tests -v
```
