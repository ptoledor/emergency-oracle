# Emergency Oracle

Dashboard Streamlit que pronostica, para los próximos 6 días, los llamados de emergencia del Cuerpo de Bomberos de Talcahuano (CBT). Usa pronóstico meteorológico, calendario e historial reciente de llamados.

## Modelo activo

`signal_hydro_ensemble_v2` (definido en `03_model/saved_models/active_models.json`): promedio ponderado de cuatro XGBoost (error cuadrático, Poisson y mediana) sobre 118 variables, con la dispersión estirada ×1,15. La probabilidad de día de alta actividad (>7 llamados) se calibra sobre el conteo.

Validación walk-forward de 6 tramos × 120 días (2024-03-18 a 2026-04-16), día 1 del forecast:

| Predicción | MAE | R² | AUC (>7) |
|---|---|---|---|
| Modelo activo | 2,236 | 0,146 | 0,664 |
| Promedio de los últimos 30 días | 2,326 | 0,009 | 0,607 |

Error por día del forecast usando el clima pronosticado con la anticipación real (Open-Meteo Previous Runs):

| Día | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| MAE modelo | 2,241 | 2,305 | 2,266 | 2,256 | 2,266 | 2,329 |
| MAE promedio 30 días | 2,369 | 2,388 | 2,388 | 2,402 | 2,416 | 2,414 |

El modelo gana en los seis días, pero en el día 6 solo en 3 de 6 tramos.

Los datos terminan el 2026-04-16. Mientras no se actualicen, los días sin conteo observado se tratan como faltantes (igual que en el entrenamiento) y el dashboard lo indica en pantalla.

## Instalación

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

`requirements.txt` contiene solo lo que necesita la app (es lo que instala Streamlit Cloud). El scraper tiene sus propias dependencias en `01_scraper/requirements.txt`.

## Pipeline

```powershell
# 1. Compilar los CSV diarios del scraper
.\.venv\Scripts\python.exe 02_data\compile_scraped.py

# 2. Dataset diario: conteos, clima (Historical Forecast API), lluvia acumulada y calendario
.\.venv\Scripts\python.exe 02_data\clean_and_augment.py

# 3. Clima pronosticado con 0-6 días de anticipación, para evaluar cada día del forecast
.\.venv\Scripts\python.exe 05_research\fetch_previous_runs_weather.py

# 4. Selección y entrenamiento del modelo de conteo (walk-forward, forecast recursivo de 6 días)
.\.venv\Scripts\python.exe 03_model\train_model.py

# 5. Submodelos por tipo de emergencia
.\.venv\Scripts\python.exe 03_model\train_category_risk_models.py
.\.venv\Scripts\python.exe 03_model\train_category_composition_models.py

# 6. Predicción por consola y dashboard
.\.venv\Scripts\python.exe 04_predict\predict_tomorrow.py --real-tomorrow
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

`train_model.py` compara cinco candidatos declarados de antemano y guarda el elegido en `03_model/saved_models/count_model_v3/` (JSON). El modelo activo solo cambia con `--promote`, después de revisar las métricas (ver `.agents/AGENTS.md`). Resultados en `05_research/results/model_selection_v3/`.

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

`test_golden_forecast.py` congela la salida del forecast del dashboard en tres escenarios. Si un cambio de modelo o de forecast es intencional, se regenera con `python 05_research/models/tests/forecast_harness.py`.

## Estructura

- `01_scraper/`: scraper de los despachos publicados y sus CSV diarios.
- `02_data/`: compilación, limpieza, deduplicación y dataset diario.
- `03_model/`: entrenamiento (`train_model.py`), evaluación por horizonte (`horizon_evaluation.py`), submodelos y modelos guardados (`saved_models/`).
- `04_predict/`: predicción por consola.
- `05_research/`: calidad de datos, backtesting, resultados y tests.
- `dashboard.py`: app Streamlit (solo presentación).
- `forecasting.py`: forecast de 6 días compartido por el dashboard y `04_predict/predict_tomorrow.py`.
- `features.py`: única definición de las variables (clima, lluvia acumulada, calendario e historial), usada al entrenar, evaluar y servir.
- `model_components.py`, `activity_levels.py`, `hourly_peak.py`: clases de modelos y utilidades de presentación.
- `PATCH_NOTES.md`: registro de cambios.
