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

El modelo gana en los seis días, pero en el día 6 solo en 3 de 6 tramos. Detalle en `05_research/results/forecast_horizons/`.

Los datos terminan el 2026-04-16. Mientras no se actualicen, el dashboard reemplaza el historial reciente por un promedio estacional y lo indica en pantalla.

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

# 2. Dataset diario con clima (Historical Forecast API) y calendario
.\.venv\Scripts\python.exe 02_data\clean_and_augment.py
.\.venv\Scripts\python.exe 05_research\weather_features.py --start-date 2022-02-03 --end-date 2026-04-17
.\.venv\Scripts\python.exe 05_research\fetch_weather_signal_v2.py

# 3. Modelo principal: predicciones fuera de muestra por objetivo y ensemble final
.\.venv\Scripts\python.exe 03_model\search_weather_v2.py
.\.venv\Scripts\python.exe 03_model\search_weather_objectives_v2.py
.\.venv\Scripts\python.exe 03_model\build_hydro_ensemble_candidate.py

# 4. Submodelos por tipo de emergencia
.\.venv\Scripts\python.exe 03_model\train_category_risk_models.py
.\.venv\Scripts\python.exe 03_model\train_category_composition_models.py

# 5. Evaluación por día del forecast (requiere 05_research\fetch_previous_runs_weather.py)
.\.venv\Scripts\python.exe 03_model\evaluate_forecast_horizons.py

# 6. Predicción por consola y dashboard
.\.venv\Scripts\python.exe 04_predict\predict_tomorrow.py --real-tomorrow
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```

Ningún script de entrenamiento cambia el modelo activo por sí solo: la promoción requiere aprobación explícita (ver `.agents/AGENTS.md`).

## Tests

```powershell
.\.venv\Scripts\python.exe -m pytest
```

`test_golden_forecast.py` congela la salida del forecast del dashboard en tres escenarios. Si un cambio de modelo o de forecast es intencional, se regenera con `python 05_research/models/tests/forecast_harness.py`.

## Estructura

- `01_scraper/`: scraper de los despachos publicados y sus CSV diarios.
- `02_data/`: compilación, limpieza, deduplicación y dataset diario.
- `03_model/`: entrenamiento, evaluación y modelos guardados (`saved_models/`).
- `04_predict/`: predicción por consola.
- `05_research/`: calidad de datos, backtesting, resultados y tests.
- `dashboard.py`: app Streamlit (solo presentación).
- `forecasting.py`: forecast de 6 días compartido por el dashboard y `04_predict/predict_tomorrow.py`.
- `features.py`: única definición de las variables (clima, lluvia acumulada, calendario e historial), usada al entrenar, evaluar y servir.
- `model_components.py`, `activity_levels.py`, `hourly_peak.py`: clases de modelos y utilidades de presentación.
- `PATCH_NOTES.md`: registro de cambios.
