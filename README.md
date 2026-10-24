# Emergency Oracle

Dashboard Streamlit que pronostica, para los próximos 6 días, los llamados de emergencia del Cuerpo de Bomberos de Talcahuano (CBT). Usa pronóstico meteorológico y calendario.

## Modelo activo

`count_model_v3` (definido en `03_model/saved_models/active_models.json`): un XGBoost con objetivo Poisson sobre 100 variables de clima pronosticado y calendario, sin historial de llamados. Se guarda en JSON y trae, para cada día del forecast, un intervalo del 80% y la probabilidad calibrada de más de 7 llamados.

Validación walk-forward de 6 tramos × 120 días (2024-03-18 a 2026-04-16), con el forecast recursivo de 6 días y el clima pronosticado con la anticipación real (Open-Meteo Previous Runs):

| Día | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| MAE modelo | 2,261 | 2,282 | 2,260 | 2,258 | 2,257 | 2,314 |
| MAE promedio 30 días | 2,369 | 2,388 | 2,388 | 2,402 | 2,416 | 2,414 |
| AUC (>7 llamados) | 0,647 | 0,617 | 0,626 | 0,625 | 0,653 | 0,605 |

Le gana al promedio de 30 días en los seis días (P ≥ 0,987). Fue elegido entre cinco candidatos declarados de antemano por ser el más simple que cumple la regla de selección; detalle en `05_research/results/model_selection_v3/`.

Los datos terminan el 2026-04-16. El conteo total no depende del historial de llamados; la composición por tipo de emergencia sí, y trata los días sin conteo como faltantes.

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
