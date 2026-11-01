# Viper Talcahuano / Concepción — incorporación al entrenamiento (2026-11-01)

## Datos

Fuentes: `01_scraper/talcahuano/*.json` (4.880 ítems, 2024-06-23 → 2026-10-31) y
`01_scraper/concepcion/*.json` (6.800 ítems, 2024-08-09 → 2026-10-31; faltan las páginas 69–73).

Hallazgos del cruce con los tweets (`02_data/build_viper_dataset.py`):

- `date` de Viper está en **UTC−3 fijo** (no sigue el horario de invierno); `dispatch.timestamp` lo confirma.
- Corrigiendo eso, **3.399 de 3.400 tweets desde 2024-06-23 tienen su par en Viper** (mismo texto, ±15 min): el bot de Twitter era un espejo del feed.
- Viper agrega ~200 mensajes de emergencia que no se tuitearon, concentrados en **días de temporal** (2024-08-01: 23 → 118; 2025-06-14/15: 16/23 → 40/42). El target de Twitter subcontaba justo los días más altos.
- `dispatch.id` identifica la emergencia. Los mensajes `SALE …` y `SEGUNDA/TERCERA ALARMA` reutilizan el id: **contar mensajes sin esos prefijos = emergencias únicas** (841 de 844 días idénticos), lo que permite reconstruir el target de incidentes también en 2022–2024.
- `6-6 a …` son reclasificaciones (nunca se tuitearon) y hay repeticiones idénticas del feed a segundos: ambas se excluyen.
- Huecos del feed > 30 h ⇒ los días tocados quedan `coverage_unknown` (54 días). Varios "ceros observados" del target anterior caían en esos huecos.

Dataset resultante: tweets hasta 2024-06-23 + Viper desde 2024-06-24 → **1.626 días** (antes 1.444), hasta 2026-10-31.

- `02_data/augmented_emergency_data_viper_mensajes.csv` — misma definición que el dashboard (cada mensaje cuenta).
- `02_data/augmented_emergency_data_viper_incidentes.csv` — emergencias únicas (sin SALE/ALARMA).

Clima: `02_data/extend_weather.py` extendió los tres caches hasta 2026-10-31 (solo agrega filas). Además se re-descargó el historial completo (`05_research/data/*_fresh.csv`) porque Open-Meteo re-procesó `weather_code` y humedad de suelo; usar una sola versión mejora todo (ver abajo). La humedad de suelo se excluyó porque el serving no la calcula.

## Resultados — Talcahuano, target mensajes

**Holdout real 2026-04-17 → 2026-10-31 (189 días, ningún modelo los vio)** — `holdout_mensajes.csv`

| Modelo | MAE | RMSE | R² | Deviance | AUC >7 |
|---|---|---|---|---|---|
| Producción congelado (`signal_hydro_ensemble_v2`) | 2,858 | 5,721 | 0,157 | 2,839 | 0,624 |
| Misma arquitectura, historia tweets (sin Viper) | 2,840 | 5,721 | 0,157 | 2,832 | 0,620 |
| Misma arquitectura, historia Viper, entrenado al 16-abr | 2,645 | 4,223 | 0,541 | 2,119 | 0,640 |
| Misma arquitectura, historia Viper, refit mensual | **2,554** | **3,997** | **0,588** | 1,998 | 0,611 |
| v3 (calendario+GLM), refit mensual | 2,557 | 4,130 | 0,561 | **1,943** | 0,624 |
| Recortar el target a 25 para entrenar | 2,747 | 5,371 | 0,257 | 2,523 | 0,605 |
| Media móvil 28 días | 3,087 | 6,354 | −0,040 | 3,813 | 0,483 |

El temporal del 2026-07-16 (67 llamados) explica buena parte: producción predijo 10,8; reentrenado con Viper, 44. En días ≤12 el refit mensual empata (MAE 2,077 → 2,055); el entrenamiento único al 16-abr empeora levemente (2,181). Recortar extremos destruye la ganancia: el modelo necesita los temporales bien contados.

**Walk-forward 6×120 (2024-09-18 → 2026-10-31, 720 días, target Viper)** — `wf_mensajes.csv`

| Entrenado con | MAE | RMSE | R² | Deviance |
|---|---|---|---|---|
| Target de Twitter (misma arquitectura) | 2,369 | 3,986 | 0,172 | 1,848 |
| Target Viper | **2,331** | **3,408** | **0,395** | **1,670** |

Con clima mezclado (cache) el target Viper daba 2,387 / 3,554 / 0,342: la versión única de clima suma otro ~2–4%.

Arquitectura: v3 vs oficial queda dentro del ruido (bootstrap 57% MAE, 33% MSE; la sola semilla mueve ±0,015 de MAE). Se prefiere la oficial: mismas 118 variables que producción.

## Target incidentes únicos — `wf_incidentes.csv`

MAE 1,813 / RMSE 2,819 / R² **0,431** (media 4,53; error relativo 40% vs 44% en mensajes). Señal más limpia, pero cambia lo que muestra el dashboard ("llamados" → "emergencias"). Umbral de alta actividad sugerido: >5 (23,7% de los días).

## Concepción

- Correlación diaria con Talcahuano casi nula (Spearman 0,08) y nula con el error del modelo usando Concepción de ayer (−0,03): **no sirve como predictor rezagado**.
- En temporales ambas se disparan juntas (2026-07-16: 67 / 38).
- Entrenamiento conjunto (filas de Concepción con indicador de ciudad): con clima consistente empeora levemente (bootstrap 8%) — descartado. `pooled_concepcion_mensajes.csv`.
- Modelo propio (walk-forward 4×120): R² 0,08, apenas sobre la media móvil de 28 días. Con ~2 años de historia aún no es producto. `concepcion_own_model_mensajes.csv`.

## Candidatos guardados

- `signal_hydro_ensemble_v2_viper_mensajes` — **promovido el 2026-11-01** (modelo oficial); refresco mensual con `03_model/refresh_viper_model.py`.
- `signal_hydro_ensemble_v2_viper_incidentes`, `signal_hydro_v3_viper_mensajes`, `signal_hydro_v3_viper_incidentes`.

Promoción: `active_models.json` apunta al candidato, los archivos canónicos de `02_data/` son la versión tweets+Viper (lags del serving verificados contra el dataset) y los caches de clima oficiales contienen la descarga fresca.

Scripts: `02_data/build_viper_dataset.py`, `02_data/extend_weather.py`, `03_model/viper_eval.py`, `viper_experiments.py`, `viper_wf.py`, `viper_concepcion.py`, `build_viper_candidate.py`.
