# Candidato hydro v3 (`signal_hydro_v3_glm_cal`) — no promovido

Protocolo: walk-forward idéntico al oficial (`TimeSeriesSplit` 6 × 120 días, 2024-03-18 a 2026-04-16, 720 días OOF).
Reproducción del oficial `signal_hydro_ensemble_v2`: MAE 2,2387 / RMSE 2,9182 / R² 0,141 (publicado 2,236 / 2,909 / 0,146).

## Resultado

| Métrica (720 días OOF) | Oficial | v3 |
|---|---|---|
| MAE | 2,2387 | **2,2206** |
| RMSE | 2,9182 | **2,8876** |
| R² | 0,141 | **0,159** |
| AUC ranking conteo (>7) | 0,662 | **0,671** |
| AUC prob. alta (calibración secuencial) | 0,653 | **0,659** |
| Brier prob. alta | 0,1427 | **0,1419** |
| Precisión / recall top-20% | 0,375 / 0,406 | **0,382 / 0,414** |
| Ratio de variabilidad | 0,396 | 0,368 |

- Mejora MAE en 5 de 6 bloques y RMSE en 5 de 6 (bloque 4, mar–jul 2025, empeora levemente: +0,002 MAE).
- Bootstrap por bloques de 30 días: 96,3% de soporte para la mejora de MAE y 95,6% para MSE.
- La ganancia es mayor en 2024 que en los bloques más recientes (bloques 5–6: −0,002 y −0,004 de MAE; −0,024 y −0,041 de RMSE).
- Costo: el pronóstico es algo menos variable (ratio 0,396 → 0,368).

## Qué cambia

1. **Calendario enriquecido** (`v3_features.calendar_features`): día libre, víspera/post día libre, fin de semana largo, Fiestas Patrias, fiestas de fin de año, vacaciones de verano/invierno, días a fin de mes, día de pago. Solo dependen de la fecha.
2. **Cabeza GLM Poisson regularizada** (α=10, estandarizada) mezclada al 25% con el blend XGBoost antes de la misma calibración de dispersión del oficial (centro 5, ×1,15, +0,10). Reduce varianza del ensemble.

## Lo que no funcionó (resultados negativos)

- Nivel lento (medias 60–365 días, EWM, mismo día de la semana 8 semanas, tendencia): empeora (MAE 2,2465).
- Anomalías meteorológicas vs 30 días previos, primera lluvia, calor seco: empeora (2,2468).
- Las tres familias juntas: 2,2454. Ponerlas también en la cabeza `active`: 2,2522.
- Regularización de XGBoost (lr lento, colsample 0,3/0,5, robusto): todo dentro de ±0,007, sin mejora consistente.
- Ponderar por recencia (vida media 365/730 días): reduce AUC; 365 días empeora claramente.
- 5 semillas promediadas: 2,2372 (irrelevante).

## Advertencia de búsqueda

Se evaluaron ~45 configuraciones sobre los mismos 720 días. La elección final se respaldó con la sensibilidad (29 de 30 vecinos α ∈ {0,3, 1, 3}, peso ∈ [0,10, 0,30] superan al oficial en MAE y RMSE) y una meseta en α 3–10, pero la mejora real esperada es pequeña (~0,8% de MAE) y debe confirmarse con pronósticos emitidos.

## Pendiente para promover

El serving (`dashboard.py`, `04_predict/predict_tomorrow.py`) todavía no calcula las columnas `CAL_*`. Promover requiere agregarlas con `v3_features.calendar_features` en ambos caminos y actualizar `active_models.json`, con aprobación explícita.

Scripts: `03_model/hydro_v3_harness.py`, `hydro_v3_ablation.py`, `hydro_v3_regularization.py`, `hydro_v3_structural.py`, `hydro_v3_robustness.py`, `hydro_v3_final_check.py`, `build_hydro_v3_candidate.py`.
