# Reglas del Proyecto (Emergency Oracle)

Este documento contiene las directrices operacionales, políticas de datos y el flujo metodológico del proyecto para guiar a los agentes de IA en el desarrollo y mantenimiento del CBT Oracle (Bomberos de Talcahuano).

---

## 1. Comparación y Promoción de Modelos (Gate de Promoción)

- **Registro en Comparador**: Cada vez que se entrene o evalúe una nueva configuración de modelo, sus métricas de validación cruzada y la importancia de sus variables explicativas deben integrarse en la pestaña de comparación del dashboard (`tab_compare`).
- **Gate de Promoción**: No se debe promover o activar un nuevo modelo como el principal/operacional de forma automática. El flujo obligatorio es:
  1. Entrenar y guardar el modelo con un sufijo descriptivo (por ejemplo, con splits y semillas) para no alterar el modelo activo.
  2. Reportar al usuario las métricas obtenidas.
  3. Preguntar explícitamente si desea promoverlo como el modelo oficial antes de actualizar el archivo de configuración `active_models.json`.

---

## 2. Estado Actual y Reglas de Trabajo

- **Zona Horaria**: Operacionalmente todo se maneja en `America/Santiago`.
- **Actualización de Cambios**: Antes de cerrar una actualización relevante, registrar las modificaciones, el contexto y el motivo en `PATCH_NOTES.md`.
- **Deduplicación**: `mark_duplicates()` sigue sin filtrar (`_IS_DUPLICATE = False`). Desde 2026-11-01 los datos Viper se limpian en `build_viper_dataset.py` (repeticiones del feed y reclasificaciones `6-6`); `dispatch.id` identifica una emergencia y los mensajes `SALE …`/`SEGUNDA ALARMA` la reutilizan (solo se excluyen con `--target incidentes`). No alterar esta lógica sin instrucción directa del usuario.
- **Viper**: los dumps JSON de Viper (`01_scraper/talcahuano/`) son la fuente oficial de conteos desde 2024-06-24 (el campo `date` está en UTC−3 fijo). Se entregan manualmente; no automatizar scraping sin instrucción del usuario. Concepción (`01_scraper/concepcion/`) no aporta a Talcahuano (ver `05_research/results/viper_talcahuano/README.md`).
- **Nomenclatura**:
  - Headers internos de datos y código de variables deben estar **sin tildes**.
  - Textos visibles en gráficos y etiquetas del dashboard **pueden y deben usar tildes** correctamente.
  - Para los nombres de modelos en gráficos/tablas, acortar las semillas a la nomenclatura: `10S`, `20S`, `30S`.
- **Dashboard**:
  - Pestaña de Forecast: Mostrar el modelo activo debajo de las tarjetas de pronóstico (`Modelo: {current_model}`). No incluir la leyenda explicativa de percentiles (`Baja < p33...`).
  - Pestaña de Comparación: Mostrar un layout de columnas comparativas sin filas descriptivas extra innecesarias (como "Validación y Evaluación").

---

## 3. Política de Datos y Conteo de Eventos

- **Consistencia**: El entrenamiento y el servicio (serving/inference) deben usar exactamente la misma política de conteo de incidentes.
- **Tratamiento de Duplicados**: Se corre la función `mark_duplicates()` para obtener `incident_flags`, pero `_IS_DUPLICATE = False` siempre (no filtrar registros duplicados hasta orden contraria).
- **Imputaciones**:
  - Los días reportados como `observed_zero` desde `05_research/data_quality/output/daily_target_audit.csv` se asignan como `EVENTOS = 0`.
  - Registros marcados como `coverage_unknown` se excluyen del entrenamiento.
- **Categorías Internas**:
  - `N_RESCATE_VEH`: Rescate vehicular.
  - `N_INCENDIO_ESTR` + `N_INCENDIO_FOREST`: Incendio.
  - `N_EMERGENCIAS_CLIMATICAS`: Emergencias climáticas.

---

## 4. Modelos Activos Actuales

Definidos y resueltos dinámicamente mediante `03_model/saved_models/active_models.json`:
- **Regresor/Clasificador Principal (Operativo)**: `signal_hydro_ensemble_v2_viper_mensajes` (HydroObjectiveEnsembleRegressor + RegressorProbabilityClassifier, 118 variables), promovido el 2026-11-01. Entrenado con historia tweets+Viper (1.626 días). Walk-forward 6×120: MAE 2,33 / RMSE 3,41 / R² 0,39.
- **Respaldo**: `signal_hydro_ensemble_v2` (misma arquitectura, historia solo Twitter hasta 2026-04-16).
- **Datos canónicos**: `02_data/augmented_emergency_data.csv` y `02_data/compiled_scraped_data.csv` son la versión tweets+Viper; la versión solo Twitter está en `*_twitter.csv`.
- **Submodelos de Categorías (Riesgos Secundarios)**: `03_model/saved_models/category_risk_models.pkl` (aún entrenados con historia Twitter).

---

## 5. Pipeline Metodológico de Ejecución

El flujo secuencial de scripts para preparar datos, entrenar y predecir es:

```powershell
# 0. Refresco mensual con un nuevo dump Viper (dataset + clima + walk-forward + modelo activo)
.\.venv\Scripts\python.exe 03_model\refresh_viper_model.py

# 1. Limpieza y aumento de features (clima + calendarios deterministas). Sin argumentos
#    procesa el archivo canonico; el refresco Viper usa --coverage-override 02_data\viper_coverage.csv
.\.venv\Scripts\python.exe 02_data\clean_and_augment.py

# 2. Entrenamiento de validación cruzada estructurada repetida (ej. 5-Fold, 30 semillas)
.\.venv\Scripts\python.exe 03_model\train_repeated_kfold.py --splits 5 --repeats 30

# 3. Modelado de riesgos secundarios por categoría
.\.venv\Scripts\python.exe 03_model\train_category_risk_models.py

# 4. Predicción del día de mañana (Consola)
.\.venv\Scripts\python.exe 04_predict\predict_tomorrow.py --real-tomorrow

# 5. Ejecución local de la interfaz Streamlit
.\.venv\Scripts\python.exe -m streamlit run dashboard.py
```
