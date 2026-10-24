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
- **Deduplicación**: Actualmente desactivada (`dedup_mode = disabled`). No activar o alterar la lógica de deduplicación sin instrucción directa del usuario.
- **Scraping**: No utilizar scraping Viper/XY en esta etapa.
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

Definidos y resueltos dinámicamente mediante `active_models.json` (clave `count_model`):
- **Modelo de conteo (Operativo)**: `03_model/saved_models/count_model_v3/` (`CountModel`, XGBoost Poisson sin historial, 100 variables, guardado en JSON; MAE día 1 2,261 con clima de anticipación real).
- **Probabilidad de alta actividad**: `HorizonProbabilityClassifier`, logística por día del forecast definida en `metadata.json` del mismo directorio.
- **Submodelos de Categorías**: `category_risk_models.pkl` (riesgo) y `category_composition_models.pkl` (composición).
- Se entrena y selecciona con `03_model/train_model.py`; `--promote` solo con aprobación explícita.

---

## 5. Pipeline Metodológico de Ejecución

El flujo secuencial de scripts para preparar datos, entrenar, evaluar y predecir está en la sección `Pipeline` de `README.md`; mantener ambos documentos coherentes.

- Antes de cerrar un cambio, correr `.\.venv\Scripts\python.exe -m pytest` (también corre en CI en cada PR).
- `test_golden_forecast.py` congela el forecast del dashboard: solo se regenera (`python 05_research/models/tests/forecast_harness.py`) cuando el cambio de predicción es intencional y aprobado.
