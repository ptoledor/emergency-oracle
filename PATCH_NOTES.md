# Patch Notes

## 2026-06-18 - Hotfix critico modelo/dashboard

- Cambio: training y CLI ahora filtran mensajes no incidentes con `incident_flags`, pero mantienen deduplicacion apagada.
  Contexto: training contaba mensajes operacionales; serving los filtraba.
  Motivo: evitar train/serve skew en el target diario.

- Cambio: `observed_zero` del audit queda como `EVENTOS = 0`; `coverage_unknown` sigue fuera.
  Contexto: dias cero validados se estaban eliminando.
  Motivo: el modelo debe aprender que existen dias reales con cero eventos.

- Cambio: dashboard carga `category_risk_artifact` antes de `load_data_and_predict()`.
  Contexto: cold cache podia dejar `df=None`.
  Motivo: evitar crash al iniciar Streamlit.

- Cambio: modelo operacional baja `operational_precision_target` a `0.20`.
  Contexto: recall operativo estaba cerca de 4-6%.
  Motivo: alerta preventiva necesita capturar mas dias riesgosos.

- Cambio: feature search principal usa `HistGradientBoostingRegressor(loss="poisson")` y permutation importance.
  Contexto: antes se rankeaban features con objetivo squared-error.
  Motivo: seleccionar variables con objetivo consistente con produccion.

- Cambio: riesgos secundarios usan thresholds calibrados con holdout temporal de train.
  Contexto: incendio tenia F1 test 0.0 con umbral OOF.
  Motivo: mejorar transferencia de thresholds a periodo posterior.

- Cambio: se agregan interacciones climaticas simples (`TEMP_HUM_INDEX`, `VIENTO_LLUVIA_INDEX`, `STORM_COMPOUND_INDEX`, `FIRE_DRY_INDEX_7D`).
  Contexto: investigacion local sugiere senal climatica compuesta.
  Motivo: dejar que backtesting acepte o descarte estas variables.

- Cambio: CLI usa fallback local de clima si Open-Meteo/proxy falla.
  Contexto: equipo actual puede estar bloqueado por proxy.
  Motivo: mantener inferencia local operativa sin API externa.

- Cambio: KFold robusto queda visible como contraste estructural con posible leakage temporal.
  Contexto: usuario decidio conservar esa pestaña.
  Motivo: comparar sensibilidad estructural sin confundirlo con validacion operacional.

- Cambio: `agends.md` queda como handoff breve y exige actualizar `PATCH_NOTES.md`.
  Contexto: trabajo alterna entre agentes.
  Motivo: mantener continuidad cuando se agoten tokens o cambie agente.

- Cambio: entrenamiento y prediccion fijan `LOKY_MAX_CPU_COUNT`, `OMP_NUM_THREADS` y `SKLEARN_NUM_THREADS` en 1.
  Contexto: Windows/sandbox bloqueaba pools internos (`WinError 5`).
  Motivo: hacer reproducible el pipeline en este equipo.

- Cambio: `train.py` usa fallback directo si `os.replace()` falla al guardar modelos.
  Contexto: reemplazo atomico de `.pkl` fue bloqueado por permisos locales.
  Motivo: completar reentrenamiento sin corromper el pipeline.

- Resultado: dataset reentrenado con 1444 filas, 22 dias `EVENTOS = 0`, cero `coverage_unknown` en training y diff categoria-total = 0.
  Contexto: validacion posterior a `clean_and_augment.py`.
  Motivo: auditar que la politica de target quedo aplicada.

- Resultado: modelo blend nuevo queda con MAE test 2.243, Brier 0.167, blend directo/categorias 77%/23% y KFold OOF MAE 2.292.
  Contexto: ejecucion de `train.py`, `train_category_risk_models.py` y `train_kfold_model.py`.
  Motivo: dejar trazabilidad de performance despues del cambio critico.

## 2026-06-19 - Gate de baseline y consistencia metodologica

- Cambio: el gate de baseline queda como advertencia, no como reemplazo del modelo.
  Contexto: el blend MAE 2.243 no supera baseline mediana MAE 2.173, pero reemplazarlo por mediana elimina variabilidad operacional.
  Motivo: mantener el modelo de conteo real en produccion y mostrar baseline solo como comparacion metodologica.

- Cambio: canonical `regressor_climatic_augmented.pkl` vuelve a usar `CategoryBlendRegressor`.
  Contexto: el artefacto canónico habia sido reemplazado por `ConstantRegressor` mediana train.
  Motivo: devolver prediccion operacional variable; metadata queda con `passes_baseline_gate=False` y `baseline_gate_action=warn_only`.

- Cambio: `_pruned.pkl` deja de ser sobreescrito por el blend de categorias.
  Contexto: el nombre decia pruned pero el contenido era blend.
  Motivo: conservar trazabilidad de artefactos.

- Cambio: `CategoryBlendRegressor.predict()` valida features estrictamente.
  Contexto: antes podia dropear columnas silenciosamente.
  Motivo: fallar temprano ante train/serve skew.

- Cambio: tabla de percentiles de sobredemanda usa probabilidades del train, igual que los badges.
  Contexto: mezclaba full dataset con umbrales train-only.
  Motivo: alinear percentiles visibles con etiquetas.

- Cambio: cache de clima valida rango de fechas y lags operativos usan `fillna(0)`.
  Contexto: cache podia quedar corto y lags de training usaban ffill distinto al CLI.
  Motivo: evitar clima stale y train/serve skew.

- Cambio: walk-forward usa threshold calibrado en vez de 0.5 fijo.
  Contexto: F1 no era comparable con produccion.
  Motivo: reportar validacion con la misma politica operacional.

- Cambio: limpieza de `tmp*.tmp` en `saved_models` y borrado manual de temporales huerfanos.
  Contexto: Windows dejo `.tmp` cuando `os.replace()` fue bloqueado.
  Motivo: higiene de repo y disco.

- Cambio: cache del dashboard ahora depende del timestamp de dataset/modelos.
  Contexto: predicciones historicas podian verse distintas de `4.0` por cache viejo tras cambiar el regresor canonical.
  Motivo: al cambiar `.pkl` o dataset, Streamlit recalcula historico y pronostico.

- Cambio: pestaña `Estadisticas de Modelos` agrega baselines `Regresor media` y `Regresor mediana`.
  Contexto: el usuario necesita comparar modelos variables contra regresores constantes.
  Motivo: mostrar MAE/MSE/R2 del mismo tramo test sin esconder que media/mediana son predicciones fijas.

- Cambio: tarjetas de estadisticas de modelos quedan con la misma grilla fija de metricas: MAE, MSE, R2, Brier, ROC-AUC, Accuracy, Precision, Recall y F1-Score.
  Contexto: baselines media/mediana no tienen clasificador ni probabilidad.
  Motivo: comparar cuatro cuadros con mismo formato, usando `N/A` donde la metrica no aplica.

- Cambio: baselines `Regresor media` y `Regresor mediana` muestran solo MAE, MSE y R2.
  Contexto: son regresores puros, no clasificadores.
  Motivo: evitar filas `N/A` en Accuracy, Precision, Recall, F1, Brier y ROC-AUC.

- Cambio: `Estadisticas de Modelos` separa `Modelos de Conteo` y `Modelos de Alerta`.
  Contexto: se estaban mezclando metricas del `ConstantRegressor` con metricas del clasificador operacional.
  Motivo: mantener lectura metodologica correcta: conteo usa MAE/MSE/R2; alerta usa Brier/ROC-AUC/Accuracy/Precision/Recall/F1.

- Cambio: descarga de clima en dashboard, CLI y limpieza usa 3 intentos con backoff antes de fallar.
  Contexto: un 503 transitorio podia activar fallback local o romper entrenamiento.
  Motivo: evitar clima sintetico/cacheado por errores temporales de Open-Meteo.

- Cambio: dashboard muestra warning visible cuando usa clima local estimado.
  Contexto: el fallback `_source=local_fallback` existia pero podia pasar desapercibido.
  Motivo: que el operador sepa que el pronostico usa estimacion local, no clima real de Open-Meteo.

- Cambio: `train_kfold_model.py` acepta `--splits` y se entreno modelo `10KFold`.
  Contexto: el usuario pidio agregar un modelo 10KFold ademas del 5KFold existente.
  Motivo: comparar sensibilidad estructural con mas folds aleatorios sin reemplazar el modelo robusto 5KFold.

- Resultado: artefactos `*_kfold10.pkl`, `kfold10_evaluation.csv` y `kfold10_oof_predictions.csv` agregados.
  Contexto: ejecucion `python 03_model/train_kfold_model.py --splits 10`.
  Motivo: dejar el modelo 10KFold disponible en estadisticas y comparacion de modelos.

## 2026-06-19 - Transición de MSE a RMSE y Clarificación de Muestras de Evaluación

- Cambio: se reemplazó la visualización de MSE por RMSE (Raíz del Error Cuadrático Medio) en las tarjetas de estadísticas del dashboard y en los scripts de entrenamiento (impresión en consola y metadatos).
  Contexto: el usuario solicitó cambiar MSE por RMSE para evaluar el error en la misma escala que la variable objetivo (eventos diarios).
  Motivo: facilitar la interpretación del error cuadrático ponderado de forma directa.

- Cambio: la función `render_model_metrics` de `dashboard.py` ahora calcula dinámicamente RMSE como la raíz de MSE en caso de cargar modelos legacy/anteriores que solo posean MSE.
  Contexto: evitar errores de retrocompatibilidad al leer modelos que no hayan sido reentrenados.
  Motivo: mantener robusto el dashboard ante cualquier cambio en el formato de los archivos serializados.

- Cambio: las tarjetas de estadísticas agregan la fila descriptiva "Evaluación" (e.g. "OOF completo (1444 de 1444 días)" vs "Set de prueba (289 días)") y "Validación".
  Contexto: el usuario notó que el MSE de 10KFold era peor que el del regresor de la media en la tabla, a pesar de tener R2 positivo. Esto ocurría porque el regresor media se evaluaba en el set de prueba temporal (289 días, varianza ~8.25), mientras que los modelos KFold se evaluaban en el dataset completo (1444 días, varianza ~9.23) usando predicciones Out-of-Fold.
  Motivo: clarificar que las evaluaciones de KFold (completo OOF) y del Modelo Operacional/Baselines (set de prueba temporal) no son comparables directamente debido a que se evalúan sobre distintas muestras con diferente varianza/distribución.

- Resultado: reentrenamiento exitoso de todos los artefactos de modelos (`regressor_climatic_augmented.pkl`, `metadata_climatic_augmented.pkl`, `*_kfold.pkl`, `*_kfold10.pkl`) incluyendo las nuevas métricas calculadas de forma nativa.
  Contexto: ejecución del pipeline de entrenamiento local.
  Motivo: guardar las variables y baselines de RMSE calculados directamente desde el script de entrenamiento para persistencia a largo plazo.

- Cambio: se agregaron los cálculos de índices climáticos compuestos (`TEMP_HUM_INDEX`, `VIENTO_LLUVIA_INDEX`, `STORM_COMPOUND_INDEX`, `FIRE_DRY_INDEX_7D`) en `predict_6_days` dentro de `dashboard.py`.
  Contexto: la predicción dinámica de 6 días en el dashboard fallaba al alinear características con un KeyError ya que las variables compuestas no eran calculadas para los modelos que ahora las consumen.
  Motivo: asegurar que el pipeline de predicción interactivo del dashboard cuente con las mismas variables compuestas que el entrenamiento.

- Cambio: se unificaron las tablas de métricas de conteo y alerta en una sola grilla de 5 columnas en la pestaña `Estadísticas de Modelos` (Tab 3).
  Contexto: el usuario solicitó dejar todas las métricas juntas (regresión y clasificación/alerta) en lugar de mostrarlas en bloques separados.
  Motivo: simplificar y unificar la comparación metodológica visualizando todos los parámetros de rendimiento en un único cuadro consolidado por modelo, mostrando `N/A` para las métricas no aplicables (e.g. métricas de clasificación en regresores baseline).

- Cambio: se entrenó un modelo robusto `15KFold` (`--splits 15`) y se incorporó al dashboard.
  Contexto: requerimiento del usuario de analizar la consistencia metodológica estructural con 15 pliegues aleatorios.
  Motivo: comparar el desempeño en la pestaña de comparación de modelos.

- Cambio: se renombró la pestaña `Predicción Operacional` (Tab 1) a `Forecast` y se configuró para utilizar internamente el modelo `10KFold`.
  Contexto: requerimiento de usar el modelo 10KFold como el modelo principal de pronóstico diario y reporte.
  Motivo: alinear la experiencia de predicción en producción con el modelo robusto seleccionado por el operador.

- Cambio: la pestaña `Estadísticas de Modelo` (Tab 3) se simplificó para mostrar únicamente las métricas y la importancia de variables del modelo de Forecast (`10KFold`).
  Contexto: el usuario solicitó que en esta pestaña vaya únicamente el modelo en Forecast.
  Motivo: mantener foco en el modelo operativo.

- Cambio: la pestaña `Comparación de Modelos` (Tab 6) se expandió a una grilla de 5 columnas que compara `Baseline Media`, `Operacional` (blend), `5KFold`, `10KFold` y `15KFold`.
  Contexto: el usuario solicitó que esta pestaña compare estos modelos específicos de interés.
  Motivo: centralizar la comparación cruzada en un solo lugar.

## 2026-06-19 - Repeated 5-Fold CV y Limpieza del Dashboard

- Cambio: se removió la pestaña "Predicción Robusta" (antigua Tab 2) de `dashboard.py`.
  Contexto: el usuario solicitó limpiar el dashboard eliminando este panel de predicción robusta alternativo.
  Motivo: simplificar la interfaz gráfica y enfocar al operador en el Forecast principal.

- Cambio: las pestañas de navegación en `dashboard.py` ahora usan nombres descriptivos explícitos (`tab_forecast`, `tab_stats`, `tab_history`, `tab_seasonal`, `tab_compare`) en lugar de variables genéricas indexadas (`tab1`..`tab6`).
  Contexto: al eliminar la segunda pestaña, la indexación manual de variables tabulares se volvía propensa a errores por desplazamientos.
  Motivo: mejorar la legibilidad del código y prevenir bugs futuros por cambios en el orden o cantidad de pestañas.

- Cambio: se implementó y ejecutó el script `03_model/train_repeated_kfold.py` para entrenar un modelo con validación Repeated 5-Fold utilizando 10 semillas aleatorias diferentes (50 particiones en total).
  Contexto: solicitud del usuario de explorar la estabilidad estructural del modelo mediante validación cruzada repetida con 10 semillas.
  Motivo: obtener métricas de validación Out-Of-Fold más robustas y estables que promedien la influencia del ruido aleatorio en la división de los pliegues.

- Resultado: generación de los artefactos de modelo `regressor_climatic_augmented_repeated_kfold.pkl`, `classifier_climatic_augmented_repeated_kfold.pkl`, `metadata_climatic_augmented_repeated_kfold.pkl`, `repeated_kfold_evaluation.csv` (métricas de las 50 corridas) y `repeated_kfold_oof_predictions.csv` (predicciones OOF suavizadas por promedio).
  Contexto: entrenamiento exitoso del modelo repeated K-Fold.
  Motivo: proveer persistencia a las estadísticas de la validación cruzada repetida para su visualización y auditoría posterior.

- Cambio: se integró el modelo Repeated 5KFold en la pestaña de comparación de modelos de `dashboard.py` expandiendo las grillas de métricas y gráficos de importancia a 6 columnas (`Baseline Media`, `Operacional` (blend), `5KFold`, `10KFold`, `15KFold` y `Repeated 5KFold`).
  Contexto: solicitud del usuario de explorar el desempeño de este modelo comparándolo con el resto del ecosistema de validaciones.
  Motivo: visualizar si el Repeated KFold logra mejorar el rendimiento y estabilizar el ranking de importancia de variables frente a KFold de una sola semilla.

- Cambio: se restauraron las funciones auxiliares de visualización y métricas (`render_model_metrics`, `build_constant_regressor_metadata`, `build_operational_display_metadata`, `build_classification_metadata`, `render_metric_explanations` y `render_importance_chart`) que habían sido eliminadas accidentalmente al borrar el bloque de la pestaña robusta.
  Contexto: NameError al iniciar el dashboard.
  Motivo: estas funciones se encontraban declaradas físicamente dentro del bloque de la pestaña robusta y son consumidas por el resto de pestañas.

## 2026-06-19 - Promoción del Modelo Oficial y Depuración de Legacy

- Cambio: el modelo Repeated 5-Fold (10 semillas) fue promovido como el modelo oficial del proyecto, sobrescribiendo los archivos de modelo principal (`regressor_climatic_augmented.pkl`, `classifier_climatic_augmented.pkl` y `metadata_climatic_augmented.pkl`).
  Contexto: el usuario decidió establecer Repeated 5-Fold como el modelo operacional principal.
  Motivo: alinear todas las predicciones de producción e inferencia CLI con el modelo de validación repetida más estable.

- Cambio: se eliminaron todos los modelos legacy (blend operacional original, 5KFold, 10KFold y 15KFold) de los cargadores y las vistas comparativas en el dashboard.
  Contexto: requerimiento del usuario de remover todos los otros modelos de las comparativas.
  Motivo: limpiar la pantalla de comparaciones y evitar ruidos metodológicos al comparar múltiples modelos obsoletos.

- Cambio: la vista de comparación de modelos (`tab_compare`) se redujo a una grilla limpia de 2 columnas que enfrenta a `Baseline Media` contra `Repeated 5KFold (Oficial)`.
  Contexto: simplificación solicitada por el usuario.
  Motivo: focalizar la comparación visual en la mejora neta aportada por el modelo robusto oficial respecto a una media naive.

- Cambio: se corrigió una ordenación alfabética incondicional de variables explicativas en `load_data_and_predict` al cargar modelos `HistGradientBoostingRegressor` puros.
  Contexto: NameError/ValueError en predicciones si el modelo oficial no es un regresor blend de categorías.
  Motivo: garantizar que las variables se pasen en el orden exacto en que el modelo fue entrenado en `train_repeated_kfold.py`.

- Cambio: se agregó un parámetro opcional `key` a `render_importance_chart` y se pasaron claves únicas (`key="importance_forecast"` y `key="importance_comparison"`) en sus respectivas llamadas.
  Contexto: StreamlitDuplicateElementId al renderizar el dashboard.
  Motivo: Streamlit genera IDs automáticos a partir del tipo de elemento y sus parámetros. Al dibujar el mismo gráfico Plotly con la misma importancia de variables en múltiples pestañas, Streamlit generaba un ID duplicado causando el colapso de la aplicación.

- Cambio: se actualizaron los rangos de clasificación en `secondary_level` para utilizar límites fijos de probabilidad: 0-20% (Baja), 20-40% (Normal), 40-60% (Alta) y >60% (Muy Alta).
  Contexto: solicitud del usuario de ajustar las etiquetas del modelo principal de sobredemanda y modelos secundarios de categorías.
  Motivo: simplificar y homogeneizar las alertas operativas usando rangos de probabilidad absolutos fáciles de interpretar en lugar de percentiles variables.

## 2026-06-19 - Entrenamiento de r5f30s, Depuración General y Migración a .agents

- Cambio: se entrenó un nuevo modelo Repeated 5-Fold con 30 semillas (r5f30s), logrando un R² OOF del 8.98% y mejorando las métricas sobre el modelo de 20 semillas.
  Contexto: solicitud del usuario de explorar si 30 semillas logran una mejor generalización y estabilidad.
  Motivo: reducir la varianza de la validación cruzada y obtener un modelo con mayor capacidad predictiva estructurada.

- Cambio: se reestructuró la pestaña de comparación del dashboard para presentar una grilla de 3 columnas que compara directamente el modelo anterior (Repeated 5-Fold 20S), el nuevo modelo (Repeated 5-Fold 30S) y el modelo oficial activo.
  Contexto: requerimiento del usuario de analizar los dos modelos lado a lado e integrarlos en el comparador.
  Motivo: facilitar la toma de decisiones sobre la promoción del modelo a producción.

- Cambio: se limpiaron todos los archivos del directorio de modelos guardados (saved_models) que pertenecían a configuraciones obsoletas o secundarias (ej. splits simples de 10 y 15 folds, configuraciones repetidas antiguas de 10 y 20 folds), conservando solo el modelo principal de 20 semillas y el de 30 semillas.
  Contexto: solicitud del usuario de eliminar archivos innecesarios.
  Motivo: reducir el uso de disco del repositorio y evitar confusiones con artefactos legacy en desuso.

- Cambio: se migraron y unificaron todos los instructivos y handoffs de agentes (`agends.md` y `FLUJO_ENTRENAMIENTO.md`) hacia `.agents/AGENTS.md`, eliminando los archivos duplicados de la raíz.
  Contexto: orden y organización de la documentación del proyecto.
  Motivo: concentrar todas las reglas de trabajo, políticas de datos y especificaciones del proyecto en la carpeta oficial de personalizaciones del agente (.agents).

- Cambio: se eliminaron archivos de bitácora y logs de depuración del proyecto (`st_err.log`, `st_out.log`, etc.) de la raíz del repositorio.
  Contexto: limpieza general.
  Motivo: mantener el espacio de trabajo limpio y profesional.

## 2026-06-19 - Ingeniería de Features Avanzada e Introducción de XGBoost (Breakthrough 12.6% R²)

- Cambio: se agregaron 5 nuevas variables explicativas avanzadas: `ES_PRE_FERIADO` (víspera de feriado), `DIAS_DESDE_ULTIMA_LLUVIA` (días secos acumulados), `VPD` (Vapor Pressure Deficit medio), `VPD_MAX` (Vapor Pressure Deficit máximo), y medias móviles de mayor ventana `EVENTOS_rolling_mean_14d` y `EVENTOS_rolling_mean_30d` en `clean_and_augment.py`.
  Contexto: requerimiento de aumentar las características predictivas con índices climáticos físicos e indicadores de feriados.
  Motivo: capturar de forma directa el estrés hídrico de la vegetación (riesgo de incendios forestales) y la inercia de llamados a mediano plazo.

- Cambio: se introdujo soporte de entrenamiento y evaluación nativa de XGBoost (`xgb.XGBRegressor` y `xgb.XGBClassifier`) en `train_repeated_kfold.py` con una validación cruzada estructurada de 5 pliegues y 30 semillas (r5f30s).
  Contexto: contrastar el desempeño de XGBoost frente a HistGradientBoosting y RandomForest tradicionales.
  Motivo: XGBoost maneja de forma óptima variables continuas no lineales e interacciones complejas.

- Cambio: se actualizaron el dashboard (`dashboard.py`) y el script CLI (`predict_tomorrow.py`) para calcular dinámicamente y con consistencia temporal todas las nuevas variables explicativas durante el serving e inferencia diaria.
  Contexto: prevenir crashes por KeyError al alimentar los nuevos modelos con datos parciales de clima.
  Motivo: asegurar que el serve y el training compartan la misma distribución y definición de variables (evitar train/serve skew).

- Cambio: se reestructuró la pestaña de comparación del dashboard (`tab_compare`) a una grilla limpia de 3 columnas que contrasta los hitos evolutivos: `Repeated 5-Fold (20S) (RF)` (original), `Repeated 5-Fold (30S) (RF)` (nuevas variables) y `Repeated 5-Fold (30S) (XG)` (XGBoost, que ahora es también el oficial). Se removió la columna duplicada del oficial redundante y se agregaron los sufijos de algoritmo (RF y XG) a las etiquetas del comparador y al forecast activo.
  Contexto: sugerencia del usuario de clarificar los algoritmos usados en la interfaz y eliminar la columna duplicada tras la promoción de XGBoost.
  Motivo: mantener el comparador centrado, limpio, e informativo sobre la tecnología utilizada.

- Cambio: se promovió el modelo de XGBoost a productivo actualizando `active_models.json` a `repeated_5fold_30seeds_xgboost` y físicamente copiando los archivos a las rutas canónicas del modelo principal (`regressor_climatic_augmented.pkl`, `classifier_climatic_augmented.pkl` y `metadata_climatic_augmented.pkl`).
  Contexto: instrucción directa del usuario de promover el modelo.
  Motivo: asegurar redundancia absoluta tanto para la resolución dinámica de configuraciones como para los fallbacks de carga tradicionales.

- Resultado: XGBoost (XG) alcanzó un desempeño récord con un R² OOF de **12.63%** (frente al 9.85% de HistGB/RF con las mismas variables, y 8.85% de la versión anterior de 20S), reduciendo el MAE a **2.215 y mejorando el Brier Score a 0.132.**
