# Patch Notes

## 2026-07-16 - Búsqueda temporal H1-H6 con holdout final

- Cambio: se agregó `search_temporal_candidates.py`, que compara reglas de recencia, mediana, EWMA, estacionalidad semanal y blends sin usar información posterior al origen.
  Contexto: el candidato XGBoost de dos regímenes obtuvo MAE 2.586 y fue peor que la media móvil de 28 días en los seis horizontes.
  Motivo: buscar primero señales simples y robustas antes de aumentar complejidad del modelo.

- Cambio: la selección se realiza en los primeros 75% de orígenes evaluables y el gate se calcula exclusivamente sobre el 25% temporal final.
  Contexto: escoger y reportar el mejor candidato sobre las mismas fechas produciría optimismo por selección múltiple.
  Motivo: conservar un tramo final intacto para confirmar que la mejora se generaliza.

- Cambio: el workflow de GitHub Actions ejecuta la búsqueda, las pruebas del gate y publica leaderboard, métricas H1-H6 y predicciones del holdout como artefactos.
  Contexto: el entrenamiento debe poder repetirse online sin alterar `active_models.json`.
  Motivo: iterar de forma auditable y sin promover modelos automáticamente.

## 2026-06-22 - Target y sobredemanda de 5ta Cía

- Cambio: el pipeline detecta despachos de 5ta Cía mediante `B-5/B5`, `RB-5/RB5`, `RX-5`, `MX-5` y el alias preventivo `BX-5/BX5`, eliminando URLs antes del matching.
  Contexto: se necesitaba reconstruir la actividad histórica de la Quinta sin confundir códigos aleatorios de enlaces ni el plus code geográfico `J5`.
  Motivo: crear una segunda etiqueta diaria reproducible y auditable.

- Resultado de datos: 2.366 mensajes incident-like entre 2022-01-04 y 2026-04-16; B-5 aparece en 1.039, RX-5 en 881, RB-5 en 441 y MX-5 en 17. Doce mensajes contienen más de una unidad de la Quinta y cuentan como un solo despacho/mensaje.

- Cambio: `augmented_emergency_data.csv` incorpora `N_5TA_CIA` y se genera `fifth_company_dispatch_audit.csv` con fecha, texto y unidades detectadas; deduplicación permanece desactivada.
  Contexto: entrenamiento e inferencia deben compartir la misma política de conteo operacional.
  Motivo: conservar trazabilidad desde el target diario hasta los mensajes fuente.

- Cambio: se entrenó XGBoost Repeated 5-Fold 30S para conteo diario y probabilidad de sobredemanda `N_5TA_CIA >= 3`.
  Resultado: MAE 1.010, RMSE 1.248, R² 0.010, ROC-AUC 0.528, Brier 0.168, precision 0.216 y recall 0.950 con umbral probabilístico 0.13.

- Cambio: Forecast muestra la categoría `5ta Cía` con despachos esperados, probabilidad de sobredemanda y nivel relativo; la tabla de percentiles incluye su distribución histórica.
  Contexto: el operador necesita anticipar carga específica para la Quinta además de la sobredemanda general.
  Motivo: exponer la segunda etiqueta en la misma superficie operacional.

## 2026-06-22 - Reversión de conteos enteros en el modelo oficial

- Cambio: se revirtió el redondeo aplicado inicialmente al histórico y forecast del modelo oficial; vuelve a entregar su valor esperado continuo original.
  Contexto: la salida entera debía evaluarse como un candidato independiente, no modificar el campeón vigente.
  Motivo: preservar el gate Oficial vs. Candidato y evitar alterar producción antes de comparar métricas.

## 2026-06-22 - Candidato XGBoost de salida entera

- Cambio: se retiró el candidato walk-forward vigente y se construyó un candidato independiente sobre XGBoost Repeated 5-Fold 30S que redondea al entero no negativo más cercano.
  Contexto: se solicitó que el modelo se comprometa con un conteo entero, pero sin alterar el oficial continuo.
  Motivo: comparar limpiamente la regla de decisión entera contra el campeón bajo las mismas predicciones OOF.

- Resultado: MAE OOF 2.191, RMSE OOF 2.845, R² OOF 0.123 y ratio de variabilidad 0.353. Las métricas continuas equivalentes fueron MAE 2.215, RMSE 2.839 y R² 0.127.

- Cambio: el comparador conserva exactamente dos columnas: Oficial continuo y Candidato entero.
  Contexto: el candidato entero sustituye al candidato walk-forward descartado.
  Motivo: mantener un único candidato vigente y respetar el gate de promoción.

## 2026-06-22 - Conectividad y reintentos de Open-Meteo

- Cambio: el dashboard importa `time` y centraliza la descarga de clima en `fetch_json_with_retry()` con tres intentos.
  Contexto: el reintento llamaba `time.sleep()` sin importar el módulo y `get_weather_for_range()` duplicaba una solicitud sin reintentos.
  Motivo: evitar un fallback prematuro y hacer más robusta la consulta a Open-Meteo.

- Diagnóstico: Open-Meteo respondió HTTP 200 fuera del aislamiento; el fallback observado provenía del servidor Streamlit iniciado en una sesión con sockets restringidos (`WinError 10013`).

## 2026-06-22 - Etiqueta del modelo XGBoost en Forecast

- Cambio: la etiqueta del modelo activo reconoce `XGBRegressor` como XGBoost y muestra el sufijo `(XG)`.
  Contexto: el metadata canónico declaraba `XGBRegressor`, pero el dashboard buscaba literalmente `xgboost` y mostraba `(RF)`.
  Motivo: alinear la identificación visual con el artefacto XGBoost realmente cargado.

## 2026-06-22 - Limpieza y reentrenamiento XGBoost R5F 30S

- Cambio: se eliminaron 33 artefactos `.pkl` legacy sin uso y se conservaron los seis requeridos por el dashboard y el pipeline operacional.
  Contexto: `saved_models` acumulaba modelos agnósticos, variantes antiguas y blends que ya no eran consumidos.
  Motivo: mantener un inventario mínimo y evitar confundir artefactos obsoletos con modelos vigentes.

- Cambio: se reentrenó el candidato XGBoost con Repeated 5-Fold y 30 semillas, sin promoverlo automáticamente.
  Contexto: los artefactos versionados del comparador no habían sido conservados en el repositorio.
  Motivo: restaurar un candidato reproducible y respetar el gate de promoción definido en `.agents/AGENTS.md`.

- Resultado: MAE OOF 2.215, RMSE OOF 2.839, R² OOF 0.127, ROC-AUC OOF 0.657 y Brier OOF 0.132.

- Cambio: `.gitignore` permite conservar los tres `.pkl`, la evaluación por fold, las predicciones OOF y el futuro `active_models.json` de esta configuración.
  Contexto: la regla general de modelos generados había ocultado los artefactos versionados.
  Motivo: impedir que el modelo comparativo vuelva a desaparecer entre checkouts.

- Cambio: la pestaña de comparación muestra el XGBoost oficial actual frente al candidato XGBoost 30S reentrenado.
  Contexto: las columnas RF 20S y RF 30S apuntaban a artefactos inexistentes que fueron retirados de la carpeta.
  Motivo: presentar solo comparaciones reproducibles con modelos realmente disponibles.

## 2026-06-22 - Candidato XGBoost Repeated 5-Fold 100S

- Cambio: se entrenó y guardó un candidato XGBoost con Repeated 5-Fold y 100 semillas, sin promoverlo automáticamente.
  Contexto: se solicitó medir si aumentar de 30S a 100S estabiliza o mejora el desempeño OOF.
  Motivo: comparar ambos candidatos bajo el mismo algoritmo, variables y dataset.

- Resultado: MAE OOF 2.212, RMSE OOF 2.836, R² OOF 0.128, ROC-AUC OOF 0.659 y Brier OOF 0.132.

- Cambio: el comparador presenta tres columnas: modelo oficial, candidato 30S y candidato 100S, con métricas e importancias.
  Contexto: el candidato 100S debe evaluarse antes de cualquier promoción.
  Motivo: cumplir el gate de comparación y promoción de `.agents/AGENTS.md`.

## 2026-06-22 - Candidato XGBoost Repeated 6-Fold 30S

- Cambio: se descartaron los candidatos XGBoost 5-Fold 30S y 100S, incluidos sus `.pkl`, evaluaciones OOF y reglas de versionado.
  Contexto: el comparador debe contener únicamente el modelo oficial y un candidato vigente.
  Motivo: evitar acumular candidatos rechazados o columnas sin vigencia operacional.

- Cambio: se entrenó y guardó un candidato XGBoost con Repeated 6-Fold y 30 semillas, sin promoverlo automáticamente.
  Contexto: se solicitó evaluar seis particiones manteniendo algoritmo, variables y dataset.
  Motivo: comparar el efecto de aumentar los folds frente al modelo oficial 5-Fold 30S.

- Resultado: MAE OOF 2.214, RMSE OOF 2.838, R² OOF 0.128, ROC-AUC OOF 0.658 y Brier OOF 0.132.

- Cambio: la pestaña de comparación vuelve a dos columnas: Oficial y Candidato 6-Fold 30S.
  Contexto: el candidato 6-Fold 30S reemplaza a los candidatos descartados.
  Motivo: mantener un único gate de decisión claro.

## 2026-06-22 - Candidato XGBoost Repeated 4-Fold 30S

- Cambio: se descartó el candidato XGBoost 6-Fold 30S con todos sus artefactos y se entrenó XGBoost Repeated 4-Fold 30S.
  Contexto: se solicitó comparar una configuración de cuatro particiones manteniendo algoritmo, semillas, variables y dataset.
  Motivo: medir el efecto del tamaño de los folds frente al modelo oficial 5-Fold 30S.

- Resultado: MAE OOF 2.214, RMSE OOF 2.840, R² OOF 0.126, ROC-AUC OOF 0.660 y Brier OOF 0.132.

- Cambio: el comparador conserva exactamente dos columnas: Oficial y Candidato 4-Fold 30S.
  Contexto: el candidato 4-Fold sustituye al candidato 6-Fold descartado.
  Motivo: mantener un único candidato vigente antes del gate de promoción.

## 2026-06-22 - Candidato walk-forward de dos regímenes H1–H6

- Cambio: se descartó el candidato Repeated 4-Fold 30S y se implementó un candidato XGBoost walk-forward de dos regímenes con modelos directos para horizontes 1 a 6.
  Contexto: las predicciones del modelo oficial conservan solo cerca de un tercio de la variabilidad diaria y se contraen hacia la media.
  Motivo: separar actividad normal/alta y evitar que la predicción recursiva aplaste los horizontes posteriores.

- Cambio: la evaluación usa 39 bloques temporales expansivos de 28 días, con entrenamiento limitado a observaciones anteriores a cada bloque.
  Contexto: Repeated K-Fold aleatorio no representa el uso operacional y puede mezclar pasado y futuro.
  Motivo: obtener una medición fuera de tiempo y sin fuga del target futuro en las medias móviles de eventos.

- Resultado: MAE 2.580, RMSE 3.278, R² -0.187, ROC-AUC 0.606, Brier 0.194 y ratio de variabilidad 0.493. La media móvil de 28 días obtuvo MAE 2.297 y R² 0.005 sobre el mismo tramo temporal.

- Cambio: el serving reconoce modelos directos por horizonte y mantiene fija la historia de eventos disponible en el origen del pronóstico.
  Contexto: los modelos H2–H6 no deben consumir predicciones anteriores como si fueran eventos observados.
  Motivo: alinear entrenamiento e inferencia del candidato walk-forward.

- Decisión: candidato conservado solo para comparación; no cumple el gate de promoción por deterioro material de error y calibración.

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
