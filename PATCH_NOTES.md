# Patch Notes

## 2026-07-17 - Rediseño móvil de Forecast

- Cambio: en teléfonos, las cuatro pestañas forman una grilla 2×2 con texto ajustable y dejan de quedar cortadas horizontalmente.

- Cambio: los niveles por emergencia pasan a una grilla 2×2 dentro de cada tarjeta móvil, con píldoras más grandes, borde semaforizado y glosas legibles.

- Cambio: las seis variables meteorológicas se organizan en dos columnas para reducir la altura de cada tarjeta. Verificado a 390×844 sin desbordamiento horizontal.

- Refinamiento visual: las píldoras por emergencia quedan neutras y el semáforo se concentra únicamente en la flecha. `Otro` usa ahora el icono visible `❓`.

## 2026-07-16 - Tarjetas operacionales por tipo de emergencia

- Cambio: se elimina completamente `Rango probable 90%` de las tarjetas diarias.

- Cambio: Incendios, Rescates, Climáticas y Otros se muestran en una fila horizontal inmediatamente debajo del nivel de actividad, únicamente como llamados esperados por tipo.

- Cambio visual: cada tipo, su conteo y su tendencia forman una sola píldora con fondo semaforizado: azul `↓`, verde `–`, naranjo `↑` y rojo `↑↑`.

- Simplificación: se elimina el bloque redundante `Prob. actividad alta`. Las píldoras muestran icono y glosa completa: Incendio, Rescate, Climático y Otro.

- Cambio: se elimina el cuadro inferior `Composición esperada y base habitual`; la composición permanece únicamente en las píldoras compactas de cada tarjeta.

- Semáforo coherente de Incendios, Rescates y Climáticas: combina la probabilidad calibrada de actividad alta con el cambio del conteo frente a su base y utiliza el nivel más conservador. Así, Rescate con 10% permanece verde/Normal y un Climático de 0,1 no puede quedar rojo si su conteo no está también muy aumentado. `Otros`, sin clasificador probabilístico propio, conserva la comparación contra su base habitual.

## 2026-07-16 - Composición esperada con base habitual

- Cambio: se añade, sin reemplazar las probabilidades actuales, una tabla diaria con la composición esperada de llamados para incendios, rescates, emergencias climáticas y otros.

- Interpretación: cada grupo muestra su base habitual, el conteo esperado reconciliado con el pronóstico oficial, su porcentaje del total y la variación respecto de la base. Cuando el total aumenta, `% del alza` indica qué grupos explican los incrementos positivos.

- Cambio visual: cada tarjeta diaria muestra explícitamente `Prob. tipo de llamado` para Incendios, Rescates, Climáticas y Otros, junto al porcentaje y conteo esperado. Se diferencia de `Prob. actividad alta`, que conserva las alertas categóricas existentes.

- Selección: cada grupo se validó temporalmente contra una regresión Poisson estacional regularizada. Solo incendios y climáticas incorporan señal meteorológica adicional porque mejoraron fuera de muestra; rescates y otros conservan la base para evitar sobreajuste.

- Validación temporal final: MAE de incendios baja de 1,0624 a 1,0367 y el de climáticas de 0,2972 a 0,1858. Rescates y otros mantienen sus baselines, con MAE de 1,3160 y 1,1506.

## 2026-07-16 - Nivel de actividad por conteo operacional

- Cambio: el badge principal deja de usar percentiles internos del modelo y pasa a cortes estables por llamados esperados: `Baja <4`, `Normal 4–<6`, `Alta 6–<8` y `Muy alta ≥8`.
  Motivo: impedir que una prediccion cercana a 5,7 sea rotulada como muy alta solo por pertenecer al 20% superior de una distribucion comprimida.

- Cambio: se retira `Pulso 1–100` de las tarjetas para evitar una segunda escala que compita con el conteo y el nivel operacional.

- Cambio: se retiran de Forecast los despachos y la probabilidad de alta actividad de 5ta Cia, incluida su fila de referencia historica.

- Cambio: el rango probable 90% gana contraste, tamano y un fondo sutil para facilitar su lectura en las tarjetas.

- Cambio: se elimina la pestaña final `Comparacion de Modelos`; los tres artefactos de respaldo permanecen intactos.

## 2026-07-16 - Recalibracion temporal de riesgos por categoria

- Cambio: rescate vehicular, incendio y climaticas aplican calibracion sigmoidal sobre probabilidades Random Forest generadas OOF temporalmente. El modelo final conserva el ranking y entrega porcentajes compatibles con la frecuencia observada.

- Validacion en el 20% temporal final: Brier baja de 0,1613 a 0,1004 en rescate, de 0,1519 a 0,1107 en incendio y de 0,0287 a 0,0279 en climaticas; el ROC-AUC permanece en 0,596, 0,645 y 0,888 respectivamente.

- Cambio: la tabla de percentiles y los badges usan ahora la misma distribucion OOF calibrada. Se elimina la mezcla anterior entre probabilidades in-sample y umbrales OOF.

- Cambio: los badges categóricos dejan de traducir percentiles relativos como severidad absoluta. Usan cortes operacionales sobre la probabilidad calibrada: Baja <5%, Normal 5-<15%, Alta 15-<30% y Muy Alta >=30%.

## 2026-07-16 - Compatibilidad con hot-reload de Streamlit

- Correccion: si el proceso de Streamlit conserva en `sys.modules` una version anterior de `model_components`, el dashboard recarga el modulo antes de deserializar el ensemble. Esto evita `Can't get attribute 'HydroObjectiveEnsembleRegressor'` durante un despliegue sin reinicio completo del worker.

- Cambio: se incrementa la version de cache de datos/modelos para descartar cualquier resultado parcial del despliegue anterior.

## 2026-07-16 - Ensemble hidrometeorologico multiobjetivo

- Cambio: se promueve `signal_hydro_ensemble_v2`, un ensemble de cuatro cabezas XGBoost: 30% del modelo anterior, 40% squared-error depth 2, 15% Poisson y 15% quantile median. Una calibracion final expande moderadamente la dispersion (`1.15x`) sin agregar aleatoriedad.
  Motivo: producir una senal mas variable y estimulante, pero exigir simultaneamente mejor MAE, RMSE, R2 y ranking fuera de muestra.

- Cambio: se agregan diez senales hidrometeorologicas con paridad entre entrenamiento y serving: lluvia, chubascos, horas de chubasco/tormenta/trueno, nivel de congelacion y bulbo humedo. El dashboard y la CLI las calculan desde Open-Meteo con fallbacks deterministas.

- Resultado walk-forward, seis bloques de 120 dias: MAE baja de 2.254 a 2.236, RMSE de 2.926 a 2.909, R2 sube de 0.137 a 0.146 y AUC de ranking del conteo de 0.658 a 0.664. El ratio de variabilidad sube de 39.4% a 40.2% y la concentracion en 4-5,x baja de 63.3% a 62.5%; precision y recall top-20% se mantienen.

- Robustez: mejora MAE y RMSE en cinco de seis bloques; el bootstrap movil por bloques entrega 99.3% de soporte para la mejora de MAE y 91.4% para MSE. Tambien mejora en conjunto sobre los dos bloques temporales mas recientes.

- Verificacion: inferencia CLI con 118 variables sin faltantes y Streamlit AppTest sin excepciones ni errores. El comparador usa `active_models.json` para rotular correctamente el XGBoost anterior y el ensemble oficial.

- Respaldo: el registro de modelos principales conserva solo tres generaciones: `climatic_augmented` (base original), `signal_xgb_d3_flexible` (oficial anterior) y `signal_hydro_ensemble_v2` (nuevo oficial). Los modelos de riesgo por categoria y 5ta Compania permanecen como auxiliares necesarios para sus tarjetas, no como generaciones del predictor principal.

## 2026-07-16 - Rango predictivo en Forecast

- Cambio: cada tarjeta muestra en formato discreto un `Rango probable 90%` para el conteo diario.
  Metodo: percentiles 5 y 95 de los 720 residuos fuera de muestra del backtest walk-forward; el limite inferior se trunca en cero y los extremos se redondean hacia afuera.
  Motivo: comunicar incertidumbre sobre el resultado futuro sin presentar una desviacion estandar simetrica ni confundirla con un intervalo de confianza del promedio.

- Cambio: se retira del Forecast la probabilidad general de sobredemanda y sus resumenes; el conteo y el nivel operacional pasan a concentrar la lectura de carga diaria.

## 2026-07-16 - Rafaga media en Forecast

- Cambio: las tarjetas de Forecast muestran `Ráfaga media` desde `WX_GUST_MEAN`, separada de `Viento medio`.
  Contexto: `VIENTO_MAX` conserva su definicion de entrenamiento basada en velocidad sostenida maxima.
  Motivo: exponer la intensidad promedio de las rachas horarias sin introducir train/serve skew en el modelo activo; `WX_GUST_MAX` permanece como variable interna del modelo.

## 2026-07-16 - Promocion del modelo de alta resolucion en Streamlit

- Cambio: `active_models.json` activa `signal_xgb_d3_flexible` como modelo `climatic_augmented`; su metadata queda marcada `operational_use=true`, `is_primary=true` y `promoted_model=true` tras aprobacion explicita del usuario.
  Contexto: el candidato ya habia superado al modelo anterior en RMSE, R2, resolucion y deteccion del top 20% bajo seis bloques walk-forward.
  Motivo: usar el nuevo modelo en Forecast sin depender de copias sobre las rutas canonicas.

- Cambio: se agrego `signal_features.py` como fuente compartida de las 20 señales WX avanzadas y los lags/rolling de eventos. Dashboard y CLI solicitan las mismas variables horarias de Open-Meteo, aplican fallbacks deterministas y construyen las 108 columnas en el orden de entrenamiento.
  Contexto: el candidato consumia forecast historico, lluvia extrema, rachas, presion, VPD, CAPE, ET0, historial de eventos y categorias que el serving anterior no calculaba por completo.
  Motivo: eliminar train/serve skew antes de activar el modelo.

- Cambio: las predicciones historicas de Streamlit fusionan `historical_forecast_features.csv`; el forecast de seis dias actualiza recursivamente conteos y lags de categorias mediante proporciones recientes. Comparacion de Modelos muestra `ANTERIOR` vs. el nuevo `OFICIAL` con el mismo protocolo temporal.

- Verificacion: `predict_tomorrow.py --date 2026-04-16` produjo conteo y probabilidad con las 108 variables; Streamlit AppTest renderizo cinco pestañas, el Pulso y la etiqueta del modelo activo con cero excepciones, warnings de interfaz o errores.

## 2026-07-16 - Candidato XGBoost de alta resolucion y Pulso 1-100

- Cambio: se creo un benchmark walk-forward reproducible de seis bloques expansivos de 120 dias para evaluar simultaneamente error, deteccion de dias altos y resolucion de la señal. Se probaron 46 combinaciones viables de familias/objetivos y grupos de variables, mas calibraciones secuenciales y ablations dirigidas; `RandomForestRegressor(criterion="absolute_error")` se descarto por costo computacional no viable.
  Contexto: el modelo oficial concentra 73,9% de las predicciones temporales entre 4 y 5,x y conserva solo 30,3% de la desviacion real.
  Motivo: evitar seleccionar extremos llamativos que empeoren la prediccion y comparar todos los candidatos sobre exactamente los mismos dias futuros.

- Resultado: el candidato `signal_xgb_d3_flexible` usa XGBoost squared-error y 108 variables operacionales, incluyendo historia reciente y 20 señales de forecast historico con cobertura completa. En 720 dias walk-forward obtiene MAE 2,254, RMSE 2,926, R2 0,137 y AUC de ranking de conteo 0,658, frente a MAE 2,267, RMSE 3,017, R2 0,082 y AUC 0,653 del oficial temporal.
  Contexto: la mejora de MAE es pequeña y no concluyente en bootstrap por bloques, pero la mejora de MSE tiene 95,9% de soporte; gana MAE en tres de seis bloques.
  Motivo: conservar una lectura honesta de la incertidumbre estadistica en vez de sobredimensionar una diferencia de 0,013 eventos.

- Resultado de resolucion: el ratio de variabilidad sube de 30,3% a 39,4%, las salidas 4-5,x bajan de 73,9% a 63,3% y el top 20% del score mejora precision de dias >7 desde 31,3% a 38,2% y recall desde 33,8% a 41,4%.

- Cambio: la probabilidad de sobredemanda del candidato se calibra con Platt scaling sobre el score de conteo mediante `RegressorProbabilityClassifier`. En backtest temporal obtiene ROC-AUC 0,639 y Brier 0,144, frente a 0,592 y 0,150 del clasificador oficial equivalente.
  Motivo: reutilizar el ranking mas informativo del conteo y evitar una segunda cabeza XGBoost que solo obtuvo AUC 0,625.

- Cambio: Forecast agrega `Pulso 1-100`, percentil empirico del conteo previsto, manteniendo visibles las llamadas esperadas. Comparacion de Modelos muestra Oficial vs. Candidato con metricas walk-forward comparables, ratio de variabilidad, frecuencia 4-5 y precision/recall del top 20%.
  Motivo: ofrecer una señal dinamica y dopaminica sin convertir ruido aleatorio en un conteo falso.

- Gate: los artefactos `regressor_signal_xgb_d3_flexible.pkl`, `classifier_signal_xgb_d3_flexible.pkl` y `metadata_signal_xgb_d3_flexible.pkl` se guardaron como candidato. El modelo oficial y `active_models.json` no fueron modificados; cualquier promocion requiere confirmacion explicita del usuario.

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
