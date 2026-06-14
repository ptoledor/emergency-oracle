# 🧯 Emergency Oracle — Predictor de Emergencias para Bomberos de Talcahuano

## Resumen Ejecutivo

**Emergency Oracle** es un sistema de predicción de emergencias de extremo a extremo para el **Cuerpo de Bomberos de Talcahuano (CBT)**, Chile. El sistema predice la cantidad de llamadas de emergencia diarias y la probabilidad de días críticos (>7 eventos), permitiendo la planificación preventiva de guardias y dotación de personal.

El pipeline completo abarca: **scraping de datos → limpieza y aumentación → entrenamiento de modelos → inferencia en tiempo real → dashboard interactivo**.

---

## Ubicación Geográfica

- **Ciudad:** Talcahuano, Región del Biobío, Chile
- **Coordenadas:** -36.731106, -73.11023
- **Fuente de Datos Climáticos:** API Open-Meteo (archivo histórico y pronóstico)

---

## Estructura del Proyecto

```
emergency-oracle/
│
├── 01_scraper/                          # Módulo de recolección de datos
│   ├── twitter_scraper.py               # Scraper de tweets de @Central_CBT (Playwright + Stealth)
│   ├── cookies.json                     # Cookies de sesión para Twitter/X
│   └── scraped_data/                    # CSVs diarios generados por el scraper
│
├── 02_data/                             # Módulo de datos y preprocesamiento
│   ├── compiled_scraped_data.csv        # Tweets compilados (fuente primaria cruda)
│   ├── Clave_CBT.xlsx                   # Catálogo de códigos de emergencia CBT (10-X-X)
│   ├── clean_and_augment.py             # Script de limpieza y feature engineering
│   ├── augmented_emergency_data.csv     # Dataset final aumentado (salida de clean_and_augment.py)
│   ├── weather_archive_talcahuano.csv   # Cache local del archivo climático Open-Meteo
│   ├── tweets_procesados.csv            # Tweets procesados intermedios
│   ├── puntos_emergencias.gpkg          # GeoPackage con ubicaciones de emergencias
│   ├── process_data.ipynb               # Notebook exploratorio de procesamiento
│   └── process_data_clusters.ipynb      # Notebook de análisis de clusters geoespaciales
│
├── 03_model/                            # Módulo de entrenamiento
│   ├── train.py                         # Script principal de entrenamiento (3 pipelines)
│   └── saved_models/                    # Modelos serializados (.pkl)
│       ├── regressor_agnostic.pkl       # Modelo Base — Regresor
│       ├── classifier_agnostic.pkl      # Modelo Base — Clasificador
│       ├── metadata_agnostic.pkl        # Modelo Base — Metadatos
│       ├── regressor_agnostic_augmented.pkl     # Modelo Climático con Inercia — Regresor
│       ├── classifier_agnostic_augmented.pkl    # Modelo Climático con Inercia — Clasificador
│       ├── metadata_agnostic_augmented.pkl      # Modelo Climático con Inercia — Metadatos
│       ├── regressor_agnostic_augmented_v3.pkl  # Modelo Climático (Puro) — Regresor
│       ├── classifier_agnostic_augmented_v3.pkl # Modelo Climático (Puro) — Clasificador
│       └── metadata_agnostic_augmented_v3.pkl   # Modelo Climático (Puro) — Metadatos
│
├── 04_predict/                          # Módulo de inferencia
│   └── predict_tomorrow.py              # CLI para predicción individual por fecha
│
└── dashboard.py                         # Dashboard Streamlit interactivo (1279 líneas)
```

---

## Pipeline de Datos (Paso a Paso)

### Paso 1: Scraping (`01_scraper/twitter_scraper.py`)
- **Fuente:** Cuenta oficial de Twitter/X `@Central_CBT`.
- **Método:** Playwright con stealth mode; scraping incremental por día.
- **Rango:** Desde 2010-01-01 hasta hoy.
- **Salida:** Un CSV por día en `01_scraper/scraped_data/`, luego compilados en `02_data/compiled_scraped_data.csv`.
- **Campos clave:** `Fecha`, `Texto` (texto del tweet con código de emergencia tipo `10-X-X`).

### Paso 2: Limpieza y Aumentación (`02_data/clean_and_augment.py`)
1. **Extracción de códigos de emergencia** del texto de cada tweet usando regex (`\b(10-\d+(?:-\d+)?)\b`).
2. **Imputación de códigos faltantes:** Clasifica tweets sin código explícito como `10-2-3` (pastizal/forestal) o `10-0-6` (incendio) según palabras clave.
3. **Merge con catálogo CBT** (`Clave_CBT.xlsx`) para obtener la categoría de emergencia.
4. **Agregación diaria:** Conteo total de eventos por día + conteo por categoría (incendio estructural, forestal, rescate vehicular, rescate personas, gases).
5. **Serie temporal continua:** Rellena días sin tweets con 0 eventos.
6. **Feature engineering climático:**
   - Descarga datos horarios de Open-Meteo (temperatura, humedad, viento, precipitación).
   - Calcula estadísticos diarios: **máx, mín, media, asimetría (skewness) y curtosis** de cada variable meteorológica a partir de las 24 lecturas horarias.
   - Lags climáticos: `LLUVIA_lag_1/2/3`, `LLUVIA_accum_3d`, `LLUVIA_rolling_mean_7d`, `VIENTO_MEDIO_lag_1`, `HUM_MEDIA_lag_1`.
7. **Feature engineering operativo:**
   - Lags de eventos: `EVENTOS_lag_1/2/3/7`.
   - Lags por categoría: `N_INCENDIO_ESTR_lag_1`, `N_INCENDIO_FOREST_lag_1`, etc.
   - Estadísticas móviles: `EVENTOS_rolling_mean_3d/7d`, `EVENTOS_rolling_std_3d/7d`, `EVENTOS_rolling_max_3d/7d`.
8. **Features de calendario:** Mes, día de semana, fin de semana, feriado (librería `holidays.Chile`), feriado irrenunciable, encodings cíclicos seno/coseno.
9. **Salida:** `augmented_emergency_data.csv` (~1560 días × 42 columnas).

### Paso 3: Entrenamiento (`03_model/train.py`)
- **Algoritmo:** Scikit-learn `GradientBoostingRegressor` + `GradientBoostingClassifier`.
- **Hiperparámetros:** `n_estimators=150`, `max_depth=4`, `min_samples_leaf=5`, `learning_rate=0.05`, `subsample=0.8`.
- **Split temporal:** 80% entrenamiento / 20% test (sin shuffle, respeta cronología).
- **Feature pruning automático:** Se eliminan variables con importancia < 0.8% en el set de entrenamiento.
- **Umbral de clasificación:** Optimizado por el índice Youden J (máximo de Sensibilidad + Especificidad - 1) sobre la curva ROC del set de test.
- **Tres pipelines se entrenan:**

| Prefijo Interno | Nombre en UI | Excluye | Variables Clave |
|---|---|---|---|
| `_agnostic` | **Modelo Base** | Calendario + Skew/Kurt | Solo medias y extremos climáticos + lags operativos |
| `_agnostic_augmented` | **Modelo Climático con Inercia de Actividad** | Solo calendario | Clima completo (con skew/kurt) + lags operativos |
| `_agnostic_augmented_v3` | **Modelo Climático (Puro)** | Calendario + todos los lags/rollings de eventos | Solo señal climática, sin historia de llamadas |

- **Cada pipeline guarda 3 archivos:** `regressor{prefix}.pkl`, `classifier{prefix}.pkl`, `metadata{prefix}.pkl`.
- **Metadata incluye:** `feature_cols`, `classification_threshold`, `umbral_alta_actividad`, métricas de evaluación (MAE, MSE, R², Accuracy, Precision, Recall, F1, ROC-AUC), e importancia de características.

### Paso 4: Inferencia (`04_predict/predict_tomorrow.py`)
- **CLI con argumentos:**
  - `--date YYYY-MM-DD` → predice una fecha específica.
  - `--real-tomorrow` → predice el día de mañana real.
  - `--v3` → usa el Modelo Climático (Puro) en vez del Modelo Climático con Inercia de Actividad.
  - Sin argumentos → predice el día siguiente al último dato del dataset.
- **Proceso:**
  1. Carga modelo y metadatos seleccionados.
  2. Obtiene lags de eventos del CSV local (imputa media histórica de 5.46 si la fecha está fuera del dataset).
  3. Descarga clima horario real o pronosticado desde Open-Meteo.
  4. Calcula skew/kurt de cada variable sobre las 24h.
  5. Construye vector de features y predice.
  6. Emite reporte con cantidad esperada de emergencias, probabilidad crítica y recomendación operativa.

---

## Dashboard Interactivo (`dashboard.py`)

**Stack:** Streamlit + Plotly + CSS custom (tema claro/oscuro).

### Pestañas:

| Pestaña | Contenido |
|---|---|
| **🔮 Predicciones 7 Días** | Curva de tendencia recursiva de los 3 modelos superpuestos + tarjetas diarias con clima y semáforo de alerta |
| **⚡ Importancia de Variables** | Tablas de métricas side-by-side (3 columnas) + 3 gráficos de barras horizontales de importancia relativa (750px) + glosario completo |
| **📊 Curvas de Estacionalidad** | Promedio por día del año (1–365) con slider de suavizado interactivo + selector de modelo por radio button |
| **💡 Recomendaciones** | Sugerencias de mejora del sistema |

### Predicción Recursiva a 7 Días:
El dashboard ejecuta una simulación autoregresiva: para cada día del horizonte de predicción, usa la predicción del día anterior como lag de entrada del día siguiente. Esto permite generar pronósticos de 7 días incluso cuando no hay datos reales futuros.

### KPIs Principales:
- **Media de Eventos Reales** vs. **Media de Eventos Predichos** (diario).
- **MAE del Modelo Climático con Inercia** vs. **MAE del Modelo Base**.

---

## Métricas de Rendimiento en Test

| Métrica | Modelo Base | Modelo Climático con Inercia de Actividad | Modelo Climático (Puro) |
|---|---|---|---|
| MAE | 2.57 | **2.55** | 2.62 |
| MSE | 9.84 | **9.58** | 10.24 |
| R² | -1.4% | **+1.3%** | -5.4% |
| Umbral Youden J | 0.24 | 0.20 | 0.16 |
| Accuracy | **74.4%** | 68.6% | 49.7% |
| Precision | 21.2% | **24.3%** | 19.9% |
| Recall | 31.5% | 53.7% | **63.0%** |
| F1-Score | 25.4% | **33.5%** | 30.2% |

> **Interpretación:** El Modelo Climático con Inercia de Actividad es el mejor equilibrio entre precisión de regresión y detección de días críticos. El Modelo Climático (Puro) demuestra que la señal del clima por sí sola explica el 63% de los días de alta demanda, pero necesita la historia de actividad para calibrar el volumen absoluto.

---

## Entorno y Dependencias

- **Python:** 3.12 (Miniforge/Conda)
- **Entorno virtual:** `geo312` (`C:\Users\ptole\miniforge3\envs\geo312\python.exe`)
- **Dependencias principales:**
  - `streamlit` — Dashboard web
  - `plotly` — Gráficos interactivos
  - `scikit-learn` — Gradient Boosting (Regresor + Clasificador)
  - `pandas`, `numpy` — Manipulación de datos
  - `requests` — API Open-Meteo
  - `holidays` — Feriados de Chile
  - `playwright`, `playwright-stealth` — Web scraping
  - `openpyxl` — Lectura de Excel (Clave_CBT.xlsx)

---

## Cómo Ejecutar

```bash
# Activar entorno
conda activate geo312

# 1. Scraping (opcional, solo si se necesitan más datos)
cd 01_scraper
python twitter_scraper.py

# 2. Limpieza y aumentación
cd ../02_data
python clean_and_augment.py

# 3. Entrenamiento de modelos
cd ../03_model
python train.py

# 4a. Predicción por consola
cd ../04_predict
python predict_tomorrow.py                    # Siguiente al dataset
python predict_tomorrow.py --real-tomorrow    # Mañana real
python predict_tomorrow.py --v3               # Modelo Climático (Puro)

# 4b. Dashboard interactivo
cd ..
streamlit run dashboard.py                    # http://localhost:8502
```

---

## Variables del Dataset Final (`augmented_emergency_data.csv`)

### Meteorológicas del Día (15 variables)
| Variable | Descripción |
|---|---|
| `TEMP_MAX`, `TEMP_MIN`, `TEMP_MEDIA` | Temperatura máxima, mínima y media (°C) |
| `TEMP_SKEW`, `TEMP_KURT` | Asimetría y curtosis del perfil horario de temperatura |
| `HUM_MAX`, `HUM_MIN`, `HUM_MEDIA` | Humedad relativa máxima, mínima y media (%) |
| `HUM_SKEW`, `HUM_KURT` | Asimetría y curtosis del perfil horario de humedad |
| `VIENTO_MAX`, `VIENTO_MEDIO` | Velocidad del viento máxima y media (km/h) |
| `VIENTO_SKEW`, `VIENTO_KURT` | Asimetría y curtosis del perfil horario del viento |
| `LLUVIA` | Precipitación acumulada diaria (mm) |

### Lags Climáticos (7 variables)
| Variable | Descripción |
|---|---|
| `LLUVIA_lag_1`, `LLUVIA_lag_2`, `LLUVIA_lag_3` | Lluvia de hace 1, 2 y 3 días |
| `LLUVIA_accum_3d` | Lluvia acumulada últimos 3 días |
| `LLUVIA_rolling_mean_7d` | Media de lluvia últimos 7 días |
| `VIENTO_MEDIO_lag_1` | Viento medio de ayer |
| `HUM_MEDIA_lag_1` | Humedad media de ayer |

### Lags Operativos de Eventos (15 variables)
| Variable | Descripción |
|---|---|
| `EVENTOS_lag_1/2/3/7` | Total de llamadas hace 1, 2, 3 y 7 días |
| `N_INCENDIO_ESTR_lag_1` | Incendios estructurales de ayer |
| `N_INCENDIO_FOREST_lag_1` | Incendios forestales de ayer |
| `N_RESCATE_VEH_lag_1` | Rescates vehiculares de ayer |
| `N_RESCATE_PERS_lag_1` | Rescates de personas de ayer |
| `N_GASES_lag_1` | Emanaciones de gases de ayer |
| `EVENTOS_rolling_mean_3d/7d` | Media móvil de eventos (3 y 7 días) |
| `EVENTOS_rolling_std_3d/7d` | Desviación estándar móvil (3 y 7 días) |
| `EVENTOS_rolling_max_3d/7d` | Máximo móvil (3 y 7 días) |

### Calendario (11 variables — excluidas en todos los modelos actuales)
| Variable | Descripción |
|---|---|
| `MES`, `DIA_SEMANA` | Mes (1–12) y día de semana (0=Lun, 6=Dom) |
| `ES_FIN_SEMANA`, `ES_FERIADO` | Flags binarios |
| `ES_FERIADO_IRRENUNCIABLE` | Feriados con cierre obligatorio de comercio |
| `MES_SIN/COS`, `DIA_SIN/COS`, `DANO_SIN/COS` | Encodings cíclicos seno/coseno |

### Variable Objetivo
| Variable | Descripción |
|---|---|
| `EVENTOS` | Cantidad total de llamadas de emergencia del día |

---

## Decisiones de Diseño Clave

1. **Sin variables de calendario:** Los tres modelos activos excluyen mes, día de semana y encodings cíclicos para evitar sobreajuste estacional y garantizar adaptabilidad al cambio climático.
2. **Asimetría y curtosis horaria:** El diferencial clave entre el Modelo Base y los modelos Climáticos es la inclusión de skewness y kurtosis calculados sobre las 24 lecturas horarias de cada variable meteorológica. Estos capturan la dinámica intra-diaria (ráfagas de viento, caídas de humedad, picos térmicos) que las medias diarias ocultan.
3. **Umbral Youden J:** En vez del umbral por defecto de 0.50, se calibra un umbral de clasificación que maximiza la detección de días críticos controlando falsas alarmas. Esto es vital porque los días de alta demanda representan solo ~15% de los datos.
4. **Feature pruning dinámico:** Variables con importancia < 0.8% se eliminan automáticamente en entrenamiento para reducir ruido.
5. **Imputación de lags futuros:** Cuando se predice más allá del dataset (lags desconocidos), se imputa la media histórica de 5.46 eventos/día en vez de asumir 0.
