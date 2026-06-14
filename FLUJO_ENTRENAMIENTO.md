# Flujo de entrenamiento

## 1. Preparación de datos

`02_data/clean_and_augment.py` limpia la serie diaria y genera variables climáticas aumentadas: rezagos (`LAG`), medias, máximos y desviaciones móviles, estacionalidad y día de la semana.

Las variables que contienen resultados futuros o actividad operativa se excluyen para evitar fuga de información. La división es temporal: 80% para entrenamiento y 20% final para prueba.

## 2. Selección de variables

`03_model/train.py` ordena las variables por importancia promedio usando cinco cortes de `TimeSeriesSplit`. Después prueba subconjuntos de distintos tamaños y combinaciones en paralelo.

La selección usa desempeño fuera de muestra temporal, principalmente MAE, junto con ROC-AUC y F1. El conjunto directo seleccionado contiene 31 variables.

## 3. Selección de modelos

Se entrenan dos enfoques:

1. **Directo:** `GradientBoostingRegressor` predice el total diario con 31 variables.
2. **Por categorías:** predice por separado seis tipos de emergencia y luego los suma. Cada categoría selecciona su familia y cantidad de variables mediante validación temporal.

La predicción principal combina ambos enfoques. Se probaron 101 pesos, de 0% a 100% en pasos de 1%, usando únicamente predicciones OOF temporales. La mejor mezcla fue:

```text
Predicción final = 53% modelo directo + 47% modelos por categoría
```

## 4. Resultados

| Modelo | MAE prueba | R² prueba | MAE temporal OOF |
|---|---:|---:|---:|
| Directo, 31 variables | 2.538 | -0.023 | 2.735 |
| Optimizado por categorías, principal | **2.418** | **0.049** | **2.640** |

Clasificación de días de alta actividad: ROC-AUC `0.610` y F1 `0.343`.

El R² continúa siendo bajo, por lo que una parte importante de la variación diaria sigue siendo aleatoria o depende de información actualmente no disponible.

## 5. Ejecución

```powershell
python 02_data/clean_and_augment.py
python 03_model/train.py
streamlit run dashboard.py --server.port 8502
```

`train.py` ejecuta la selección de variables y, al final, la optimización por categorías y de los pesos de mezcla.
