# Programa de mejora predictiva

Este directorio contiene experimentos aislados. Ningún script debe sobrescribir
los datos, modelos o metadatos usados por el dashboard de producción.

## Objetivo

Mejorar dos salidas distintas:

1. Conteo esperado de llamados diarios.
2. Riesgo calibrado de un día de alta actividad.

ROC-AUC no representa porcentaje de aciertos. El modelo actual es el campeón
hasta que un candidato gane bajo el protocolo temporal definido aquí.

## Política de ceros

- `observed_zero`: cobertura del día confirmada y ningún incidente observado.
  Se conserva como target `0` y participa en entrenamiento.
- `observed_nonzero`: cobertura confirmada con uno o más incidentes únicos.
  Participa en entrenamiento.
- `coverage_unknown`: archivo faltante, scraping fallido, respuesta parcial o
  cobertura no verificable. Target queda nulo y se excluye de entrenamiento y
  evaluación.

Nunca convertir `coverage_unknown` a cero.

## Definición del target

El target experimental debe representar incidentes únicos, no publicaciones.
Como mínimo:

- convertir timestamps a `America/Santiago`;
- excluir mensajes puramente operativos, como estados de unidades;
- agrupar ampliaciones o despachos del mismo incidente mediante identificadores
  disponibles, código, ubicación y proximidad temporal;
- conservar trazabilidad desde cada incidente agregado a sus publicaciones.

Las reglas automáticas deben validarse manualmente sobre una muestra etiquetada.

## Protocolo de evaluación

- Mantener un test final bloqueado de 9 a 12 meses.
- Usar rolling-origin en el período de desarrollo.
- Ejecutar selección de variables, hiperparámetros y calibración dentro de cada
  fold, sin usar el bloque de validación para preparar candidatos.
- Separar selección para regresión y clasificación.
- Comparar siempre contra mediana histórica y media móvil de 28 días.
- No promover modelos usando una única partición o una única métrica.

## Métricas mínimas

Conteo:

- MAE, RMSE, R2;
- MASE contra media móvil de 28 días;
- Poisson deviance;
- cobertura y amplitud de intervalos predictivos.

Riesgo:

- ROC-AUC y PR-AUC;
- Brier score y log-loss;
- calibración por deciles;
- precision, recall y F1;
- alertas por semana, falsas alertas por acierto y críticos omitidos.

## Criterio de promoción

Un candidato reemplaza al campeón solo si:

1. mejora o iguala baselines en la mayoría de folds;
2. no empeora materialmente en estaciones o períodos recientes;
3. entrega probabilidades mejor calibradas;
4. mantiene costo operacional aceptable;
5. gana en el test bloqueado abierto una sola vez.

## Estructura

- `data_quality/`: auditoría y reconstrucción del target.
- `backtesting/`: folds, baselines y métricas comunes.
- `models/`: candidatos y calibradores.

## Dataset experimental

Construir el dataset con target auditado:

```powershell
.\.venv\Scripts\python.exe 05_research\build_experimental_dataset.py
```

La salida queda en `05_research/data/experimental_target_dataset.csv`. Conserva
`EVENTOS_ORIGINAL`, usa `EVENTOS` para el conteo auditado, mantiene ceros
confirmados y deja `coverage_unknown` como target vacio.

## Ablacion meteorologica

```powershell
.\.venv\Scripts\python.exe 05_research\weather_features.py `
  --start-date 2022-02-03 --end-date 2026-04-17

.\.venv\Scripts\python.exe 05_research\run_weather_ablation.py
```

El experimento usa Historical Forecast API, excluye targets cero agregados y
compara grupos de lluvia/viento, atmosfera y riesgo de incendio. El resumen
queda en `05_research/results/weather_ablation/experiment_summary.json`.
