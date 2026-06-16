# Hallazgos del equipo predictivo

## Diagnostico

El valor cercano a 61% corresponde a ROC-AUC, no a porcentaje de aciertos ni a
confiabilidad. La evaluacion actual mezcla tres problemas:

1. El target cuenta publicaciones y mensajes operativos, no siempre incidentes.
2. Dias sin cobertura del scraper fueron tratados como cero.
3. El entrenamiento usa clima observado del dia completo, mientras produccion
   usa un pronostico meteorologico.

Por esto, optimizar solamente el algoritmo puede mejorar una metrica sin mejorar
la operacion real.

## Politica de ceros

- Un cero con cobertura confirmada es dato real y debe entrenar.
- Un dia sin archivo, con archivo vacio no verificado o scraping incompleto debe
  quedar con target nulo.
- En el dataset experimental quedaron 1.444 dias observados, 91 desconocidos y
  22 ceros confirmados dentro del periodo con variables de modelo.

## Reconstruccion preliminar

La auditoria:

- convierte UTC a `America/Santiago`;
- excluye 820 mensajes de estado de unidades;
- identifica 2.246 publicaciones candidatas a duplicado;
- deja 143 dias con cobertura desconocida en el universo completo.

La reconstruccion automatica es un limite inferior. Debe validarse manualmente
antes de reemplazar el target productivo.

## Benchmarks

Target original, clima solamente:

- mejor MAE: 2,637;
- mejor ROC-AUC agregado: 0,606;
- prevalencia de dia critico `>7`: 22,1%.

Target auditado, clima solamente:

- mejor MAE: 1,895;
- ROC-AUC de riesgo Negative Binomial: 0,634 con `>7`;
- prevalencia `>7`: 8,7%.

El descenso del MAE no prueba mejora por si solo: el target auditado tiene menor
media y varianza. La media baja de 5,48 a 4,21 incidentes.

Para conservar una frecuencia critica parecida, `>5` entrega 24,2%. Con ese
target:

- clima solamente: ROC-AUC 0,576 para riesgo Negative Binomial;
- clima + calendario: ROC-AUC 0,574;
- umbral de alerta 30%: precision 30,9%, recall 33,1% con clima + calendario;
- umbral 50% pierde demasiados dias criticos.

En conteo, la mediana historica obtuvo MAE 1,856 en el backtest rolling-origin.
Los modelos evaluados quedaron cerca, pero no la superaron de forma convincente.

## Decision

No promover ningun modelo experimental todavia. Mantener produccion estable y
trabajar en este orden:

1. Etiquetar manualmente muestra estratificada de incidentes, duplicados y ceros.
2. Congelar definicion del target y severidad operacional.
3. Crear archivo historico de pronosticos emitidos antes de las 06:00.
4. Repetir rolling-origin con el mismo dato disponible en entrenamiento y uso.
5. Agregar calendario enriquecido, extremos meteorologicos e indice de incendio.
6. Agregar mareas y combinacion lluvia + pleamar.
7. Evaluar SENAPRED, calidad del aire y eventos planificados.

## Equipo recomendado

- Calidad de datos: cobertura, timezone, deduplicacion y target.
- Validacion: holdout sellado, rolling-origin, baselines y costos de alerta.
- Modelado: conteos sobredispersos, calibracion e intervalos predictivos.
- Senales: clima de forecast, calendario, mareas, incendio y alertas externas.
- Integracion: solo promueve candidatos que ganen en varios periodos y mantengan
  costo operacional aceptable.

## Archivos

- `data_quality/`: auditoria diaria y trazabilidad.
- `build_experimental_dataset.py`: separa cero observado de cobertura desconocida.
- `backtesting/`: folds temporales, baselines y metricas.
- `models/benchmark.py`: modelos de conteo y riesgo calibrado.
- `results/`: resultados reproducibles de los experimentos.
