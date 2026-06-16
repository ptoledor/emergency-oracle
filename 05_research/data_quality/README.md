# Auditoria experimental del target

Herramienta aislada para auditar cobertura del scraping y reconstruir un target
diario preliminar. No modifica datos, modelos ni pipeline productivo.

## Principios

- Convierte timestamps UTC a `America/Santiago`.
- Separa mensajes `ESTADO DE UNIDADES`.
- Marca posibles duplicados por URL exacta, texto exacto o
  `codigo + ubicacion` dentro de una ventana temporal.
- Un CSV vacio no demuestra cero emergencias por defecto.
- Un dia con cobertura incompleta queda `coverage_unknown`.
- `target_count` queda vacio para cobertura desconocida. Nunca se imputa como cero.
- La reconstruccion cuenta solo mensajes parecidos a incidentes y colapsa grupos
  candidatos. Es un limite inferior experimental, no un target aprobado.

## Uso

Desde la raiz del proyecto:

```powershell
.\.venv\Scripts\python.exe 05_research\data_quality\audit_target.py
```

Opciones:

```powershell
.\.venv\Scripts\python.exe 05_research\data_quality\audit_target.py --help
.\.venv\Scripts\python.exe 05_research\data_quality\audit_target.py `
  --duplicate-window-minutes 30 `
  --output-dir 05_research\data_quality\output
```

`--trust-empty-files` permite considerar CSV vacios como ceros observados. Es una
opcion deliberadamente no predeterminada porque el scraper puede marcar fechas
terminadas tras respuestas fallidas.

## Reportes

- `source_coverage.csv`: estado de cada fecha/archivo del scraper.
- `message_audit.csv`: mensajes convertidos a Santiago y sus banderas.
- `duplicate_candidates.csv`: mensajes pertenecientes a grupos candidatos.
- `daily_target_audit.csv`: estado diario y target reconstruido.
- `parse_errors.csv`: filas con timestamps invalidos.
- `summary.csv`: resumen de cobertura, mensajes y estados diarios.

Estados diarios:

- `observed_zero`: cobertura completa y cero incidentes reconstruidos.
- `observed_nonzero`: cobertura completa y uno o mas incidentes reconstruidos.
- `coverage_unknown`: falta al menos una fuente UTC necesaria, el archivo esta
  vacio sin verificar, no figura como completado o no pudo leerse.

Un dia local de Santiago cruza normalmente dos fechas UTC. Por eso la cobertura
solo se considera completa cuando ambas fuentes UTC son utilizables.

## Pruebas

```powershell
.\.venv\Scripts\python.exe -m unittest discover `
  -s 05_research\data_quality\tests -v
```

## Limitaciones

- `progress.json` no prueba que una respuesta haya sido completa.
- Las reglas de duplicados generan candidatos; requieren muestra validada a mano.
- Mensajes sin codigo ni palabra `EMERGENCIA` quedan auditados pero fuera del
  conteo reconstruido.
- Antes de entrenar, revisar una muestra estratificada de mensajes y dias cero.

