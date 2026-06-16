# Handoff Viper Dedup

Estado al cierre:

- El pipeline de deduplicacion esta en `05_research/viper_dedup.py`.
- La entrada por defecto es `02_data/tweets_procesados.csv`.
- La salida por defecto queda en `05_research/data/viper_dedup/`.
- La cache de URLs queda en `05_research/data/viper_coordinate_cache.json`.
- Regla principal: si el mismo punto `x,y` aparece nuevamente dentro de 2 horas, se conserva solo el primer mensaje.
- Regla fallback: si no hay coordenada, se deduplica por misma `URL` dentro de 2 horas.

## Bloqueo en este PC

Este computador no puede acceder a `t.co` correctamente:

- `requests` falla por handshake SSL.
- `curl.exe --ssl-no-revoke` llega a `Cisco Umbrella 403`.
- Por eso aqui no se pudo validar scraping real de coordenadas Viper.

Resultado aun sin Viper, usando solo misma URL dentro de 2 horas:

```json
{
  "messages": 10494,
  "unique_urls": 3037,
  "coordinates_ok": 0,
  "duplicates_removed": 1898,
  "duplicates_by_xy": 0,
  "duplicates_by_url": 1898,
  "daily_rows": 1525,
  "window_hours": 2.0
}
```

## Continuar en PC desbloqueado

Primero traer cambios:

```powershell
git pull
```

Validar tests:

```powershell
.venv\Scripts\python.exe -m unittest 05_research.models.tests.test_viper_dedup
```

Intentar resolver URLs con `curl`:

```powershell
.venv\Scripts\python.exe 05_research\viper_dedup.py --resolver curl --retry-failed
```

Si funciona, revisar:

```powershell
Get-Content 05_research\data\viper_dedup\summary.json
```

El campo clave es `coordinates_ok`. Si queda mayor que cero, ya tenemos coordenadas Viper y podemos pasar a benchmark con target deduplicado.

## Archivos generados

- `05_research/data/viper_dedup/messages_viper_deduplicated.csv`
  - Mensaje original + coordenada si existe.
  - `incident_rank = 1`: cuenta como incidente.
  - `incident_rank = 0`: duplicado dentro de ventana.
  - `dedup_reason`: explica si fue `duplicate_xy_2h_of:*` o `duplicate_url_2h_of:*`.

- `05_research/data/viper_dedup/daily_viper_target.csv`
  - Target diario deduplicado.
  - `target_count`: incidentes contados.
  - `raw_message_count`: mensajes originales.
  - `duplicate_xy_2h_count`: duplicados removidos.

## Siguiente paso recomendado

Despues de obtener coordenadas reales:

1. Recalcular `EVENTOS` usando `daily_viper_target.csv`.
2. Recalcular lags y rolling features de `EVENTOS`.
3. Correr benchmark temporal contra el modelo actual.
4. Promover solo si mejora MAE en al menos 3 de 5 folds y no empeora Brier/ROC-AUC.

