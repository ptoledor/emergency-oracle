import pandas as pd
import numpy as np
import requests
import os
import time
from pathlib import Path

import sys
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from dedup import mark_duplicates, assign_local_date, DEFAULT_TIMEZONE

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from features import (
    CALENDAR_COLUMNS,
    CATEGORY_SOURCES,
    EVENT_HISTORY_COLUMNS,
    OPEN_METEO_HOURLY_QUERY,
    WEATHER_DERIVED_COLUMNS,
    aggregate_weather_daily,
    calendar_features,
    event_history_table,
    weather_derived_features,
)

WEATHER_CACHE_VERSION = "historical_forecast_hourly_v2"
PROJECT_TIMEZONE = DEFAULT_TIMEZONE


def fetch_json_with_retry(url, timeout=30, retries=3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(2 * attempt)
    raise RuntimeError(f"Error Open-Meteo despues de {retries} intentos: {last_error}")


def load_historical_weather(start_date, end_date, cache_path):
    """Daily weather from the Historical Forecast API, cached by version and range."""
    version_path = cache_path.parent / ".weather_cache_version"
    if cache_path.exists() and version_path.exists():
        cached = pd.read_csv(cache_path)
        if (
            version_path.read_text(encoding="utf-8").strip() == WEATHER_CACHE_VERSION
            and str(cached['FECHA_DIA'].min()) <= start_date
            and str(cached['FECHA_DIA'].max()) >= end_date
        ):
            print(f"Cargando clima desde caché (versión={WEATHER_CACHE_VERSION})...")
            return cached

    print("Descargando clima horario (Historical Forecast API) desde Open-Meteo...")
    chunks = []
    chunk_start = pd.Timestamp(start_date)
    while chunk_start <= pd.Timestamp(end_date):
        chunk_end = min(chunk_start + pd.DateOffset(years=1) - pd.Timedelta(days=1), pd.Timestamp(end_date))
        url = (
            "https://historical-forecast-api.open-meteo.com/v1/forecast?"
            "latitude=-36.731106&longitude=-73.11023&"
            f"start_date={chunk_start:%Y-%m-%d}&end_date={chunk_end:%Y-%m-%d}&"
            f"hourly={OPEN_METEO_HOURLY_QUERY}&timezone=America%2FSantiago&format=json"
        )
        chunks.append(pd.DataFrame(fetch_json_with_retry(url, timeout=120)['hourly']))
        chunk_start = chunk_end + pd.Timedelta(days=1)
    daily = aggregate_weather_daily(pd.concat(chunks, ignore_index=True))
    daily.index = pd.to_datetime(daily.index).strftime('%Y-%m-%d')
    daily = daily.rename_axis('FECHA_DIA').reset_index()
    daily.to_csv(cache_path, index=False)
    version_path.write_text(WEATHER_CACHE_VERSION, encoding="utf-8")
    print(f"Clima diario guardado en caché: {cache_path}")
    return daily


def main():
    print("=== Paso 2: Limpieza y Aumentación de Datos (v2) ===")

    # 1. Rutas de archivos
    base_dir = Path(__file__).resolve().parent.parent
    raw_tweets_path = base_dir / "02_data" / "compiled_scraped_data.csv"
    claves_cbt_path = base_dir / "02_data" / "Clave_CBT.xlsx"
    weather_cache_path = base_dir / "02_data" / "weather_archive_talcahuano.csv"
    output_data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    audit_target_path = base_dir / "05_research" / "data_quality" / "output" / "daily_target_audit.csv"

    if not os.path.exists(raw_tweets_path):
        raise FileNotFoundError(f"No se encontró el archivo de tweets: {raw_tweets_path}")
    if not os.path.exists(claves_cbt_path):
        raise FileNotFoundError(f"No se encontró el archivo de claves: {claves_cbt_path}")

    # 2. Cargar y procesar tweets
    print("Cargando tweets y catálogo de claves...")
    df_raw = pd.read_csv(raw_tweets_path, sep=';', decimal=',')
    codigos = pd.read_excel(claves_cbt_path)

    codigos_clean = codigos.drop_duplicates(subset=['CODIGO_EMERGENCIA']).copy()
    print(f"Claves CBT: {codigos.shape[0]} -> {codigos_clean.shape[0]} (deduplicadas)")

    df = df_raw.copy()
    df['FECHA_DIA'] = df['Fecha'].apply(
        lambda ts: assign_local_date(pd.to_datetime(ts, utc=True), PROJECT_TIMEZONE)
    )
    
    patron = r"\b(10-\d+(?:-\d+)?)\b"
    df['CODIGO_EMERGENCIA'] = df['Texto'].str.extract(patron)

    mask_pastizal = df['Texto'].str.contains(r'PASTIZAL|FORESTAL', case=False, na=False)
    mask_incendio = df['Texto'].str.contains(r'INCENDIO', case=False, na=False)
    df['CODIGO_EMERGENCIA'] = np.where(
        df['CODIGO_EMERGENCIA'].notna(), df['CODIGO_EMERGENCIA'],
        np.where(mask_pastizal, '10-2-3',
        np.where(mask_incendio, '10-0-6', '0-0-0'))
    )

    df_merged = pd.merge(df, codigos_clean, how='left', on='CODIGO_EMERGENCIA')
    print(f"Tweets tras unión limpia: {df_merged.shape[0]} filas")

    # --- Deduplicacion DESACTIVADA, filtro de no-incidentes ACTIVADO ---
    print("Deduplicacion desactivada: cada mensaje incident-like cuenta como evento.")
    _, incident_flags = mark_duplicates(df_merged, 'Fecha', 'Texto')
    df_merged['_IS_DUPLICATE'] = False
    df_merged['_IS_INCIDENT_LIKE'] = df_merged.index.map(
        lambda i: incident_flags.get(i, True)
    )
    n_total = len(df_merged)
    n_incident = int(df_merged['_IS_INCIDENT_LIKE'].sum())
    n_dup = 0
    print(f"Mensajes: {n_total} total -> {n_incident} incident-like -> "
          f"{n_incident - n_dup} únicos ({n_dup} duplicados removidos)")

    # 3. Serie temporal continua
    min_date_str = df_merged['FECHA_DIA'].min()
    max_date_str = df_merged['FECHA_DIA'].max()
    date_range = pd.date_range(start=min_date_str, end=max_date_str, freq='D')
    df_calendar = pd.DataFrame({'FECHA_DIA': date_range.strftime('%Y-%m-%d')})
    print(f"Calendario continuo: {df_calendar.shape[0]} días ({min_date_str} -> {max_date_str})")

    # === CONTEO TOTAL DE EVENTOS POR DÍA (solo incident-like únicos) ===
    df_incidents = df_merged[df_merged['_IS_INCIDENT_LIKE'] & ~df_merged['_IS_DUPLICATE']].copy()
    df_daily_events = df_incidents.groupby('FECHA_DIA').size().reset_index(name='EVENTOS')
    # === CONTEOS POR CATEGORÍA DE EMERGENCIA POR DÍA ===
    # Definir las categorías principales que pueden tener efecto predictivo
    categorias_clave = {
        'INCENDIO ESTRUCTURAL': 'N_INCENDIO_ESTR',
        'INCENDIO PASTIZAL O FORESTAL': 'N_INCENDIO_FOREST',
        'RESCATE VEHICULAR': 'N_RESCATE_VEH',
        'RESCATE DE PERSONAS': 'N_RESCATE_PERS',
        'EMERGENCIAS CLIMATICAS': 'N_EMERGENCIAS_CLIMATICAS',
        'EMANACIÓN DE GASES': 'N_GASES',
    }
    
    # Llenar NaN en CATEGORIA_EMERGENCIA con 'OTROS'
    df_incidents['CATEGORIA_EMERGENCIA'] = df_incidents['CATEGORIA_EMERGENCIA'].fillna('OTROS')
    
    # Crear un pivot de conteos por categoría y día
    df_cat_counts = df_incidents.groupby(['FECHA_DIA', 'CATEGORIA_EMERGENCIA']).size().unstack(fill_value=0)
    
    # Renombrar columnas a las que nos interesan y agrupar el resto en "OTROS"
    cat_columns_present = {}
    for cat_original, cat_nuevo in categorias_clave.items():
        if cat_original in df_cat_counts.columns:
            cat_columns_present[cat_original] = cat_nuevo
    
    df_cat_renamed = df_cat_counts.rename(columns=cat_columns_present)
    # Mantener solo las columnas renombradas
    cols_to_keep = list(cat_columns_present.values())
    cols_others = [c for c in df_cat_renamed.columns if c not in cols_to_keep]
    df_cat_renamed['N_OTROS'] = df_cat_renamed[cols_others].sum(axis=1)
    df_cat_final = df_cat_renamed[cols_to_keep + ['N_OTROS']].reset_index()
    
    print(f"Categorías de emergencia rastreadas: {cols_to_keep + ['N_OTROS']}")

    # Unir eventos totales y por categoría al calendario
    df_daily = pd.merge(df_calendar, df_daily_events, on='FECHA_DIA', how='left')
    df_daily['DAY_STATE'] = 'observed_nonzero'
    df_daily.loc[df_daily['EVENTOS'].isna(), 'DAY_STATE'] = 'no_data'

    # --- Marcar coverage_unknown desde el audit si está disponible ---
    if os.path.exists(audit_target_path):
        print(f"Cargando audit de cobertura desde: {audit_target_path}")
        df_audit = pd.read_csv(audit_target_path)
        coverage_map = dict(zip(
            df_audit['local_date'],
            df_audit['day_state'],
        ))
        audit_states = df_daily['FECHA_DIA'].map(coverage_map)
        n_zero = int((audit_states == 'observed_zero').sum())
        n_unknown = int((audit_states == 'coverage_unknown').sum())
        if n_zero > 0:
            print(f"Preservando {n_zero} dias observed_zero con EVENTOS=0")
        if n_unknown > 0:
            print(f"Marcando {n_unknown} días con coverage_unknown (sin observación confiable)")
        df_daily.loc[audit_states == 'observed_zero', 'DAY_STATE'] = 'observed_zero'
        df_daily.loc[audit_states == 'coverage_unknown', 'DAY_STATE'] = 'coverage_unknown'

    # observed_zero queda como 0; coverage_unknown/no_data quedan fuera del entrenamiento.
    observed_mask = df_daily['DAY_STATE'].isin(['observed_nonzero', 'observed_zero'])
    df_daily.loc[df_daily['DAY_STATE'] == 'observed_zero', 'EVENTOS'] = 0
    df_daily['EVENTOS'] = df_daily['EVENTOS'].where(observed_mask, np.nan)
    
    df_daily = pd.merge(df_daily, df_cat_final, on='FECHA_DIA', how='left')
    for col in cols_to_keep + ['N_OTROS']:
        df_daily[col] = df_daily[col].fillna(0).where(observed_mask, np.nan)

    n_observed = int(observed_mask.sum())
    n_zero = int((df_daily['DAY_STATE'] == 'observed_zero').sum())
    n_unknown = int((df_daily['DAY_STATE'] == 'coverage_unknown').sum())
    print(
        f"Distribucion de eventos diarios "
        f"({n_observed} observados, {n_zero} ceros, {n_unknown} coverage_unknown):"
    )
    print(df_daily.loc[observed_mask, 'EVENTOS'].describe())

    # 4. Datos meteorológicos: Historical Forecast API (pronósticos emitidos,
    # no observados), agregados con la misma función que usa el forecast.
    df_clima = load_historical_weather(min_date_str, max_date_str, weather_cache_path)
    df_clima['FECHA_DIA'] = df_clima['FECHA_DIA'].astype(str)
    df_daily = pd.merge(df_daily, df_clima, on='FECHA_DIA', how='left')
    
    # Interpolar NaN de clima solo hacia adelante (forward-fill) para evitar fuga de información futura
    weather_numeric = [
        c for c in df_daily.select_dtypes(include=[np.number]).columns
        if c not in ['EVENTOS'] + cols_to_keep + ['N_OTROS']
    ]
    df_daily[weather_numeric] = df_daily[weather_numeric].ffill()


    # 5. Feature Engineering EXTENDIDO
    print("Construyendo features extendidas...")
    
    # --- Historial de llamados: los días sin cobertura quedan faltantes,
    # igual que al servir (features.prepare_event_history).
    history = event_history_table(df_daily.set_index('FECHA_DIA')[['EVENTOS', *CATEGORY_SOURCES.values()]])
    df_daily = pd.concat([df_daily, history.set_index(df_daily.index)], axis=1)

    # --- Clima derivado y calendario: definiciones compartidas con el servicio ---
    daily_weather = df_daily.set_index(pd.to_datetime(df_daily['FECHA_DIA']))
    derived = weather_derived_features(daily_weather).set_index(df_daily.index)
    calendar = calendar_features(daily_weather.index)[CALENDAR_COLUMNS].set_index(df_daily.index)
    df_daily = pd.concat([df_daily, derived, calendar], axis=1)

    # Limpiar filas con NaN de los shifts, coverage_unknown o clima faltante
    df_daily = df_daily.sort_values('FECHA_DIA').reset_index(drop=True)
    df_daily = df_daily.dropna(subset=['EVENTOS']).copy()

    # Se exige historia meteorológica completa (30 días) y al menos un conteo
    # observado en los 30 días previos; los lags de días sin cobertura quedan
    # faltantes y el modelo los trata como tales.
    weather_history_cols = list(WEATHER_DERIVED_COLUMNS)
    df_daily = df_daily.dropna(
        subset=weather_history_cols + ['EVENTOS_rolling_mean_30d']
    ).reset_index(drop=True)
    lag_roll_cols = weather_history_cols + EVENT_HISTORY_COLUMNS

    # Weather columns: only forward-fill (never backfill to avoid future leakage)
    weather_cols = [
        c for c in df_daily.select_dtypes(include=[np.number]).columns
        if c not in ['EVENTOS'] and c not in lag_roll_cols
        and not c.startswith('N_') and c not in ['MES', 'DIA_SEMANA', 'ES_FIN_SEMANA', 'ES_FERIADO', 'ES_FERIADO_IRRENUNCIABLE']
        and not c.startswith('MES_') and not c.startswith('DIA_') and not c.startswith('DANO_')
    ]
    df_daily[weather_cols] = df_daily[weather_cols].ffill()
    
    # Conservar los conteos por categoría como objetivos para modelos
    # especializados. Se excluyen explícitamente de los predictores al entrenar.
    drop_cols = ['DAY_STATE']
    df_daily = df_daily.drop(columns=[c for c in drop_cols if c in df_daily.columns])
    
    df_daily.to_csv(output_data_path, index=False, sep=';')
    feature_cols = [c for c in df_daily.columns if c not in ['FECHA_DIA', 'EVENTOS']]
    print(f"\nDataset guardado: {df_daily.shape[0]} filas × {len(feature_cols)} features")
    print(f"Features: {feature_cols}")

if __name__ == "__main__":
    main()
