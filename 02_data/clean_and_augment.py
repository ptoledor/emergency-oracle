import pandas as pd
import numpy as np
import requests
import os
import time
import holidays
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import sys
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from dedup import mark_duplicates, assign_local_date, DEFAULT_TIMEZONE

PROJECT_TIMEZONE = DEFAULT_TIMEZONE
FIFTH_COMPANY_UNIT_PATTERN = r"(?<![A-Z0-9])(?:B|RB|RX|MX|BX)[- ]?5(?![A-Z0-9])"
FIFTH_COMPANY_UNIT_CAPTURE_PATTERN = r"(?<![A-Z0-9])(B|RB|RX|MX|BX)[- ]?5(?![A-Z0-9])"


def fifth_company_dispatch_mask(text):
    """Identify dispatches containing a known 5th Company apparatus."""
    without_urls = text.fillna("").str.replace(r"https?://\S+", "", regex=True)
    return without_urls.str.contains(
        FIFTH_COMPANY_UNIT_PATTERN,
        case=False,
        regex=True,
        na=False,
    )


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


def main():
    print("=== Paso 2: Limpieza y Aumentación de Datos (v2) ===")

    # 1. Rutas de archivos
    base_dir = Path(__file__).resolve().parent.parent
    raw_tweets_path = base_dir / "02_data" / "compiled_scraped_data.csv"
    claves_cbt_path = base_dir / "02_data" / "Clave_CBT.xlsx"
    weather_cache_path = base_dir / "02_data" / "weather_archive_talcahuano.csv"
    output_data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    audit_target_path = base_dir / "05_research" / "data_quality" / "output" / "daily_target_audit.csv"
    fifth_company_audit_path = base_dir / "02_data" / "fifth_company_dispatch_audit.csv"

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
    df_incidents['_IS_5TA_CIA'] = fifth_company_dispatch_mask(df_incidents['Texto'])
    fifth_audit = df_incidents.loc[
        df_incidents['_IS_5TA_CIA'], ['Fecha', 'FECHA_DIA', 'Texto']
    ].copy()
    fifth_text_without_urls = fifth_audit['Texto'].fillna('').str.replace(
        r"https?://\S+", "", regex=True
    )
    fifth_audit['UNIDADES_5TA'] = fifth_text_without_urls.apply(
        lambda value: ','.join(sorted({
            f"{match.upper()}-5"
            for match in pd.Series([value.upper()]).str.findall(
                FIFTH_COMPANY_UNIT_CAPTURE_PATTERN,
                flags=0,
            ).iloc[0]
        }))
    )
    fifth_audit.to_csv(fifth_company_audit_path, sep=';', index=False)
    df_daily_fifth = (
        df_incidents.groupby('FECHA_DIA')['_IS_5TA_CIA']
        .sum()
        .astype(float)
        .reset_index(name='N_5TA_CIA')
    )
    print(
        "Despachos 5ta Cia: "
        f"{int(df_incidents['_IS_5TA_CIA'].sum())} mensajes incident-like; "
        "unidades B-5, RB-5, RX-5, MX-5 y alias preventivo BX-5"
    )
    
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
    df_daily = pd.merge(df_daily, df_daily_fifth, on='FECHA_DIA', how='left')
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
    df_daily['N_5TA_CIA'] = df_daily['N_5TA_CIA'].fillna(0).where(observed_mask, np.nan)
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

    # 4. Datos meteorológicos (Historical Forecast API — pronósticos emitidos, no observados)
    lat, lon = -36.731106, -73.11023
    WEATHER_CACHE_VERSION = "forecast_v1"
    cache_version_path = weather_cache_path.parent / ".weather_cache_version"

    rebuild_cache = True
    if os.path.exists(weather_cache_path):
        try:
            df_cached = pd.read_csv(weather_cache_path)
            cached_version = ""
            if os.path.exists(cache_version_path):
                cached_version = Path(cache_version_path).read_text(encoding="utf-8").strip()
            cache_has_range = (
                'FECHA_DIA' in df_cached.columns
                and str(df_cached['FECHA_DIA'].min()) <= min_date_str
                and str(df_cached['FECHA_DIA'].max()) >= max_date_str
            )
            if 'VIENTO_SKEW' in df_cached.columns and 'HUM_SKEW' in df_cached.columns and cache_has_range:
                if cached_version == WEATHER_CACHE_VERSION:
                    print(f"Cargando clima desde caché (versión={cached_version})...")
                    df_clima = df_cached
                    rebuild_cache = False
                else:
                    print(f"Cache versión '{cached_version}' obsoleta. Re-descargando...")
        except Exception:
            pass

    if rebuild_cache:
        print("Descargando clima horario (Historical Forecast API) desde Open-Meteo...")
        url = (f"https://historical-forecast-api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={min_date_str}&end_date={max_date_str}&"
               f"hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&"
               f"timezone=America%2FSantiago&format=json")
        data_raw = fetch_json_with_retry(url, timeout=30, retries=3)
        df_hourly = pd.DataFrame(data_raw['hourly'])
        df_hourly['time'] = pd.to_datetime(df_hourly['time'])
        df_hourly['FECHA_DIA'] = df_hourly['time'].dt.strftime('%Y-%m-%d')
        
        # Fórmulas de asimetría y curtosis
        def get_skew(series):
            vals = series.values
            mean = np.mean(vals)
            std = np.std(vals)
            std_safe = 0.1 if std == 0 else std
            return np.mean((vals - mean)**3) / (std_safe**3)

        def get_kurt(series):
            vals = series.values
            mean = np.mean(vals)
            std = np.std(vals)
            std_safe = 0.1 if std == 0 else std
            return np.mean((vals - mean)**4) / (std_safe**4) - 3

        print("Agregando datos horarios a nivel diario...")
        df_clima = df_hourly.groupby('FECHA_DIA').agg(
            TEMP_MAX=('temperature_2m', 'max'),
            TEMP_MIN=('temperature_2m', 'min'),
            TEMP_MEDIA=('temperature_2m', 'mean'),
            TEMP_SKEW=('temperature_2m', get_skew),
            TEMP_KURT=('temperature_2m', get_kurt),
            
            HUM_MAX=('relative_humidity_2m', 'max'),
            HUM_MIN=('relative_humidity_2m', 'min'),
            HUM_MEDIA=('relative_humidity_2m', 'mean'),
            HUM_SKEW=('relative_humidity_2m', get_skew),
            HUM_KURT=('relative_humidity_2m', get_kurt),
            
            VIENTO_MAX=('wind_speed_10m', 'max'),
            VIENTO_MEDIO=('wind_speed_10m', 'mean'),
            VIENTO_SKEW=('wind_speed_10m', get_skew),
            VIENTO_KURT=('wind_speed_10m', get_kurt),
            
            LLUVIA=('precipitation', 'sum')
        ).reset_index()
        
        df_clima.to_csv(weather_cache_path, index=False)
        Path(cache_version_path).write_text(WEATHER_CACHE_VERSION, encoding="utf-8")
        print("Clima horario agregado y guardado en caché (Historical Forecast API).")

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
    
    # Para lag features usamos una versión forward-fill de EVENTOS y categorías
    # (conservando NaN en el target para días coverage_unknown).
    eventos_lag_source = df_daily['EVENTOS'].fillna(0)
    cat_lag_source = {
        col: df_daily[col].fillna(0) for col in cols_to_keep + ['N_OTROS']
    }
    
    # --- Lags de eventos totales ---
    for lag in [1, 2, 3, 7]:
        df_daily[f'EVENTOS_lag_{lag}'] = eventos_lag_source.shift(lag)
    
    # --- Lags de categorías clave (solo lag_1) ---
    for col in cols_to_keep:
        df_daily[f'{col}_lag_1'] = cat_lag_source[col].shift(1)
    
    # --- Rolling stats de eventos (ventanas de 3 y 7 días, excluyendo hoy) ---
    eventos_shifted = eventos_lag_source.shift(1)
    df_daily['EVENTOS_rolling_mean_3d'] = eventos_shifted.rolling(3, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_3d'] = eventos_shifted.rolling(3, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_3d'] = eventos_shifted.rolling(3, min_periods=1).max()
    df_daily['EVENTOS_rolling_mean_7d'] = eventos_shifted.rolling(7, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_7d'] = eventos_shifted.rolling(7, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_7d'] = eventos_shifted.rolling(7, min_periods=1).max()
    df_daily['EVENTOS_rolling_mean_14d'] = eventos_shifted.rolling(14, min_periods=1).mean()
    df_daily['EVENTOS_rolling_mean_30d'] = eventos_shifted.rolling(30, min_periods=1).mean()

    # --- Lluvia: memoria hídrica multiescala, siempre excluyendo hoy ---
    for lag in [1, 2, 3, 5, 7, 10, 14]:
        df_daily[f'LLUVIA_LAG_{lag}D'] = df_daily['LLUVIA'].shift(lag)

    lluvia_shifted = df_daily['LLUVIA'].shift(1)
    for window in [3, 7, 14, 30]:
        rolling = lluvia_shifted.rolling(window, min_periods=window)
        df_daily[f'LLUVIA_PROMEDIO_{window}D_PREV'] = rolling.mean()
        df_daily[f'LLUVIA_TOTAL_{window}D_PREV'] = rolling.sum()
        df_daily[f'LLUVIA_DESV_{window}D_PREV'] = rolling.std().fillna(0)
        df_daily[f'LLUVIA_MAX_{window}D_PREV'] = rolling.max()
        df_daily[f'DIAS_SECOS_{window}D_PREV'] = (
            lluvia_shifted.le(0.1).rolling(window, min_periods=window).sum()
        )

    # --- Días desde la última lluvia ---
    dias_desde_lluvia = []
    count = 0
    for rain in lluvia_shifted:
        if pd.isna(rain):
            dias_desde_lluvia.append(np.nan)
        elif rain > 0.1:
            count = 0
            dias_desde_lluvia.append(count)
        else:
            count += 1
            dias_desde_lluvia.append(count)
    df_daily['DIAS_DESDE_ULTIMA_LLUVIA'] = dias_desde_lluvia

    # --- Lags de clima ---
    df_daily['VIENTO_MEDIO_lag_1'] = df_daily['VIENTO_MEDIO'].shift(1)
    df_daily['HUM_MEDIA_lag_1'] = df_daily['HUM_MEDIA'].shift(1)

    # --- Interacciones climaticas simples ---
    df_daily['TEMP_HUM_INDEX'] = df_daily['TEMP_MEDIA'] * df_daily['HUM_MEDIA'] / 100
    df_daily['VIENTO_LLUVIA_INDEX'] = df_daily['VIENTO_MEDIO'] * df_daily['LLUVIA']
    df_daily['STORM_COMPOUND_INDEX'] = df_daily['VIENTO_MAX'] * (1 + df_daily['LLUVIA'])
    df_daily['FIRE_DRY_INDEX_7D'] = (
        df_daily['TEMP_MAX'] * df_daily['DIAS_SECOS_7D_PREV']
        / (1 + df_daily['LLUVIA_TOTAL_7D_PREV'])
    )

    # --- Índice de Déficit de Presión de Vapor (VPD) ---
    es_media = 0.6108 * np.exp((17.27 * df_daily['TEMP_MEDIA']) / (df_daily['TEMP_MEDIA'] + 237.3))
    df_daily['VPD'] = es_media * (1 - df_daily['HUM_MEDIA'] / 100)
    
    es_max = 0.6108 * np.exp((17.27 * df_daily['TEMP_MAX']) / (df_daily['TEMP_MAX'] + 237.3))
    df_daily['VPD_MAX'] = es_max * (1 - df_daily['HUM_MIN'] / 100)

    # --- Calendario y feriados ---
    chile_holidays = holidays.Chile(years=range(2022, 2027))
    df_daily['FECHA_DT'] = pd.to_datetime(df_daily['FECHA_DIA'])
    df_daily['MES'] = df_daily['FECHA_DT'].dt.month
    df_daily['DIA_SEMANA'] = df_daily['FECHA_DT'].dt.dayofweek
    df_daily['ES_FIN_SEMANA'] = df_daily['DIA_SEMANA'].isin([5, 6]).astype(int)
    
    # Identificar feriados irrenunciables fijos en Chile y elecciones
    feriados_irrenunciables = {(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)}
    
    def determinar_irrenunciable(row):
        fecha_str = row['FECHA_DIA']
        dt = row['FECHA_DT']
        if (dt.month, dt.day) in feriados_irrenunciables:
            return 1
        name = chile_holidays.get(fecha_str)
        if name and ("elecciones" in name.lower() or "plebiscito" in name.lower()):
            return 1
        return 0

    df_daily['ES_FERIADO'] = df_daily['FECHA_DIA'].apply(lambda x: 1 if x in chile_holidays else 0)
    df_daily['ES_FERIADO_IRRENUNCIABLE'] = df_daily.apply(determinar_irrenunciable, axis=1)
    
    # Pre-feriado: si el día de mañana es feriado
    df_daily['ES_PRE_FERIADO'] = df_daily['FECHA_DIA'].apply(
        lambda x: 1 if (datetime.strptime(x, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d') in chile_holidays else 0
    )
    
    df_daily['DIA_DEL_ANO'] = df_daily['FECHA_DT'].dt.dayofyear

    # --- Codificación cíclica (captura estacionalidad sin discontinuidades) ---
    df_daily['MES_SIN'] = np.sin(2 * np.pi * df_daily['MES'] / 12)
    df_daily['MES_COS'] = np.cos(2 * np.pi * df_daily['MES'] / 12)
    df_daily['DIA_SIN'] = np.sin(2 * np.pi * df_daily['DIA_SEMANA'] / 7)
    df_daily['DIA_COS'] = np.cos(2 * np.pi * df_daily['DIA_SEMANA'] / 7)
    df_daily['DANO_SIN'] = np.sin(2 * np.pi * df_daily['DIA_DEL_ANO'] / 365)
    df_daily['DANO_COS'] = np.cos(2 * np.pi * df_daily['DIA_DEL_ANO'] / 365)

    # Limpiar filas con NaN de los shifts, coverage_unknown o clima faltante
    df_daily = df_daily.sort_values('FECHA_DIA').reset_index(drop=True)
    df_daily = df_daily.dropna(subset=['EVENTOS']).copy()

    # Drop rows where lag/rolling features are still NaN (first ~30 rows).
    # These have incomplete history and would otherwise be backfilled with future data.
    lag_roll_cols = [
        c for c in df_daily.columns
        if c.startswith('EVENTOS_lag_') or c.startswith('EVENTOS_rolling_')
        or c.startswith('LLUVIA_LAG_') or c.startswith('LLUVIA_PROMEDIO_')
        or c.startswith('LLUVIA_TOTAL_') or c.startswith('LLUVIA_DESV_')
        or c.startswith('LLUVIA_MAX_') or c.startswith('DIAS_SECOS_')
        or c.endswith('_lag_1') or c == 'DIAS_DESDE_ULTIMA_LLUVIA'
    ]
    df_daily = df_daily.dropna(subset=lag_roll_cols).reset_index(drop=True)

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
    drop_cols = ['FECHA_DT', 'DIA_DEL_ANO', 'DAY_STATE']
    df_daily = df_daily.drop(columns=[c for c in drop_cols if c in df_daily.columns])
    
    df_daily.to_csv(output_data_path, index=False, sep=';')
    feature_cols = [c for c in df_daily.columns if c not in ['FECHA_DIA', 'EVENTOS']]
    print(f"\nDataset guardado: {df_daily.shape[0]} filas × {len(feature_cols)} features")
    print(f"Features: {feature_cols}")

if __name__ == "__main__":
    main()
