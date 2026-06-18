import pandas as pd
import numpy as np
import requests
import os
import holidays
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import sys
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
from dedup import mark_duplicates, assign_local_date, DEFAULT_TIMEZONE

PROJECT_TIMEZONE = DEFAULT_TIMEZONE


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

    # --- Deduplicación DESACTIVADA: cada tweet = un evento ---
    print("Deduplicación desactivada: cada tweet cuenta como evento.")
    df_merged['_IS_DUPLICATE'] = False
    df_merged['_IS_INCIDENT_LIKE'] = True
    n_total = len(df_merged)
    n_incident = n_total
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
    df_daily['DAY_STATE'] = 'observed'
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
        n_unknown = int((audit_states == 'coverage_unknown').sum())
        if n_unknown > 0:
            print(f"Marcando {n_unknown} días con coverage_unknown (sin observación confiable)")
        df_daily.loc[audit_states == 'coverage_unknown', 'DAY_STATE'] = 'coverage_unknown'

    # coverage_unknown y no_data -> NaN (no se rellenan con 0)
    df_daily['EVENTOS'] = df_daily['EVENTOS'].where(df_daily['DAY_STATE'] == 'observed', np.nan)
    
    df_daily = pd.merge(df_daily, df_cat_final, on='FECHA_DIA', how='left')
    for col in cols_to_keep + ['N_OTROS']:
        df_daily[col] = df_daily[col].where(df_daily['DAY_STATE'] == 'observed', np.nan)

    n_observed = int((df_daily['DAY_STATE'] == 'observed').sum())
    n_unknown = int((df_daily['DAY_STATE'] == 'coverage_unknown').sum())
    print(f"Distribución de eventos diarios ({n_observed} observados, {n_unknown} coverage_unknown):")
    print(df_daily.loc[df_daily['DAY_STATE'] == 'observed', 'EVENTOS'].describe())

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
            if 'VIENTO_SKEW' in df_cached.columns and 'HUM_SKEW' in df_cached.columns:
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
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Error Open-Meteo: {response.text}")
        data_raw = response.json()
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
    eventos_ff = df_daily['EVENTOS'].ffill()
    cat_ff = {col: df_daily[col].ffill() for col in cols_to_keep + ['N_OTROS']}
    
    # --- Lags de eventos totales ---
    for lag in [1, 2, 3, 7]:
        df_daily[f'EVENTOS_lag_{lag}'] = eventos_ff.shift(lag)
    
    # --- Lags de categorías clave (solo lag_1) ---
    for col in cols_to_keep:
        df_daily[f'{col}_lag_1'] = cat_ff[col].shift(1)
    
    # --- Rolling stats de eventos (ventanas de 3 y 7 días, excluyendo hoy) ---
    eventos_shifted = eventos_ff.shift(1)
    df_daily['EVENTOS_rolling_mean_3d'] = eventos_shifted.rolling(3, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_3d'] = eventos_shifted.rolling(3, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_3d'] = eventos_shifted.rolling(3, min_periods=1).max()
    df_daily['EVENTOS_rolling_mean_7d'] = eventos_shifted.rolling(7, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_7d'] = eventos_shifted.rolling(7, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_7d'] = eventos_shifted.rolling(7, min_periods=1).max()

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

    # --- Lags de clima ---
    df_daily['VIENTO_MEDIO_lag_1'] = df_daily['VIENTO_MEDIO'].shift(1)
    df_daily['HUM_MEDIA_lag_1'] = df_daily['HUM_MEDIA'].shift(1)

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
    # Rellenar cualquier NaN restante en features de clima con forward-fill + back-fill del primer valor
    numeric_cols = df_daily.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c not in ['EVENTOS']]
    df_daily[numeric_cols] = df_daily[numeric_cols].ffill().bfill()
    
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
