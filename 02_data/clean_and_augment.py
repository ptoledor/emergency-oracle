import pandas as pd
import numpy as np
import requests
import os
import holidays

def main():
    print("=== Paso 2: Limpieza y Aumentación de Datos (v2) ===")

    # 1. Rutas de archivos
    base_dir = "c:/Users/ptole/Desktop/Pitters-Git/emergency-oracle"
    raw_tweets_path = f"{base_dir}/02_data/compiled_scraped_data.csv"
    claves_cbt_path = f"{base_dir}/02_data/Clave_CBT.xlsx"
    weather_cache_path = f"{base_dir}/02_data/weather_archive_talcahuano.csv"
    output_data_path = f"{base_dir}/02_data/augmented_emergency_data.csv"

    if not os.path.exists(raw_tweets_path):
        raise FileNotFoundError(f"No se encontró el archivo de tweets: {raw_tweets_path}")
    if not os.path.exists(claves_cbt_path):
        raise FileNotFoundError(f"No se encontró el archivo de claves: {claves_cbt_path}")

    # 2. Cargar y procesar tweets
    print("Cargando tweets y catálogo de claves...")
    df_raw = pd.read_csv(raw_tweets_path, sep=';', decimal=',')
    codigos = pd.read_excel(claves_cbt_path)

    codigos_clean = codigos.drop_duplicates(subset=['CODIGO_EMERGENCIA']).copy()
    print(f"Claves CBT: {codigos.shape[0]} → {codigos_clean.shape[0]} (deduplicadas)")

    df = df_raw.copy()
    df['FECHA_DIA'] = df['Fecha'].astype(str).str[:10]
    
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

    # 3. Serie temporal continua
    min_date_str = df_merged['FECHA_DIA'].min()
    max_date_str = df_merged['FECHA_DIA'].max()
    date_range = pd.date_range(start=min_date_str, end=max_date_str, freq='D')
    df_calendar = pd.DataFrame({'FECHA_DIA': date_range.strftime('%Y-%m-%d')})
    print(f"Calendario continuo: {df_calendar.shape[0]} días ({min_date_str} → {max_date_str})")

    # === CONTEO TOTAL DE EVENTOS POR DÍA ===
    df_daily_events = df_merged.groupby('FECHA_DIA').size().reset_index(name='EVENTOS')
    
    # === CONTEOS POR CATEGORÍA DE EMERGENCIA POR DÍA ===
    # Definir las categorías principales que pueden tener efecto predictivo
    categorias_clave = {
        'INCENDIO ESTRUCTURAL': 'N_INCENDIO_ESTR',
        'INCENDIO PASTIZAL O FORESTAL': 'N_INCENDIO_FOREST',
        'RESCATE VEHICULAR': 'N_RESCATE_VEH',
        'RESCATE DE PERSONAS': 'N_RESCATE_PERS',
        'EMANACIÓN DE GASES': 'N_GASES',
    }
    
    # Llenar NaN en CATEGORIA_EMERGENCIA con 'OTROS'
    df_merged['CATEGORIA_EMERGENCIA'] = df_merged['CATEGORIA_EMERGENCIA'].fillna('OTROS')
    
    # Crear un pivot de conteos por categoría y día
    df_cat_counts = df_merged.groupby(['FECHA_DIA', 'CATEGORIA_EMERGENCIA']).size().unstack(fill_value=0)
    
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
    df_daily['EVENTOS'] = df_daily['EVENTOS'].fillna(0).astype(int)
    
    df_daily = pd.merge(df_daily, df_cat_final, on='FECHA_DIA', how='left')
    for col in cols_to_keep + ['N_OTROS']:
        df_daily[col] = df_daily[col].fillna(0).astype(int)

    print(f"Distribución de eventos diarios:")
    print(df_daily['EVENTOS'].describe())

    # 4. Datos meteorológicos
    lat, lon = -36.731106, -73.11023
    
    if os.path.exists(weather_cache_path):
        print("Cargando clima desde caché...")
        df_clima = pd.read_csv(weather_cache_path)
    else:
        print("Descargando clima desde Open-Meteo...")
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={min_date_str}&end_date={max_date_str}&"
               f"daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
               f"relative_humidity_2m_max,relative_humidity_2m_min,relative_humidity_2m_mean,"
               f"wind_speed_10m_max,wind_speed_10m_mean,"
               f"precipitation_sum&"
               f"timezone=America%2FSantiago&format=json")
        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error Open-Meteo: {response.text}")
        data_raw = response.json()
        df_clima = pd.DataFrame(data_raw['daily'])
        df_clima.columns = [
            'FECHA_DIA', 
            'TEMP_MAX', 'TEMP_MIN', 'TEMP_MEDIA',
            'HUM_MAX', 'HUM_MIN', 'HUM_MEDIA',
            'VIENTO_MAX', 'VIENTO_MEDIO',
            'LLUVIA'
        ]
        df_clima.to_csv(weather_cache_path, index=False)
        print("Clima guardado en caché.")

    df_clima['FECHA_DIA'] = df_clima['FECHA_DIA'].astype(str)
    df_daily = pd.merge(df_daily, df_clima, on='FECHA_DIA', how='left')
    
    # Interpolar NaN numéricos de clima
    numeric_cols = df_daily.select_dtypes(include=[np.number]).columns
    df_daily[numeric_cols] = df_daily[numeric_cols].interpolate(method='linear')

    # 5. Feature Engineering EXTENDIDO
    print("Construyendo features extendidas...")
    
    # --- Lags de eventos totales ---
    for lag in [1, 2, 3, 7]:
        df_daily[f'EVENTOS_lag_{lag}'] = df_daily['EVENTOS'].shift(lag)
    
    # --- Lags de categorías clave (solo lag_1) ---
    for col in cols_to_keep:
        df_daily[f'{col}_lag_1'] = df_daily[col].shift(1)
    
    # --- Rolling stats de eventos (ventanas de 3 y 7 días, excluyendo hoy) ---
    eventos_shifted = df_daily['EVENTOS'].shift(1)
    df_daily['EVENTOS_rolling_mean_3d'] = eventos_shifted.rolling(3, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_3d'] = eventos_shifted.rolling(3, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_3d'] = eventos_shifted.rolling(3, min_periods=1).max()
    df_daily['EVENTOS_rolling_mean_7d'] = eventos_shifted.rolling(7, min_periods=1).mean()
    df_daily['EVENTOS_rolling_std_7d'] = eventos_shifted.rolling(7, min_periods=1).std().fillna(0)
    df_daily['EVENTOS_rolling_max_7d'] = eventos_shifted.rolling(7, min_periods=1).max()

    # --- Lluvia: lags y acumulados ---
    df_daily['LLUVIA_lag_1'] = df_daily['LLUVIA'].shift(1)
    df_daily['LLUVIA_lag_2'] = df_daily['LLUVIA'].shift(2)
    df_daily['LLUVIA_lag_3'] = df_daily['LLUVIA'].shift(3)
    df_daily['LLUVIA_accum_3d'] = df_daily['LLUVIA_lag_1'] + df_daily['LLUVIA_lag_2'] + df_daily['LLUVIA_lag_3']
    lluvia_shifted = df_daily['LLUVIA'].shift(1)
    df_daily['LLUVIA_rolling_mean_7d'] = lluvia_shifted.rolling(7, min_periods=1).mean()

    # --- Calendario y feriados ---
    chile_holidays = holidays.Chile(years=range(2022, 2027))
    df_daily['FECHA_DT'] = pd.to_datetime(df_daily['FECHA_DIA'])
    df_daily['MES'] = df_daily['FECHA_DT'].dt.month
    df_daily['DIA_SEMANA'] = df_daily['FECHA_DT'].dt.dayofweek
    df_daily['ES_FIN_SEMANA'] = df_daily['DIA_SEMANA'].isin([5, 6]).astype(int)
    df_daily['ES_FERIADO'] = df_daily['FECHA_DIA'].apply(lambda x: 1 if x in chile_holidays else 0)
    df_daily['DIA_DEL_ANO'] = df_daily['FECHA_DT'].dt.dayofyear

    # --- Codificación cíclica (captura estacionalidad sin discontinuidades) ---
    df_daily['MES_SIN'] = np.sin(2 * np.pi * df_daily['MES'] / 12)
    df_daily['MES_COS'] = np.cos(2 * np.pi * df_daily['MES'] / 12)
    df_daily['DIA_SIN'] = np.sin(2 * np.pi * df_daily['DIA_SEMANA'] / 7)
    df_daily['DIA_COS'] = np.cos(2 * np.pi * df_daily['DIA_SEMANA'] / 7)
    df_daily['DANO_SIN'] = np.sin(2 * np.pi * df_daily['DIA_DEL_ANO'] / 365)
    df_daily['DANO_COS'] = np.cos(2 * np.pi * df_daily['DIA_DEL_ANO'] / 365)

    # Limpiar filas con NaN de los shifts
    df_daily = df_daily.dropna().copy()
    
    # Eliminar columnas auxiliares que no son features
    drop_cols = ['FECHA_DT', 'DIA_DEL_ANO'] + cols_to_keep + ['N_OTROS']
    df_daily = df_daily.drop(columns=drop_cols)
    
    df_daily.to_csv(output_data_path, index=False, sep=';')
    feature_cols = [c for c in df_daily.columns if c not in ['FECHA_DIA', 'EVENTOS']]
    print(f"\nDataset guardado: {df_daily.shape[0]} filas × {len(feature_cols)} features")
    print(f"Features: {feature_cols}")

if __name__ == "__main__":
    main()
