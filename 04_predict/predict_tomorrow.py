import pandas as pd
import numpy as np
import requests
import os
import pickle
import datetime
import holidays
import argparse
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROJECT_TIMEZONE = ZoneInfo("America/Santiago")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "02_data") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "02_data"))

from dedup import mark_duplicates, assign_local_date, DEFAULT_TIMEZONE


def project_today():
    return datetime.datetime.now(PROJECT_TIMEZONE).date()

def get_events_and_categories_for_dates(csv_path, codes_path, dates):
    """
    Carga los tweets locales, deduplica incident-like, excluye operacionales,
    y retorna conteos consistentes con clean_and_augment.py.
    """
    df = pd.read_csv(csv_path, sep=';', decimal=',')
    df['FECHA_DIA'] = df['Fecha'].apply(
        lambda ts: assign_local_date(pd.to_datetime(ts, utc=True), DEFAULT_TIMEZONE)
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

    codigos = pd.read_excel(codes_path)
    codigos_clean = codigos.drop_duplicates(subset=['CODIGO_EMERGENCIA']).copy()
    df_merged = pd.merge(df, codigos_clean, how='left', on='CODIGO_EMERGENCIA')

    # Deduplicar y filtrar solo incident-like únicos (consistente con clean_and_augment.py)
    dup_flags, incident_flags = mark_duplicates(df_merged, 'Fecha', 'Texto')
    df_merged['_IS_DUPLICATE'] = df_merged.index.map(lambda i: dup_flags.get(i, False))
    df_merged['_IS_INCIDENT_LIKE'] = df_merged.index.map(lambda i: incident_flags.get(i, False))
    df_incidents = df_merged[df_merged['_IS_INCIDENT_LIKE'] & ~df_merged['_IS_DUPLICATE']].copy()

    daily_counts = df_incidents.groupby('FECHA_DIA').size().to_dict()
    event_counts = [daily_counts.get(d, 0) for d in dates]

    lag_1_date = dates[-1]
    df_lag1 = df_incidents[df_incidents['FECHA_DIA'] == lag_1_date]

    categorias_clave = {
        'INCENDIO ESTRUCTURAL': 'N_INCENDIO_ESTR_lag_1',
        'INCENDIO PASTIZAL O FORESTAL': 'N_INCENDIO_FOREST_lag_1',
        'RESCATE VEHICULAR': 'N_RESCATE_VEH_lag_1',
        'RESCATE DE PERSONAS': 'N_RESCATE_PERS_lag_1',
        'EMANACIÓN DE GASES': 'N_GASES_lag_1',
    }

    category_counts = {v: 0 for v in categorias_clave.values()}
    if not df_lag1.empty:
        df_lag1['CATEGORIA_EMERGENCIA'] = df_lag1['CATEGORIA_EMERGENCIA'].fillna('OTROS')
        df_lag1_cats = df_lag1.groupby('CATEGORIA_EMERGENCIA').size().to_dict()
        for cat_orig, cat_new in categorias_clave.items():
            category_counts[cat_new] = df_lag1_cats.get(cat_orig, 0)

    return event_counts, category_counts


def main():
    # Parámetros y directorios base
    base_dir = PROJECT_ROOT
    models_dir = base_dir / "03_model" / "saved_models"
    raw_tweets_path = base_dir / "02_data" / "compiled_scraped_data.csv"
    claves_cbt_path = base_dir / "02_data" / "Clave_CBT.xlsx"
    lat, lon = -36.731106, -73.11023  # Coordenadas de Talcahuano

    # Por defecto, predice el día siguiente al dataset
    parser = argparse.ArgumentParser(description="Predictor de emergencias para Bomberos de Talcahuano")
    parser.add_argument('--date', type=str, help="Fecha a predecir (YYYY-MM-DD). Por defecto predice el día siguiente al dataset.")
    parser.add_argument('--real-tomorrow', action='store_true', help="Fuerza a predecir el día de mañana real.")
    parser.add_argument('--v3', action='store_true', help="Alias compatible del Modelo Climático Aumentado.")
    parser.add_argument('--inertia', action='store_true', help="Usa el spin-off con inercia de actividad.")
    args = parser.parse_args()

    # Cargar modelos según selección de versión
    prefix = "_agnostic_augmented" if args.inertia else "_climatic_augmented"
    with open(f"{models_dir}/regressor{prefix}.pkl", "rb") as f:
        reg_model = pickle.load(f)
    with open(f"{models_dir}/classifier{prefix}.pkl", "rb") as f:
        clf_model = pickle.load(f)
    with open(f"{models_dir}/metadata{prefix}.pkl", "rb") as f:
        metadata = pickle.load(f)

    # Detectar última fecha disponible en el dataset
    df_raw = pd.read_csv(raw_tweets_path, sep=';', decimal=',')
    max_date_in_dataset = df_raw['Fecha'].astype(str).str[:10].max()
    max_dt = datetime.datetime.strptime(max_date_in_dataset, '%Y-%m-%d').date()

    # Definir fecha objetivo y lags
    if args.real_tomorrow:
        target_date = project_today() + datetime.timedelta(days=1)
        mode = "MAÑANA REAL"
    elif args.date:
        target_date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
        mode = "FECHA ESPECÍFICA"
    else:
        target_date = max_dt + datetime.timedelta(days=1)
        mode = "PROGRESIÓN DEL DATASET"

    print(f"=== PASO 4: Predicción de Emergencias (Modo: {mode}) ===")
    print(f"Modelo en uso: {'Spin-off con Inercia de Actividad' if args.inertia else 'Modelo Climático Aumentado (Principal)'}")
    print(f"Último dato en dataset local: {max_date_in_dataset}")
    print(f"Fecha a predecir: {target_date}")

    # Calcular las fechas de los lags (necesitamos hasta lag_7)
    lag_dates = [target_date - datetime.timedelta(days=i) for i in range(7, 0, -1)]
    lag_dates_str = [d.strftime('%Y-%m-%d') for d in lag_dates]
    
    print(f"Cálculo de lags usando fechas (desde lag_7 a lag_1): {lag_dates_str}")

    # Obtener conteos de eventos y categorías
    lag_counts, category_lags = get_events_and_categories_for_dates(raw_tweets_path, claves_cbt_path, lag_dates_str)
    
    # Imputar la media de entrenamiento (5.46) para las fechas de lags que son posteriores a la base de datos
    imputed_lag_counts = [
        val if date <= max_date_in_dataset else 5.46 
        for val, date in zip(lag_counts, lag_dates_str)
    ]
    
    eventos_lag_7 = imputed_lag_counts[0]
    eventos_lag_3 = imputed_lag_counts[4]
    eventos_lag_2 = imputed_lag_counts[5]
    eventos_lag_1 = imputed_lag_counts[6]
    
    # Calcular estadísticas móviles de eventos (excluyendo el día de predicción)
    eventos_rolling_mean_3d = np.mean(imputed_lag_counts[4:])  # lag_3, lag_2, lag_1
    eventos_rolling_std_3d = np.std(imputed_lag_counts[4:])
    eventos_rolling_max_3d = np.max(imputed_lag_counts[4:])
    
    eventos_rolling_mean_7d = np.mean(imputed_lag_counts)      # lag_7 a lag_1
    eventos_rolling_std_7d = np.std(imputed_lag_counts)
    eventos_rolling_max_7d = np.max(imputed_lag_counts)

    print(f"Conteo de eventos en lags principales:")
    print(f"  - Hace 7 días ({lag_dates_str[0]}): {eventos_lag_7:.2f} (imputado={lag_dates_str[0] > max_date_in_dataset})")
    print(f"  - Hace 3 días ({lag_dates_str[4]}): {eventos_lag_3:.2f} (imputado={lag_dates_str[4] > max_date_in_dataset})")
    print(f"  - Hace 2 días ({lag_dates_str[5]}): {eventos_lag_2:.2f} (imputado={lag_dates_str[5] > max_date_in_dataset})")
    print(f"  - Ayer/Hoy ({lag_dates_str[6]}): {eventos_lag_1:.2f} (imputado={lag_dates_str[6] > max_date_in_dataset})")
    print(f"  - Media móvil 3 días: {eventos_rolling_mean_3d:.2f} (std={eventos_rolling_std_3d:.2f})")
    print(f"  - Media móvil 7 días: {eventos_rolling_mean_7d:.2f} (std={eventos_rolling_std_7d:.2f})")
    print(f"Conteo de categorías lag_1 (Ayer/Hoy): {category_lags}")

    # Obtener clima para la fecha objetivo y los lags usando datos horarios
    target_date_str = target_date.strftime('%Y-%m-%d')
    today_date = project_today()

    # Fórmulas de asimetría y curtosis
    def get_skew(vals):
        mean = np.mean(vals)
        std = np.std(vals)
        std_safe = 0.1 if std == 0 else std
        return np.mean((vals - mean)**3) / (std_safe**3)

    def get_kurt(vals):
        mean = np.mean(vals)
        std = np.std(vals)
        std_safe = 0.1 if std == 0 else std
        return np.mean((vals - mean)**4) / (std_safe**4) - 3

    clima_data = {}
    if target_date > today_date:
        print("Obteniendo pronóstico del clima en tiempo real desde Open-Meteo...")
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&"
               f"hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&"
               f"timezone=America%2FSantiago&past_days=30&forecast_days=10")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Error al descargar pronóstico: {response.text}")
        data_raw = response.json()['hourly']
    else:
        print("Obteniendo clima histórico desde Open-Meteo...")
        # Consultamos 30 días previos para memoria hídrica multiescala.
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={(target_date - datetime.timedelta(days=30)).strftime('%Y-%m-%d')}&"
               f"end_date={target_date_str}&"
               f"hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&"
               f"timezone=America%2FSantiago&format=json")
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            raise RuntimeError(f"Error al descargar clima histórico: {response.text}")
        data_raw = response.json()['hourly']
        
    df_hourly = pd.DataFrame(data_raw)
    df_hourly['time'] = pd.to_datetime(df_hourly['time'])
    df_hourly['FECHA_DIA'] = df_hourly['time'].dt.strftime('%Y-%m-%d')
    
    # Agrupar y agregar
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
    
    df_clima = df_clima.set_index('FECHA_DIA')
    
    # Extraer variables del día objetivo
    target_row = df_clima.loc[target_date_str]
    clima_data = {
        'TEMP_MAX': float(target_row['TEMP_MAX']),
        'TEMP_MIN': float(target_row['TEMP_MIN']),
        'TEMP_MEDIA': float(target_row['TEMP_MEDIA']),
        'TEMP_SKEW': float(target_row['TEMP_SKEW']),
        'TEMP_KURT': float(target_row['TEMP_KURT']),
        
        'HUM_MAX': float(target_row['HUM_MAX']),
        'HUM_MIN': float(target_row['HUM_MIN']),
        'HUM_MEDIA': float(target_row['HUM_MEDIA']),
        'HUM_SKEW': float(target_row['HUM_SKEW']),
        'HUM_KURT': float(target_row['HUM_KURT']),
        
        'VIENTO_MAX': float(target_row['VIENTO_MAX']),
        'VIENTO_MEDIO': float(target_row['VIENTO_MEDIO']),
        'VIENTO_SKEW': float(target_row['VIENTO_SKEW']),
        'VIENTO_KURT': float(target_row['VIENTO_KURT']),
        
        'LLUVIA': float(target_row['LLUVIA'])
    }
    
    # Lags (weather lags)
    lag_1_str = (target_date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    lag_2_str = (target_date - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    lag_3_str = (target_date - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
    
    clima_data['VIENTO_MEDIO_lag_1'] = float(df_clima.loc[lag_1_str]['VIENTO_MEDIO'])
    clima_data['HUM_MEDIA_lag_1'] = float(df_clima.loc[lag_1_str]['HUM_MEDIA'])

    rain_history = {
        i: float(df_clima.loc[(target_date - datetime.timedelta(days=i)).strftime('%Y-%m-%d')]['LLUVIA'])
        for i in range(1, 31)
    }
    for lag in [1, 2, 3, 5, 7, 10, 14]:
        clima_data[f'LLUVIA_LAG_{lag}D'] = rain_history[lag]
    for window in [3, 7, 14, 30]:
        rain_window = np.array([rain_history[i] for i in range(1, window + 1)])
        clima_data[f'LLUVIA_PROMEDIO_{window}D_PREV'] = float(np.mean(rain_window))
        clima_data[f'LLUVIA_TOTAL_{window}D_PREV'] = float(np.sum(rain_window))
        clima_data[f'LLUVIA_DESV_{window}D_PREV'] = float(np.std(rain_window, ddof=1))
        clima_data[f'LLUVIA_MAX_{window}D_PREV'] = float(np.max(rain_window))
        clima_data[f'DIAS_SECOS_{window}D_PREV'] = float(np.sum(rain_window <= 0.1))


    print(f"Condiciones climáticas para la predicción:")
    print(f"  - Temperatura (Mín/Med/Máx): {clima_data['TEMP_MIN']}°C / {clima_data['TEMP_MEDIA']}°C / {clima_data['TEMP_MAX']}°C")
    print(f"  - Humedad Media: {clima_data['HUM_MEDIA']}%")
    print(f"  - Viento (Medio/Máx): {clima_data['VIENTO_MEDIO']} km/h / {clima_data['VIENTO_MAX']} km/h")
    print(f"  - Precipitación estimada: {clima_data['LLUVIA']} mm")
    print(f"  - Lluvia acumulada últimos 3 días: {clima_data['LLUVIA_TOTAL_3D_PREV']:.1f} mm")
    print(f"  - Promedio lluvia últimos 7 días: {clima_data['LLUVIA_PROMEDIO_7D_PREV']:.1f} mm")

    # Características de calendario y feriado
    chile_holidays = holidays.Chile(years=[target_date.year])
    mes = target_date.month
    dia_semana = target_date.weekday()
    es_fin_semana = 1 if dia_semana in [5, 6] else 0
    es_feriado = 1 if target_date_str in chile_holidays else 0
    
    # Identificar feriados irrenunciables fijos en Chile y elecciones
    feriados_irrenunciables = {(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)}
    holiday_name = chile_holidays.get(target_date_str)
    es_feriado_irrenunciable = 0
    if (mes, target_date.day) in feriados_irrenunciables:
        es_feriado_irrenunciable = 1
    elif holiday_name and ("elecciones" in holiday_name.lower() or "plebiscito" in holiday_name.lower()):
        es_feriado_irrenunciable = 1
        
    dia_del_ano = target_date.timetuple().tm_yday

    # Encodings cíclicos
    mes_sin = np.sin(2 * np.pi * mes / 12)
    mes_cos = np.cos(2 * np.pi * mes / 12)
    dia_sin = np.sin(2 * np.pi * dia_semana / 7)
    dia_cos = np.cos(2 * np.pi * dia_semana / 7)
    dano_sin = np.sin(2 * np.pi * dia_del_ano / 365)
    dano_cos = np.cos(2 * np.pi * dia_del_ano / 365)

    # Crear el vector de características
    features = {
        'TEMP_MAX': clima_data['TEMP_MAX'],
        'TEMP_MIN': clima_data['TEMP_MIN'],
        'TEMP_MEDIA': clima_data['TEMP_MEDIA'],
        'TEMP_SKEW': clima_data['TEMP_SKEW'],
        'TEMP_KURT': clima_data['TEMP_KURT'],
        'HUM_MAX': clima_data['HUM_MAX'],
        'HUM_MIN': clima_data['HUM_MIN'],
        'HUM_MEDIA': clima_data['HUM_MEDIA'],
        'HUM_SKEW': clima_data['HUM_SKEW'],
        'HUM_KURT': clima_data['HUM_KURT'],
        'VIENTO_MAX': clima_data['VIENTO_MAX'],
        'VIENTO_MEDIO': clima_data['VIENTO_MEDIO'],
        'VIENTO_SKEW': clima_data['VIENTO_SKEW'],
        'VIENTO_KURT': clima_data['VIENTO_KURT'],
        'LLUVIA': clima_data['LLUVIA'],
        'EVENTOS_lag_1': eventos_lag_1,
        'EVENTOS_lag_2': eventos_lag_2,
        'EVENTOS_lag_3': eventos_lag_3,
        'EVENTOS_lag_7': eventos_lag_7,
        'N_INCENDIO_ESTR_lag_1': category_lags['N_INCENDIO_ESTR_lag_1'],
        'N_INCENDIO_FOREST_lag_1': category_lags['N_INCENDIO_FOREST_lag_1'],
        'N_RESCATE_VEH_lag_1': category_lags['N_RESCATE_VEH_lag_1'],
        'N_RESCATE_PERS_lag_1': category_lags['N_RESCATE_PERS_lag_1'],
        'N_GASES_lag_1': category_lags['N_GASES_lag_1'],
        'EVENTOS_rolling_mean_3d': eventos_rolling_mean_3d,
        'EVENTOS_rolling_std_3d': eventos_rolling_std_3d,
        'EVENTOS_rolling_max_3d': eventos_rolling_max_3d,
        'EVENTOS_rolling_mean_7d': eventos_rolling_mean_7d,
        'EVENTOS_rolling_std_7d': eventos_rolling_std_7d,
        'EVENTOS_rolling_max_7d': eventos_rolling_max_7d,
        'VIENTO_MEDIO_lag_1': clima_data['VIENTO_MEDIO_lag_1'],
        'HUM_MEDIA_lag_1': clima_data['HUM_MEDIA_lag_1'],
        'MES': mes,
        'DIA_SEMANA': dia_semana,
        'ES_FIN_SEMANA': es_fin_semana,
        'ES_FERIADO': es_feriado,
        'ES_FERIADO_IRRENUNCIABLE': es_feriado_irrenunciable,
        'MES_SIN': mes_sin,
        'MES_COS': mes_cos,
        'DIA_SIN': dia_sin,
        'DIA_COS': dia_cos,
        'DANO_SIN': dano_sin,
        'DANO_COS': dano_cos
    }
    weekday_columns = [
        'DIA_LUNES', 'DIA_MARTES', 'DIA_MIERCOLES', 'DIA_JUEVES',
        'DIA_VIERNES', 'DIA_SABADO', 'DIA_DOMINGO'
    ]
    for weekday, column in enumerate(weekday_columns):
        features[column] = int(dia_semana == weekday)

    for name, value in clima_data.items():
        if name.startswith('LLUVIA_') or name.startswith('DIAS_SECOS_'):
            features[name] = value

    # Convertir a DataFrame asegurando el orden de columnas del metadato
    X_pred = pd.DataFrame([features])[metadata['feature_cols']]

    # Realizar predicciones
    pred_count = float(np.clip(reg_model.predict(X_pred)[0], 0, None))
    prob_high = float(clf_model.predict_proba(X_pred)[0, 1])
    
    # Intervalo predictivo 80% via Negative Binomial
    nb_alpha = float(metadata.get('negative_binomial_alpha', 0.3))
    nb_var = pred_count + nb_alpha * pred_count ** 2
    nb_std = float(np.sqrt(max(nb_var, 0.01)))
    pred_low = max(0.0, pred_count - 1.2816 * nb_std)
    pred_high_bound = pred_count + 1.2816 * nb_std
    
    # Lógica de umbral dinámico desde metadatos
    threshold = metadata.get('classification_threshold', 0.20)
    reinforcement_threshold = metadata.get('operational_reinforcement_threshold', 0.50)
    pred_high = 1 if prob_high >= threshold else 0
    
    # Formatear el reporte de predicción en consola
    print("\n" + "="*50)
    print(f" REPORTES DE PREDICCIÓN - BOMBEROS TALCAHUANO")
    print(f" Fecha del reporte: {project_today()} | Fecha predicción: {target_date_str}")
    print("="*50)
    print(f" Cantidad esperada de emergencias: {pred_count:.1f} incidentes")
    print(f" Intervalo 80% (NB): [{pred_low:.1f}, {pred_high_bound:.1f}]")
    print(f" Probabilidad de día crítico (>7 eventos): {prob_high * 100:.1f}% (Umbral Alerta: {threshold * 100:.1f}%)")
    
    # Decisión de personal basada en la predicción combinada
    se_requiere_personal = (
        pred_count >= 8.0 or prob_high >= reinforcement_threshold
    )
    prealerta = pred_high == 1 and not se_requiere_personal

    print("-"*50)
    if se_requiere_personal:
        print("  ORDEN OPERATIVA: REFORZAR")
        print("  ACCIÓN: CONVOCAR DOTACIÓN ADICIONAL PARA MAÑANA")
    elif prealerta:
        print("  ORDEN OPERATIVA: PREALERTA")
        print("  ACCIÓN: CONFIRMAR DISPONIBILIDAD Y MANTENER PERSONAL LOCALIZABLE")
    else:
        print("  ORDEN OPERATIVA: GUARDIA NORMAL")
        print("  ACCIÓN: MANTENER DOTACIÓN ORDINARIA")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
