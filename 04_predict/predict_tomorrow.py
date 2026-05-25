import pandas as pd
import numpy as np
import requests
import os
import pickle
import datetime
import holidays
import argparse

def get_event_counts_for_dates(csv_path, codes_path, dates):
    """
    Carga los tweets locales, los limpia deduplicando los códigos CBT y
    retorna el conteo de eventos para cada una de las fechas especificadas.
    """
    df = pd.read_csv(csv_path, sep=';', decimal=',')
    df['FECHA_DIA'] = df['Fecha'].astype(str).str[:10]
    
    # Extraer código de emergencia usando el mismo regex
    patron = r"\b(10-\d+(?:-\d+)?)\b"
    df['CODIGO_EMERGENCIA'] = df['Texto'].str.extract(patron)

    # Imputar códigos
    mask_pastizal = df['Texto'].str.contains(r'PASTIZAL|FORESTAL', case=False, na=False)
    mask_incendio = df['Texto'].str.contains(r'INCENDIO', case=False, na=False)
    df['CODIGO_EMERGENCIA'] = np.where(
        df['CODIGO_EMERGENCIA'].notna(), df['CODIGO_EMERGENCIA'],
        np.where(mask_pastizal, '10-2-3',
        np.where(mask_incendio, '10-0-6', '0-0-0'))
    )

    # Cargar y deduplicar claves CBT
    codigos = pd.read_excel(codes_path)
    codigos_clean = codigos.drop_duplicates(subset=['CODIGO_EMERGENCIA']).copy()

    # Merge limpio
    df_merged = pd.merge(df, codigos_clean, how='left', on='CODIGO_EMERGENCIA')
    
    # Contar por día
    daily_counts = df_merged.groupby('FECHA_DIA').size().to_dict()
    
    return [daily_counts.get(d, 0) for d in dates]


def main():
    # Parámetros y directorios base
    base_dir = "c:/Users/ptole/Desktop/Pitters-Git/emergency-oracle"
    models_dir = f"{base_dir}/03_model/saved_models"
    raw_tweets_path = f"{base_dir}/02_data/compiled_scraped_data.csv"
    claves_cbt_path = f"{base_dir}/02_data/Clave_CBT.xlsx"
    lat, lon = -36.731106, -73.11023  # Coordenadas de Talcahuano

    # Cargar modelos
    with open(f"{models_dir}/regressor.pkl", "rb") as f:
        reg_model = pickle.load(f)
    with open(f"{models_dir}/classifier.pkl", "rb") as f:
        clf_model = pickle.load(f)
    with open(f"{models_dir}/metadata.pkl", "rb") as f:
        metadata = pickle.load(f)

    # Detectar última fecha disponible en el dataset
    df_raw = pd.read_csv(raw_tweets_path, sep=';', decimal=',')
    max_date_in_dataset = df_raw['Fecha'].astype(str).str[:10].max()
    max_dt = datetime.datetime.strptime(max_date_in_dataset, '%Y-%m-%d').date()

    # Por defecto, si el dataset está desactualizado, ofrecemos predecir el día siguiente al dataset
    # Pero también permitimos predecir el mañana real usando argumentos
    parser = argparse.ArgumentParser(description="Predictor de emergencias para Bomberos de Talcahuano")
    parser.add_argument('--date', type=str, help="Fecha a predecir (YYYY-MM-DD). Por defecto predice el día siguiente al dataset.")
    parser.add_argument('--real-tomorrow', action='store_true', help="Fuerza a predecir el día de mañana real.")
    args = parser.parse_args()

    # Definir fecha objetivo y lags
    if args.real_tomorrow:
        target_date = datetime.date.today() + datetime.timedelta(days=1)
        mode = "MAÑANA REAL"
    elif args.date:
        target_date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
        mode = "FECHA ESPECÍFICA"
    else:
        # Día siguiente al fin del dataset (útil para pruebas)
        target_date = max_dt + datetime.timedelta(days=1)
        mode = "PROGRESIÓN DEL DATASET"

    print(f"=== PASO 4: Predicción de Emergencias (Modo: {mode}) ===")
    print(f"Último dato en dataset local: {max_date_in_dataset}")
    print(f"Fecha a predecir: {target_date}")

    # Calcular las fechas de los lags para la fecha objetivo
    day_lag_1 = target_date - datetime.timedelta(days=1)
    day_lag_2 = target_date - datetime.timedelta(days=2)
    day_lag_3 = target_date - datetime.timedelta(days=3)

    lag_dates_str = [d.strftime('%Y-%m-%d') for d in [day_lag_3, day_lag_2, day_lag_1]]
    print(f"Cálculo de lags usando fechas: {lag_dates_str}")

    # Obtener conteos de eventos para los lags
    # Si las fechas exceden el max_date del dataset, notificamos que se asumen como 0 o se usan los disponibles
    lag_counts = get_event_counts_for_dates(raw_tweets_path, claves_cbt_path, lag_dates_str)
    eventos_lag_3, eventos_lag_2, eventos_lag_1 = lag_counts
    eventos_rolling_mean_3d = sum(lag_counts) / 3.0

    print(f"Conteo de eventos en lags:")
    print(f"  - Hace 3 días ({lag_dates_str[0]}): {eventos_lag_3}")
    print(f"  - Hace 2 días ({lag_dates_str[1]}): {eventos_lag_2}")
    print(f"  - Ayer/Hoy ({lag_dates_str[2]}): {eventos_lag_1}")
    print(f"  - Media móvil 3 días: {eventos_rolling_mean_3d:.2f}")

    # Obtener clima para la fecha objetivo y los lags
    # Si la fecha objetivo está en el futuro, usamos la API de pronóstico de Open-Meteo
    # Si la fecha objetivo está en el pasado (histórica), usamos la API de archivo histórico de Open-Meteo
    target_date_str = target_date.strftime('%Y-%m-%d')
    today_date = datetime.date.today()

    clima_data = {}
    if target_date > today_date:
        print("Obteniendo pronóstico del clima en tiempo real desde Open-Meteo...")
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&"
               f"daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
               f"relative_humidity_2m_max,relative_humidity_2m_min,relative_humidity_2m_mean,"
               f"wind_speed_10m_max,wind_speed_10m_mean,"
               f"precipitation_sum&"
               f"timezone=America%2FSantiago&past_days=3")
        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error al descargar pronóstico: {response.text}")
        data = response.json()['daily']
        
        # Buscar el índice de la fecha objetivo en los resultados
        try:
            idx = data['time'].index(target_date_str)
        except ValueError:
            raise ValueError(f"La fecha objetivo {target_date_str} no se encuentra en el rango del pronóstico de Open-Meteo.")

        clima_data['TEMP_MAX'] = data['temperature_2m_max'][idx]
        clima_data['TEMP_MIN'] = data['temperature_2m_min'][idx]
        clima_data['TEMP_MEDIA'] = data['temperature_2m_mean'][idx]
        clima_data['HUM_MAX'] = data['relative_humidity_2m_max'][idx]
        clima_data['HUM_MIN'] = data['relative_humidity_2m_min'][idx]
        clima_data['HUM_MEDIA'] = data['relative_humidity_2m_mean'][idx]
        clima_data['VIENTO_MAX'] = data['wind_speed_10m_max'][idx]
        clima_data['VIENTO_MEDIO'] = data['wind_speed_10m_mean'][idx]
        clima_data['LLUVIA'] = data['precipitation_sum'][idx]

        # Lluvia lags desde la API de clima
        idx_lag_1 = idx - 1
        idx_lag_2 = idx - 2
        idx_lag_3 = idx - 3
        clima_data['LLUVIA_lag_1'] = data['precipitation_sum'][idx_lag_1]
        clima_data['LLUVIA_lag_2'] = data['precipitation_sum'][idx_lag_2]
        clima_data['LLUVIA_lag_3'] = data['precipitation_sum'][idx_lag_3]
        clima_data['LLUVIA_accum_3d'] = clima_data['LLUVIA_lag_1'] + clima_data['LLUVIA_lag_2'] + clima_data['LLUVIA_lag_3']
    else:
        print("Obteniendo clima histórico desde Open-Meteo...")
        # Consultamos el rango desde lag_3 hasta el target_date
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={lag_dates_str[0]}&end_date={target_date_str}&"
               f"daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
               f"relative_humidity_2m_max,relative_humidity_2m_min,relative_humidity_2m_mean,"
               f"wind_speed_10m_max,wind_speed_10m_mean,"
               f"precipitation_sum&"
               f"timezone=America%2FSantiago&format=json")
        response = requests.get(url)
        if response.status_code != 200:
            raise RuntimeError(f"Error al descargar clima histórico: {response.text}")
        data = response.json()['daily']
        
        # El último elemento es el target
        clima_data['TEMP_MAX'] = data['temperature_2m_max'][-1]
        clima_data['TEMP_MIN'] = data['temperature_2m_min'][-1]
        clima_data['TEMP_MEDIA'] = data['temperature_2m_mean'][-1]
        clima_data['HUM_MAX'] = data['relative_humidity_2m_max'][-1]
        clima_data['HUM_MIN'] = data['relative_humidity_2m_min'][-1]
        clima_data['HUM_MEDIA'] = data['relative_humidity_2m_mean'][-1]
        clima_data['VIENTO_MAX'] = data['wind_speed_10m_max'][-1]
        clima_data['VIENTO_MEDIO'] = data['wind_speed_10m_mean'][-1]
        clima_data['LLUVIA'] = data['precipitation_sum'][-1]

        # Lluvia lags
        clima_data['LLUVIA_lag_1'] = data['precipitation_sum'][-2]
        clima_data['LLUVIA_lag_2'] = data['precipitation_sum'][-3]
        clima_data['LLUVIA_lag_3'] = data['precipitation_sum'][-4]
        clima_data['LLUVIA_accum_3d'] = clima_data['LLUVIA_lag_1'] + clima_data['LLUVIA_lag_2'] + clima_data['LLUVIA_lag_3']

    print(f"Condiciones climáticas para la predicción:")
    print(f"  - Temperatura (Mín/Med/Máx): {clima_data['TEMP_MIN']}°C / {clima_data['TEMP_MEDIA']}°C / {clima_data['TEMP_MAX']}°C")
    print(f"  - Humedad Media: {clima_data['HUM_MEDIA']}%")
    print(f"  - Viento (Medio/Máx): {clima_data['VIENTO_MEDIO']} km/h / {clima_data['VIENTO_MAX']} km/h")
    print(f"  - Precipitación estimada: {clima_data['LLUVIA']} mm")
    print(f"  - Lluvia acumulada últimos 3 días: {clima_data['LLUVIA_accum_3d']:.1f} mm")

    # Características de calendario y feriado
    chile_holidays = holidays.Chile(years=[target_date.year])
    mes = target_date.month
    dia_semana = target_date.weekday()
    es_fin_semana = 1 if dia_semana in [5, 6] else 0
    es_feriado = 1 if target_date_str in chile_holidays else 0

    # Crear el vector de características en el orden correcto
    features = {
        'TEMP_MAX': clima_data['TEMP_MAX'],
        'TEMP_MIN': clima_data['TEMP_MIN'],
        'TEMP_MEDIA': clima_data['TEMP_MEDIA'],
        'HUM_MAX': clima_data['HUM_MAX'],
        'HUM_MIN': clima_data['HUM_MIN'],
        'HUM_MEDIA': clima_data['HUM_MEDIA'],
        'VIENTO_MAX': clima_data['VIENTO_MAX'],
        'VIENTO_MEDIO': clima_data['VIENTO_MEDIO'],
        'LLUVIA': clima_data['LLUVIA'],
        'EVENTOS_lag_1': eventos_lag_1,
        'EVENTOS_lag_2': eventos_lag_2,
        'EVENTOS_lag_3': eventos_lag_3,
        'EVENTOS_rolling_mean_3d': eventos_rolling_mean_3d,
        'LLUVIA_lag_1': clima_data['LLUVIA_lag_1'],
        'LLUVIA_lag_2': clima_data['LLUVIA_lag_2'],
        'LLUVIA_lag_3': clima_data['LLUVIA_lag_3'],
        'LLUVIA_accum_3d': clima_data['LLUVIA_accum_3d'],
        'MES': mes,
        'DIA_SEMANA': dia_semana,
        'ES_FIN_SEMANA': es_fin_semana,
        'ES_FERIADO': es_feriado
    }

    # Convertir a DataFrame asegurando el orden de columnas del metadato
    X_pred = pd.DataFrame([features])[metadata['feature_cols']]

    # Realizar predicciones
    pred_count = reg_model.predict(X_pred)[0]
    prob_high = clf_model.predict_proba(X_pred)[0, 1]
    pred_high = clf_model.predict(X_pred)[0]

    # Formatear el reporte de predicción en consola
    print("\n" + "="*50)
    print(f" REPORTES DE PREDICCIÓN - BOMBEROS TALCAHUANO")
    print(f" Fecha del reporte: {datetime.date.today()} | Fecha predicción: {target_date_str}")
    print("="*50)
    print(f" Cantidad esperada de emergencias: {pred_count:.1f} incidentes")
    print(f" Probabilidad de día crítico (>7 eventos): {prob_high * 100:.1f}%")
    
    # Decisión de personal basada en la predicción combinada
    # Si la regresión es >= 8 o la clasificación dice que es alta
    se_requiere_personal = (pred_count >= 8.0) or (pred_high == 1)

    print("-"*50)
    if se_requiere_personal:
        print("  ESTADO DE ALERTA: ALTA DEMANDA PREVISTA")
        print("  RECOMENDACIÓN: SÍ, SE REQUIERE PERSONAL ADICIONAL")
        print("  (Se aconseja reforzar la guardia nocturna y unidades de rescate)")
    else:
        print("  ESTADO DE ALERTA: ACTIVIDAD NORMAL PREVISTA")
        print("  RECOMENDACIÓN: NO SE REQUIERE PERSONAL ADICIONAL")
        print("  (Guardia ordinaria suficiente)")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
