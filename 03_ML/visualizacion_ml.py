"""
Dashboard de Resultados de Machine Learning - CBT Talcahuano
Consolida métricas, importancia de variables y visualización de predicciones.
"""
import os
import sys
import asyncio
import joblib
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Configuracion de rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(BASE_DIR, '..')
MODEL_DIR = os.path.join(ROOT_DIR, 'modelos')
DATA_PATH = os.path.join(ROOT_DIR, 'dataset_final_ml.csv')
METRICS_PATH = os.path.join(ROOT_DIR, 'resultados_modelos.csv')
OUTPUT_PATH = os.path.join(BASE_DIR, 'resumen_ml.html')

LOG_FILE = os.path.join(BASE_DIR, 'visualizacion_ml_log.txt')
_log_fh = open(LOG_FILE, 'w', encoding='utf-8')

def log(m):
    print(m)
    _log_fh.write(m + '\n')
    _log_fh.flush()

try:
    log("=== Iniciando Visualizacion ML ===")

    # 1. Cargar Datos
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"No se encontro el dataset en {DATA_PATH}")
    
    df = pd.read_csv(DATA_PATH)
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'])
    
    # 2. Cargar Metricas
    if os.path.exists(METRICS_PATH):
        df_metrics = pd.read_csv(METRICS_PATH)
        log("Metricas cargadas correctamente.")
    else:
        df_metrics = pd.DataFrame(columns=['modelo', 'MAE', 'RMSE', 'Poisson_Dev', 'R2'])
        log("Advertencia: No se encontro resultados_modelos.csv")

    # 3. Cargar Modelo y Features
    features = joblib.load(os.path.join(MODEL_DIR, 'features.pkl'))
    mejor_modelo_nombre = joblib.load(os.path.join(MODEL_DIR, 'mejor_modelo.pkl'))
    log(f"Mejor modelo detectado: {mejor_modelo_nombre}")

    # Cargar el modelo segun sea XGB o RF
    if 'XGBoost' in mejor_modelo_nombre:
        import xgboost as xgb
        model = xgb.XGBRegressor()
        model.load_model(os.path.join(MODEL_DIR, 'xgb_emergencias_hora.json'))
    else:
        model = joblib.load(os.path.join(MODEL_DIR, 'rf_emergencias_hora.pkl'))

    # 4. Generar Predicciones (usamos el set de test - ultimas 20%)
    split = int(len(df) * 0.8)
    df_test = df.iloc[split:].copy()
    X_test = df_test[features]
    y_test = df_test['n_emergencias']
    
    df_test['pred'] = model.predict(X_test)
    df_test['pred'] = np.clip(df_test['pred'], 0, None)
    df_test['error'] = df_test['n_emergencias'] - df_test['pred']
    df_test['abs_error'] = df_test['error'].abs()

    log("Predicciones generadas.")

    # ─── Construccion del Dashboard ─────────────────────────────────────────────
    
    fig = make_subplots(
        rows=3, cols=3,
        subplot_titles=[
            '<b>Comparativa de Modelos (Métricas)</b>',
            '<b>Importancia de Variables (Top 15)</b>',
            '<b>Distribución de Errores (Residuos)</b>',
            '<b>Real vs Predicho (Últimas 2 Semanas)</b>',
            '<b>Gráfico de Dispersión (Real vs Pred)</b>',
            '<b>Error Promedio (MAE) por Hora</b>',
            '<b>Error Promedio por Día de Semana</b>',
            '<b>Error acumulado por Mes (Test Set)</b>',
            '<b>Resumen Ejecutivo</b>',
        ],
        vertical_spacing=0.1,
        horizontal_spacing=0.07,
        specs=[
            [{"type": "table"}, {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy", "colspan": 2}, None, {"type": "xy"}],
            [{"type": "xy"}, {"type": "xy"}, {"type": "table"}]
        ]
    )

    # 1. Tabla de Metricas
    fig.add_trace(go.Table(
        header=dict(values=['<b>Modelo</b>', '<b>MAE</b>', '<b>RMSE</b>', '<b>R²</b>'],
                    fill_color='paleturquoise', align='left'),
        cells=dict(values=[df_metrics.modelo, 
                           df_metrics.MAE.round(4), 
                           df_metrics.RMSE.round(4), 
                           df_metrics.R2.round(4)],
                   fill_color='lavender', align='left')
    ), row=1, col=1)

    # 2. Importancia de Variables
    if hasattr(model, 'feature_importances_'):
        imp = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False).head(15)
        fig.add_trace(go.Bar(
            x=imp.values, y=imp.index, orientation='h',
            marker_color='royalblue', name='Importancia'
        ), row=1, col=2)
    
    # 3. Distribucion de Errores
    fig.add_trace(go.Histogram(
        x=df_test['error'], nbinsx=50,
        marker_color='indianred', name='Residuos'
    ), row=1, col=3)

    # 4. Real vs Predicho (Timeline)
    n_viz = 24 * 14 # 2 semanas
    recent = df_test.tail(n_viz)
    fig.add_trace(go.Scatter(
        x=recent['fecha_hora'], y=recent['n_emergencias'],
        name='Real', line=dict(color='steelblue', width=2),
        fill='tozeroy', fillcolor='rgba(70,130,180,0.2)'
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=recent['fecha_hora'], y=recent['pred'],
        name='Predicción', line=dict(color='crimson', width=2, dash='dot')
    ), row=2, col=1)

    # 5. Scatter Real vs Pred
    fig.add_trace(go.Scatter(
        x=y_test, y=df_test['pred'],
        mode='markers', marker=dict(size=4, opacity=0.3, color='darkorange'),
        name='Observaciones'
    ), row=2, col=3)
    max_val = max(y_test.max(), df_test['pred'].max())
    fig.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode='lines', line=dict(color='black', dash='dash'),
        name='Ideal'
    ), row=2, col=3)

    # 6. Error por Hora
    err_hora = df_test.groupby('hora')['abs_error'].mean()
    fig.add_trace(go.Bar(
        x=err_hora.index, y=err_hora.values,
        marker_color='mediumpurple', name='MAE/Hora'
    ), row=3, col=1)

    # 7. Error por Dia de Semana
    dias = ['Lun','Mar','Mie','Jue','Vie','Sab','Dom']
    err_dia = df_test.groupby('dia_semana')['abs_error'].mean()
    fig.add_trace(go.Bar(
        x=dias, y=err_dia.values,
        marker_color='coral', name='MAE/Día'
    ), row=3, col=2)

    # 8. Tabla Resumen Ejecutivo
    mae_mejor = df_test['abs_error'].mean()
    rmse_mejor = np.sqrt(mean_squared_error(y_test, df_test['pred']))
    r2_mejor = r2_score(y_test, df_test['pred'])
    
    summary_data = [
        ['Métrica', 'Valor'],
        ['Mejor Modelo', mejor_modelo_nombre],
        ['MAE (Test)', f"{mae_mejor:.4f}"],
        ['RMSE (Test)', f"{rmse_mejor:.4f}"],
        ['R² (Test)', f"{r2_mejor:.4f}"],
        ['Total Muestras Test', f"{len(y_test)}"],
        ['Sesgo Medio', f"{df_test['error'].mean():.4f}"]
    ]
    
    fig.add_trace(go.Table(
        header=dict(values=['<b>Propiedad</b>', '<b>Valor</b>'],
                    fill_color='gold', align='left'),
        cells=dict(values=[[x[0] for x in summary_data[1:]], 
                           [x[1] for x in summary_data[1:]]],
                   fill_color='lightyellow', align='left')
    ), row=3, col=3)

    # 9. Pronostico Futuro (Next 24h)
    log("\n=== Generando Pronostico Futuro ===")
    
    def fetch_forecast(lat=-36.731106, lon=-73.11023):
        import requests
        from io import StringIO
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
            f"wind_gusts_10m,precipitation,weather_code"
            f"&timezone=America%2FSantiago&format=csv"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df_w = pd.read_csv(StringIO(resp.text), skiprows=3)
        df_w.columns = ['fecha_hora', 'temperatura', 'humedad', 'viento_vel',
                        'viento_racha', 'precipitacion', 'cod_clima']
        df_w['fecha_hora'] = pd.to_datetime(df_w['fecha_hora']).dt.tz_localize('America/Santiago', ambiguous='infer')
        return df_w

    try:
        forecast_w = fetch_forecast()
        log("Pronostico de clima obtenido.")
        
        # Filtrar desde el ultimo momento en el dataset hasta +24h desde "ahora"
        ahora = pd.Timestamp.now(tz='America/Santiago').floor('h')
        ultimo_registro = df['fecha_hora'].max()
        
        # Estandarizar zonas horarias para evitar error en date_range
        if ultimo_registro.tzinfo is not None:
            ahora = ahora.tz_convert(ultimo_registro.tzinfo)
        
        # Necesitamos rellenar el hueco entre el ultimo registro y el futuro
        slots = pd.date_range(start=ultimo_registro + pd.Timedelta(hours=1), 
                              end=ahora + pd.Timedelta(hours=24), freq='h')
        
        df_future = pd.DataFrame({'fecha_hora': slots})
        df_future = df_future.merge(forecast_w, on='fecha_hora', how='left')
        
        # Rellenar clima si falta (extrapolar ultimo conocido)
        df_future = df_future.ffill().bfill()

        # Feature Engineering para el futuro
        def enrich_temporal(df_f):
            dt = df_f['fecha_hora']
            df_f['hora']          = dt.dt.hour
            df_f['dia_semana']    = dt.dt.dayofweek
            df_f['mes']           = dt.dt.month
            df_f['dia_mes']       = dt.dt.day
            df_f['anio']          = dt.dt.year
            df_f['trimestre']     = dt.dt.quarter
            df_f['semana_anio']   = dt.dt.isocalendar().week.astype(int)
            df_f['es_fin_semana'] = (df_f['dia_semana'] >= 5).astype(int)
            df_f['es_verano']     = df_f['mes'].isin([12, 1, 2]).astype(int)
            # Feriados simplificado para el dashboard (solo proximos conocidos)
            df_f['es_feriado']    = 0 
            df_f['es_noche']      = ((df_f['hora'] >= 22) | (df_f['hora'] < 6)).astype(int)
            df_f['hora_sin']      = np.sin(2 * np.pi * df_f['hora'] / 24)
            df_f['hora_cos']      = np.cos(2 * np.pi * df_f['hora'] / 24)
            df_f['dia_sem_sin']   = np.sin(2 * np.pi * df_f['dia_semana'] / 7)
            df_f['dia_sem_cos']   = np.cos(2 * np.pi * df_f['dia_semana'] / 7)
            df_f['mes_sin']       = np.sin(2 * np.pi * df_f['mes'] / 12)
            df_f['mes_cos']       = np.cos(2 * np.pi * df_f['mes'] / 12)
            return df_f

        df_future = enrich_temporal(df_future)
        
        # Prediccion recursiva para Lags y Rolling stats
        # Esto es complejo porque dependemos de lo que predijimos antes.
        # Por simplicidad en este Dashboard, usaremos valores proyectados o "zero" 
        # para los lags si la brecha es muy grande, o recursividad simple.
        
        historico_completo = df[['fecha_hora', 'n_emergencias']].copy()
        
        future_preds = []
        for i, row in df_future.iterrows():
            # Construir lags base en lo que sabemos (hist + preds previas)
            current_ts = row['fecha_hora']
            def get_val(offset_h):
                ts = current_ts - pd.Timedelta(hours=offset_h)
                match = historico_completo[historico_completo['fecha_hora'] == ts]
                if not match.empty: return match['n_emergencias'].values[0]
                return 0 # Default fallback
            
            row_dict = row.to_dict()
            row_dict['lag_1h']   = get_val(1)
            row_dict['lag_2h']   = get_val(2)
            row_dict['lag_3h']   = get_val(3)
            row_dict['lag_24h']  = get_val(24)
            row_dict['lag_48h']  = get_val(48)
            row_dict['lag_168h'] = get_val(168)
            
            # Rolling (simplificado: media de ultmos 24h conocidos)
            row_dict['roll_mean_24h'] = historico_completo.tail(24)['n_emergencias'].mean()
            row_dict['roll_mean_7d']  = historico_completo.tail(168)['n_emergencias'].mean()
            row_dict['roll_std_7d']   = historico_completo.tail(168)['n_emergencias'].std()
            row_dict['roll_sum_24h']  = historico_completo.tail(24)['n_emergencias'].sum()
            row_dict['roll_max_24h']  = historico_completo.tail(24)['n_emergencias'].max()
            
            # Predict
            X_curr = pd.DataFrame([row_dict])[features]
            p = model.predict(X_curr)[0]
            p = max(0, p)
            future_preds.append(p)
            
            # Actualizar historico para el siguiente paso recursivo
            new_row = pd.DataFrame({'fecha_hora': [current_ts], 'n_emergencias': [p]})
            historico_completo = pd.concat([historico_completo, new_row])
            
        df_future['pred'] = future_preds
        log("Pronostico calculado.")

        # Actualizar Dashboard (Fila 4 o reajuste)
        # Vamos a reconfigurar la fila 2 para que incluya el forecast a la derecha
        fig = make_subplots(
            rows=4, cols=3,
            subplot_titles=[
                '<b>Comparativa de Modelos</b>',
                '<b>Importancia de Variables (Top 15)</b>',
                '<b>Distribución de Errores (Residuos)</b>',
                '<b>Real vs Predicho (Últimas 2 Semanas)</b>',
                '<b>Pronóstico Próximas 24 Horas</b>',
                '<b>Gráfico de Dispersión (Real vs Pred)</b>',
                '<b>Error Promedio (MAE) por Hora</b>',
                '<b>Error Promedio por Día de Semana</b>',
                '<b>Resumen Ejecutivo</b>',
                '<b>Probabilidad de Emergencia (Heatmap Temp)</b>',
            ],
            vertical_spacing=0.08,
            horizontal_spacing=0.07,
            specs=[
                [{"type": "table"}, {"type": "xy"}, {"type": "xy"}],
                [{"type": "xy", "colspan": 2}, None, {"type": "xy"}],
                [{"type": "xy", "colspan": 2}, None, {"type": "xy"}],
                [{"type": "xy"}, {"type": "xy"}, {"type": "table"}]
            ]
        )
        
        # Reducimos un poco el timeline para dejar espacio al forecast
        # (Se repite la logica de arriba pero con el nuevo grid)
        # Re-usamos las trazas anteriores... (Simplificado: volvemos a añadir todo al nuevo fig)
        
        # [Re-Añadir trazas 1-3]
        fig.add_trace(go.Table(
            header=dict(values=['<b>Modelo</b>', '<b>MAE</b>', '<b>R²</b>'], fill_color='paleturquoise'),
            cells=dict(values=[df_metrics.modelo, df_metrics.MAE.round(4), df_metrics.R2.round(4)], fill_color='lavender')
        ), row=1, col=1)
        
        if hasattr(model, 'feature_importances_'):
            imp = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False).head(15)
            fig.add_trace(go.Bar(x=imp.values, y=imp.index, orientation='h', marker_color='royalblue', name='Importancia'), row=1, col=2)
        
        fig.add_trace(go.Histogram(x=df_test['error'], nbinsx=40, marker_color='indianred', name='Residuos'), row=1, col=3)

        # [Fila 2: Timeline]
        fig.add_trace(go.Scatter(x=recent['fecha_hora'], y=recent['n_emergencias'], name='Real', line=dict(color='steelblue', width=2), fill='tozeroy'), row=2, col=1)
        fig.add_trace(go.Scatter(x=recent['fecha_hora'], y=recent['pred'], name='Predicción', line=dict(color='crimson', width=2, dash='dot')), row=2, col=1)
        
        fig.add_trace(go.Scatter(x=y_test, y=df_test['pred'], mode='markers', marker=dict(size=4, opacity=0.3, color='darkorange'), name='Observaciones'), row=2, col=3)
        max_v = max(y_test.max(), df_test['pred'].max())
        fig.add_trace(go.Scatter(x=[0, max_v], y=[0, max_v], mode='lines', line=dict(color='black', dash='dash'), name='Ideal'), row=2, col=3)

        # [Fila 3: FORECAST]
        df_forecast_viz = df_future[df_future['fecha_hora'] >= ahora]
        fig.add_trace(go.Scatter(
            x=df_forecast_viz['fecha_hora'], y=df_forecast_viz['pred'],
            name='Forecast', line=dict(color='green', width=3),
            fill='tozeroy', fillcolor='rgba(0,128,0,0.1)'
        ), row=3, col=1)
        # Añadir info de clima al hover del forecast
        fig.update_traces(
            hovertemplate='<b>%{x}</b><br>Pred: %{y:.2f} emerg/h<extra></extra>',
            row=3, col=1
        )
        
        # Heatmap de probabilidad (basado en la intensidad de la prediccion)
        grid_f = df_forecast_viz.pivot_table(index='hora', values='pred', aggfunc='mean')
        fig.add_trace(go.Heatmap(
            z=[df_forecast_viz['pred'].values],
            x=df_forecast_viz['fecha_hora'],
            y=['Intensidad'],
            colorscale='Viridis',
            showscale=False,
            name='Calor'
        ), row=3, col=3)

        # [Fila 4: Errores y Resumen]
        fig.add_trace(go.Bar(x=err_hora.index, y=err_hora.values, marker_color='mediumpurple', name='MAE/Hora'), row=4, col=1)
        fig.add_trace(go.Bar(x=dias, y=err_dia.values, marker_color='coral', name='MAE/Día'), row=4, col=2)
        fig.add_trace(go.Table(
            header=dict(values=['<b>Propiedad</b>', '<b>Valor</b>'], fill_color='gold'),
            cells=dict(values=[[x[0] for x in summary_data[1:]], [x[1] for x in summary_data[1:]]], fill_color='lightyellow')
        ), row=4, col=3)

        fig.update_layout(
            title=dict(
                text=f'<b>Dashboard Inteligente CBT Talcahuano</b><br><sup>Modelo: {mejor_modelo_nombre} | Pronóstico actualizado a {ahora.strftime("%H:%M")}</sup>',
                x=0.5, font=dict(size=20)
            ),
            height=1300,
            template='plotly_white'
        )
        
        fig.update_xaxes(title_text="Próximas 24 Horas", row=3, col=1)
        fig.update_yaxes(title_text="Emergencias Predichas", row=3, col=1)

    except Exception as fe:
        log(f"Error al generar pronostico: {fe}")
        # Si falla el pronostico, mantenemos el dashboard original (ya definido antes del try)
        pass

    # Guardar
    fig.write_html(OUTPUT_PATH, include_plotlyjs='cdn')
    log(f"Dashboard final guardado en: {OUTPUT_PATH}")

except Exception as e:
    log(f"ERROR: {str(e)}")
    import traceback
    log(traceback.format_exc())

finally:
    _log_fh.close()
