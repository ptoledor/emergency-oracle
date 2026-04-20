"""
Análisis de Estacionalidad - CBT Talcahuano
Descomposición de series temporales y patrones estacionalidad/clima.
"""
import os
import sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from statsmodels.tsa.seasonal import STL

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '..', '02_data')
DATA_PATH = os.path.join(BASE_DIR, '..', 'dataset_final_ml.csv')
TWEETS_PATH = os.path.join(DATA_DIR, 'tweets_procesados.csv')
OUTPUT_PATH = os.path.join(BASE_DIR, 'estacionalidad.html')

LOG_FILE = os.path.join(BASE_DIR, 'estacionalidad_log.txt')
_log_fh = open(LOG_FILE, 'w', encoding='utf-8')
def log(m): print(m); _log_fh.write(m + '\n'); _log_fh.flush()

try:
    log("=== Iniciando Analisis de Estacionalidad ===")
    
    # 1. Cargar y procesar datos base
    df = pd.read_csv(DATA_PATH)
    df['fecha_hora'] = pd.to_datetime(df['fecha_hora'], utc=True)
    df = df.sort_values('fecha_hora')
    
    # Definir estaciones (Hemisferio Sur)
    def get_season(m):
        if m in [12, 1, 2]: return 'Verano'
        if m in [3, 4, 5]: return 'Otoño'
        if m in [6, 7, 8]: return 'Invierno'
        return 'Primavera'
    
    df['estacion'] = df['mes'].map(get_season)
    
    # 2. Descomposición STL
    # Resamplamos a diario para una descomposición más limpia (señal menos ruidosa que horaria)
    daily = df.set_index('fecha_hora').resample('D')['n_emergencias'].sum()
    # STL requiere que no haya huecos y frecuencia fija
    daily = daily.asfreq('D').fillna(0)
    
    res = STL(daily, period=365).fit()
    
    df_decomp = pd.DataFrame({
        'observed': res.observed,
        'trend': res.trend,
        'seasonal': res.seasonal,
        'resid': res.resid
    }, index=daily.index)

    log("Descomposicion STL completada.")

    # 3. Analisis por tipo (Keyword search)
    log("Analizando tipos de emergencia por texto...")
    try:
        tw = pd.read_csv(TWEETS_PATH, sep=';')
        tw['Texto'] = tw['Texto'].str.lower()
        tw['es_incendio'] = tw['Texto'].str.contains('incendio|fuego|10-0|10-2').astype(int)
        tw['es_accidente'] = tw['Texto'].str.contains('accidente|colision|choque|10-4').astype(int)
        
        tw['Fecha'] = pd.to_datetime(tw['Fecha'], utc=True)
        tw['mes'] = tw['Fecha'].dt.month
        tw['estacion'] = tw['mes'].map(get_season)
        
        counts_tipo = tw.groupby('estacion')[['es_incendio', 'es_accidente']].sum()
    except Exception as e:
        log(f"Error analizando tipos: {e}")
        counts_tipo = pd.DataFrame()

    # ─── Construccion del Dashboard ─────────────────────────────────────────────
    
    fig = make_subplots(
        rows=4, cols=2,
        subplot_titles=[
            '<b>Descomposición: Observado (Diario)</b>',
            '<b>Descomposición: Tendencia a Largo Plazo</b>',
            '<b>Descomposición: Ciclo Estacional (Anual)</b>',
            '<b>Descomposición: Residuos (Ruido)</b>',
            '<b>Emergencias Promedio por Estación</b>',
            '<b>Incendios vs Accidentes por Estación</b>',
            '<b>Ciclo Horario Promedio por Estación</b>',
            '<b>Correlación Clima-Carga</b>',
        ],
        vertical_spacing=0.08,
        specs=[[{"type": "xy"}, {"type": "xy"}],
               [{"type": "xy"}, {"type": "xy"}],
               [{"type": "xy"}, {"type": "xy"}],
               [{"type": "xy"}, {"type": "xy"}]]
    )

    # STL Plots
    colors_stl = ['#636EFA', '#EF553B', '#00CC96', '#AB63FA']
    fig.add_trace(go.Scatter(x=df_decomp.index, y=df_decomp.observed, name='Observed', line_color=colors_stl[0]), row=1, col=1)
    fig.add_trace(go.Scatter(x=df_decomp.index, y=df_decomp.trend, name='Trend', line_color=colors_stl[1]), row=1, col=2)
    fig.add_trace(go.Scatter(x=df_decomp.index, y=df_decomp.seasonal, name='Seasonal', line_color=colors_stl[2]), row=2, col=1)
    fig.add_trace(go.Scatter(x=df_decomp.index, y=df_decomp.resid, name='Resid', mode='markers', marker=dict(size=3, color=colors_stl[3])), row=2, col=2)

    # Seasonal Aggregates
    est_order = ['Verano', 'Otoño', 'Invierno', 'Primavera']
    est_colors = ['#FFD700', '#FF8C00', '#1E90FF', '#32CD32']
    
    agg = df.groupby('estacion')['n_emergencias'].mean().reindex(est_order)
    fig.add_trace(go.Bar(x=agg.index, y=agg.values, marker_color=est_colors, name='Media Emergencias'), row=3, col=1)

    # Keywords Chart
    if not counts_tipo.empty:
        counts_tipo = counts_tipo.reindex(est_order)
        fig.add_trace(go.Bar(x=counts_tipo.index, y=counts_tipo['es_incendio'], name='Incendios', marker_color='red'), row=3, col=2)
        fig.add_trace(go.Bar(x=counts_tipo.index, y=counts_tipo['es_accidente'], name='Accidentes', marker_color='gray'), row=3, col=2)
        fig.update_layout(barmode='group')

    # Hourly cycle by season
    for i, est in enumerate(est_order):
        hourly_est = df[df['estacion'] == est].groupby('hora')['n_emergencias'].mean()
        fig.add_trace(go.Scatter(x=hourly_est.index, y=hourly_est.values, name=est, line=dict(color=est_colors[i], width=3)), row=4, col=1)

    # Climate Correlation (Heatmap o Scatter)
    # Mostramos relacion Temp vs Emergencias (promedio mensual)
    df_clima = df.groupby(['anio', 'mes', 'estacion'])[['n_emergencias', 'temperatura']].mean().reset_index()
    fig.add_trace(go.Scatter(
        x=df_clima['temperatura'], y=df_clima['n_emergencias'],
        mode='markers', text=df_clima['estacion'],
        marker=dict(size=10, color=[est_colors[est_order.index(s)] for s in df_clima['estacion']], opacity=0.8),
        name='Clima vs Carga'
    ), row=4, col=2)

    # Layout
    fig.update_layout(
        title=dict(text='<b>Análisis de Estacionalidad de Emergencias - CBT Talcahuano</b>', x=0.5, font=dict(size=22)),
        height=1400,
        template='plotly_white',
    )
    
    fig.update_xaxes(title_text='Fecha', row=1, col=1)
    fig.update_xaxes(title_text='Fecha', row=1, col=2)
    fig.update_xaxes(title_text='Hora del Día', row=4, col=1)
    fig.update_xaxes(title_text='Temperatura Media (°C)', row=4, col=2)
    fig.update_yaxes(title_text='Emergencias (Media)', row=3, col=1)
    fig.update_yaxes(title_text='N Emergencias', row=3, col=2)

    fig.write_html(OUTPUT_PATH, include_plotlyjs='cdn')
    log(f"Analisis guardado en: {OUTPUT_PATH}")

except Exception as e:
    log(f"ERROR: {e}")
    import traceback
    log(traceback.format_exc())
finally:
    _log_fh.close()
