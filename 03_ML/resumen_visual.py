"""
Resumen grafico de emergencias CBT Talcahuano
por hora / dia de semana / mes
"""
import sys, os, asyncio
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

LOG = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resumen_log.txt'), 'w', encoding='utf-8')
def log(m): LOG.write(m+'\n'); LOG.flush()

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '02_data')

log("Cargando datos...")
tweets = pd.read_csv(os.path.join(DATA_DIR, 'tweets_procesados.csv'), sep=';')
tweets['Fecha'] = pd.to_datetime(tweets['Fecha'], utc=True)
tweets['ts']    = tweets['Fecha'].dt.tz_convert('America/Santiago')

tweets['hora']       = tweets['ts'].dt.hour
tweets['dia_semana'] = tweets['ts'].dt.dayofweek
tweets['mes']        = tweets['ts'].dt.month
tweets['anio']       = tweets['ts'].dt.year
tweets['fecha_dia']  = tweets['ts'].dt.date.astype(str)

n = len(tweets)
log(f"Total emergencias: {n:,}")

# ── Preparar series horarias para promedios correctos ─────────────────────
# Necesitamos promediar sobre horas, no sumar tweets directamente
idx = pd.date_range(
    start=tweets['ts'].min().floor('D'),
    end=tweets['ts'].max().ceil('D'),
    freq='h', tz='America/Santiago'
)
dfh = pd.DataFrame({'ts': idx})
dfh['hora']       = dfh['ts'].dt.hour
dfh['dia_semana'] = dfh['ts'].dt.dayofweek
dfh['mes']        = dfh['ts'].dt.month
dfh['anio']       = dfh['ts'].dt.year

# Contar emergencias por hora
cnt = tweets.groupby(tweets['ts'].dt.floor('h')).size().rename('n').reset_index()
cnt.columns = ['ts', 'n']
dfh = dfh.merge(cnt, on='ts', how='left').fillna({'n': 0})
dfh['n'] = dfh['n'].astype(int)
log(f"Horas en indice: {len(dfh):,}")

# ── Labels ────────────────────────────────────────────────────────────────
DIAS  = ['Lunes','Martes','Miercoles','Jueves','Viernes','Sabado','Domingo']
MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
PALETA = px.colors.qualitative.Plotly

# ── 1. Por hora del dia ───────────────────────────────────────────────────
por_hora = dfh.groupby('hora')['n'].agg(['mean','sum','std']).reset_index()
por_hora.columns = ['hora','media','total','std']

# ── 2. Por dia de semana ──────────────────────────────────────────────────
por_dia = dfh.groupby('dia_semana')['n'].agg(['mean','sum']).reset_index()
por_dia.columns = ['dia_semana','media','total']
por_dia['dia_lbl'] = por_dia['dia_semana'].map(lambda x: DIAS[x])

# ── 3. Por mes ────────────────────────────────────────────────────────────
por_mes = dfh.groupby('mes')['n'].agg(['mean','sum']).reset_index()
por_mes.columns = ['mes','media','total']
por_mes['mes_lbl'] = por_mes['mes'].map(lambda x: MESES[x-1])

# ── 4. Heatmap hora x dia de semana ──────────────────────────────────────
pivot_hd = dfh.pivot_table(values='n', index='hora', columns='dia_semana', aggfunc='mean')

# ── 5. Heatmap hora x mes ─────────────────────────────────────────────────
pivot_hm = dfh.pivot_table(values='n', index='hora', columns='mes', aggfunc='mean')

# ── 6. Serie temporal mensual ─────────────────────────────────────────────
dfh['periodo'] = dfh['ts'].dt.to_period('M').astype(str)
mensual = dfh.groupby('periodo')['n'].sum().reset_index()
mensual.columns = ['periodo','total']

# ── Construccion del dashboard ─────────────────────────────────────────────
COLOR_HORA  = '#2196F3'
COLOR_DIA   = '#FF5722'
COLOR_MES   = '#4CAF50'
COLOR_TREND = '#9C27B0'

fig = make_subplots(
    rows=3, cols=3,
    subplot_titles=[
        '<b>Promedio de emergencias por hora del dia</b>',
        '<b>Promedio por dia de la semana</b>',
        '<b>Promedio por mes</b>',
        '<b>Heatmap: hora × dia de semana</b>',
        '<b>Heatmap: hora × mes</b>',
        '<b>Tendencia mensual (total)</b>',
        '<b>Distribucion de carga (hora del dia)</b>',
        '<b>% emergencias por franja horaria</b>',
        '<b>Comparativa fin de semana vs laboral</b>',
    ],
    vertical_spacing=0.12,
    horizontal_spacing=0.08,
)

# ── Fila 1 ────────────────────────────────────────────────────────────────
# 1a. Barras por hora
fig.add_trace(go.Bar(
    x=por_hora['hora'], y=por_hora['media'],
    error_y=dict(type='data', array=por_hora['std']/np.sqrt(len(dfh['anio'].unique())*365), visible=True),
    marker_color=COLOR_HORA, name='media/hora',
    hovertemplate='%{x}:00h — %{y:.3f} emerg/h<extra></extra>',
), row=1, col=1)

# 1b. Barras por dia semana
fig.add_trace(go.Bar(
    x=por_dia['dia_lbl'], y=por_dia['media'],
    marker_color=[COLOR_DIA if d < 5 else '#FF9800' for d in por_dia['dia_semana']],
    name='media/dia',
    hovertemplate='%{x}: %{y:.3f} emerg/h<extra></extra>',
), row=1, col=2)

# 1c. Barras por mes con color de calor
colores_mes = px.colors.sequential.YlOrRd
step = max(1, len(colores_mes) // 12)
fig.add_trace(go.Bar(
    x=por_mes['mes_lbl'], y=por_mes['media'],
    marker_color=[colores_mes[min(i*step, len(colores_mes)-1)] for i in range(12)],
    name='media/mes',
    hovertemplate='%{x}: %{y:.3f} emerg/h<extra></extra>',
), row=1, col=3)

# ── Fila 2 ────────────────────────────────────────────────────────────────
# 2a. Heatmap hora x dia
fig.add_trace(go.Heatmap(
    z=pivot_hd.values,
    x=[DIAS[d] for d in pivot_hd.columns],
    y=[f'{h:02d}:00' for h in pivot_hd.index],
    colorscale='YlOrRd',
    colorbar=dict(title='emerg/h', x=0.32, len=0.28, y=0.5),
    hovertemplate='%{x} %{y}: %{z:.3f} emerg/h<extra></extra>',
    name='heatmap dia',
), row=2, col=1)

# 2b. Heatmap hora x mes
fig.add_trace(go.Heatmap(
    z=pivot_hm.values,
    x=[MESES[m-1] for m in pivot_hm.columns],
    y=[f'{h:02d}:00' for h in pivot_hm.index],
    colorscale='Blues',
    colorbar=dict(title='emerg/h', x=0.655, len=0.28, y=0.5),
    hovertemplate='%{x} %{y}: %{z:.3f} emerg/h<extra></extra>',
    name='heatmap mes',
), row=2, col=2)

# 2c. Tendencia mensual
fig.add_trace(go.Scatter(
    x=mensual['periodo'], y=mensual['total'],
    mode='lines+markers',
    line=dict(color=COLOR_TREND, width=2),
    marker=dict(size=5),
    fill='tozeroy', fillcolor='rgba(156,39,176,0.12)',
    name='total mensual',
    hovertemplate='%{x}: %{y} emergencias<extra></extra>',
), row=2, col=3)

# ── Fila 3 ────────────────────────────────────────────────────────────────
# 3a. Boxplot por hora (distribucion real)
for h in range(24):
    vals = dfh.loc[dfh['hora'] == h, 'n'].values
    fig.add_trace(go.Box(
        y=vals, name=f'{h:02d}h',
        marker_color=COLOR_HORA, line_color=COLOR_HORA,
        showlegend=False, boxpoints=False,
        hovertemplate=f'{h:02d}:00 — mediana: %{{median:.2f}}<extra></extra>',
    ), row=3, col=1)

# 3b. Pie de franjas horarias
franjas = {
    'Madrugada (00-05)': dfh[dfh['hora'] < 6]['n'].sum(),
    'Manana (06-11)':     dfh[(dfh['hora'] >= 6)  & (dfh['hora'] < 12)]['n'].sum(),
    'Tarde (12-17)':      dfh[(dfh['hora'] >= 12) & (dfh['hora'] < 18)]['n'].sum(),
    'Noche (18-23)':      dfh[dfh['hora'] >= 18]['n'].sum(),
}
fig.add_trace(go.Pie(
    labels=list(franjas.keys()),
    values=list(franjas.values()),
    hole=0.4,
    marker_colors=['#1565C0','#F9A825','#EF6C00','#283593'],
    textinfo='label+percent',
    name='franjas',
    hovertemplate='%{label}: %{value} emerg (%{percent})<extra></extra>',
), row=3, col=2)

# 3c. Laboral vs fin de semana por hora
lab  = dfh[dfh['dia_semana'] < 5].groupby('hora')['n'].mean()
fds  = dfh[dfh['dia_semana'] >= 5].groupby('hora')['n'].mean()
fig.add_trace(go.Scatter(x=lab.index, y=lab.values, name='Dias laborales',
                         line=dict(color='#1976D2', width=2.5),
                         hovertemplate='%{x}:00h: %{y:.3f}<extra></extra>'), row=3, col=3)
fig.add_trace(go.Scatter(x=fds.index, y=fds.values, name='Fin de semana',
                         line=dict(color='#FF5722', width=2.5, dash='dot'),
                         hovertemplate='%{x}:00h: %{y:.3f}<extra></extra>'), row=3, col=3)

# ── Layout global ──────────────────────────────────────────────────────────
total_emerg = int(dfh['n'].sum())
rango_ini   = dfh['ts'].min().strftime('%Y-%m-%d')
rango_fin   = dfh['ts'].max().strftime('%Y-%m-%d')

fig.update_layout(
    title=dict(
        text=(f'<b>Emergencias CBT Talcahuano</b>  |  '
              f'{total_emerg:,} emergencias  |  {rango_ini} → {rango_fin}'),
        font=dict(size=17),
        x=0.5,
    ),
    height=1100,
    plot_bgcolor='#FAFAFA',
    paper_bgcolor='white',
    font=dict(family='Arial', size=11),
    showlegend=True,
    legend=dict(orientation='h', y=-0.04, x=0.5, xanchor='center'),
)

# Ejes
fig.update_xaxes(title_text='Hora', row=1, col=1, dtick=2)
fig.update_xaxes(row=1, col=2, tickangle=-30)
fig.update_xaxes(row=1, col=3, tickangle=-30)
fig.update_yaxes(title_text='emerg/hora promedio', row=1, col=1)
fig.update_yaxes(title_text='emerg/hora promedio', row=1, col=2)
fig.update_yaxes(title_text='emerg/hora promedio', row=1, col=3)
fig.update_yaxes(title_text='Hora del dia', row=2, col=1)
fig.update_yaxes(title_text='Hora del dia', row=2, col=2)
fig.update_yaxes(title_text='Total emergencias', row=2, col=3)
fig.update_xaxes(title_text='Hora', row=3, col=1, dtick=2)
fig.update_xaxes(title_text='Hora del dia', row=3, col=3, dtick=2)
fig.update_yaxes(title_text='N emergencias', row=3, col=1)
fig.update_yaxes(title_text='emerg/hora promedio', row=3, col=3)
fig.update_xaxes(row=2, col=3, tickangle=-45)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resumen_emergencias.html')
fig.write_html(OUT, include_plotlyjs='cdn')
log(f"Guardado: {OUT}")

# ── Estadisticas texto ─────────────────────────────────────────────────────
hora_pico = por_hora.loc[por_hora['media'].idxmax(), 'hora']
dia_pico  = DIAS[int(por_dia.loc[por_dia['media'].idxmax(), 'dia_semana'])]
mes_pico  = MESES[int(por_mes.loc[por_mes['media'].idxmax(), 'mes']) - 1]
hora_min  = por_hora.loc[por_hora['media'].idxmin(), 'hora']

log(f"\n=== ESTADISTICAS CLAVE ===")
log(f"Total emergencias:        {total_emerg:,}")
log(f"Media global:             {dfh['n'].mean():.4f} emerg/hora")
log(f"Hora pico:                {hora_pico:02d}:00h ({por_hora.loc[por_hora['hora']==hora_pico,'media'].values[0]:.3f} emerg/h)")
log(f"Hora mas tranquila:       {hora_min:02d}:00h ({por_hora.loc[por_hora['hora']==hora_min,'media'].values[0]:.3f} emerg/h)")
log(f"Dia mas activo:           {dia_pico}")
log(f"Mes mas activo:           {mes_pico}")
log(f"% horas con emerg:        {(dfh['n']>0).mean()*100:.1f}%")
log(f"Franja de mayor carga:    {max(franjas, key=franjas.get)}")

LOG.close()
print("OK - abrir resumen_emergencias.html")
