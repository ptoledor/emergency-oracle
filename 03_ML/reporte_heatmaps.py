"""
Reporte de Heatmaps Mensuales - CBT Talcahuano
Migración de esquema por temporada a esquema por mes (1 + 12 heatmaps).
"""
import os
import sys
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Rutas
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, '..', 'dataset_final_ml.csv')
OUTPUT_PATH = os.path.join(BASE_DIR, 'heatmaps_detallados.html')

LOG_FILE = os.path.join(BASE_DIR, 'heatmaps_log.txt')
_log_fh = open(LOG_FILE, 'w', encoding='utf-8')
def log(m): print(m); _log_fh.write(m + '\n'); _log_fh.flush()

# Nombres en español
MESES = ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']
DIAS  = ['Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sab', 'Dom']

try:
    log("=== Iniciando Generación de Heatmaps Mensuales ===")
    
    # 1. Cargar datos
    df = pd.read_csv(DATA_PATH)
    
    # 1.5 Compilados Totales
    compilado_dia_sem = df.groupby('dia_semana')['n_emergencias'].mean().reindex(range(0, 7))
    compilado_mes     = df.groupby('mes')['n_emergencias'].mean().reindex(range(1, 13))
    compilado_dia_mes = df.groupby('dia_mes')['n_emergencias'].mean().reindex(range(1, 32))
    pivot_global = df.pivot_table(index='mes', columns='dia_semana', values='n_emergencias', aggfunc='mean')
    pivot_global = pivot_global.reindex(index=range(1, 13), columns=range(0, 7))

    # 3. Heatmaps Detallados: Hora vs Día de la Semana (uno por mes)
    monthly_pivots = {}
    for m in range(1, 13):
        subset = df[df['mes'] == m]
        if not subset.empty:
            p = subset.pivot_table(index='hora', columns='dia_semana', values='n_emergencias', aggfunc='mean')
            p = p.reindex(index=range(0, 24), columns=range(0, 7))
            monthly_pivots[m] = p
        else:
            monthly_pivots[m] = pd.DataFrame(np.nan, index=range(0, 24), columns=range(0, 7))

    log("Pivots calculados.")

    # ─── Construccion del Dashboard ─────────────────────────────────────────────
    
    # Grid: 1 Fila para 3 compilados + 1 Fila para el resumen + 4 filas de 3 columnas para los 12 meses
    titles = ['<b>Por Día Semana (Prom)</b>', '<b>Por Mes (Prom)</b>', '<b>Por Día del Mes (1-31)</b>']
    titles += ['<b>Carga Promedio: Día de la Semana vs Mes</b>', '', '']
    titles += [f'<b>{m}</b>' for m in MESES]
    
    fig = make_subplots(
        rows=6, cols=3,
        subplot_titles=titles,
        vertical_spacing=0.035,
        horizontal_spacing=0.05,
        specs=[
            [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy", "colspan": 3}, None, None],
            [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}],
            [{"type": "xy"}, {"type": "xy"}, {"type": "xy"}]
        ]
    )

    colorscale_gr = [[0, 'green'], [0.5, 'yellow'], [1, 'red']]

    # 1. Compilado por Día Semana
    fig.add_trace(go.Bar(
        x=DIAS, y=compilado_dia_sem.values,
        marker=dict(color=compilado_dia_sem.values, colorscale=colorscale_gr, showscale=False),
        name='Media Día Sem'
    ), row=1, col=1)

    # 2. Compilado por Mes
    fig.add_trace(go.Bar(
        x=MESES, y=compilado_mes.values,
        marker=dict(color=compilado_mes.values, colorscale=colorscale_gr, showscale=False),
        name='Media Mes'
    ), row=1, col=2)

    # 3. Compilado por Día Mes
    fig.add_trace(go.Bar(
        x=compilado_dia_mes.index, y=compilado_dia_mes.values,
        marker=dict(color=compilado_dia_mes.values, colorscale=colorscale_gr, showscale=False),
        name='Media Día Mes'
    ), row=1, col=3)

    # 4. Heatmap Global
    fig.add_trace(go.Heatmap(
        z=pivot_global.values,
        x=DIAS,
        y=MESES,
        colorscale=colorscale_gr,
        colorbar=dict(title='Emerg/Hora', x=1.01, len=0.2, y=0.85),
    ), row=2, col=1)

    # 3-14. Heatmaps Mensuales
    month_index = 0
    for r in range(3, 7):
        for c in range(1, 4):
            m = month_index + 1
            p = monthly_pivots[m]
            fig.add_trace(go.Heatmap(
                z=p.values,
                x=DIAS,
                y=[f'{h:02d}' for h in p.index],
                colorscale=colorscale_gr,
                showscale=False,
                hovertemplate=f'<b>{MESES[month_index]}</b><br>Día: %{{x}}<br>Hora: %{{y}}:00<br>Promedio: %{{z:.3f}}<extra></extra>'
            ), row=r, col=c)
            month_index += 1

    # Ajustes de Layout
    fig.update_layout(
        title=dict(text='<b>Reporte de Estacionalidad Mensual Detallada</b>', x=0.5, font=dict(size=26)),
        height=2200,
        template='plotly_white',
    )

    # Ejes
    fig.update_yaxes(title_text='Emerg/h (Media)', row=1, col=1)
    fig.update_yaxes(autorange='reversed', row=2, col=1)
    for r in range(3, 7):
        for c in range(1, 4):
            fig.update_yaxes(autorange='reversed', row=r, col=c)

    fig.write_html(OUTPUT_PATH, include_plotlyjs='cdn')
    log(f"Reporte mensual guardado en: {OUTPUT_PATH}")

except Exception as e:
    log(f"ERROR: {e}")
    import traceback
    log(traceback.format_exc())
finally:
    _log_fh.close()
