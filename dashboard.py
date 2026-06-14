import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import pickle
import os
import requests
import datetime
import holidays
from pathlib import Path

# 1. Configuración de página
st.set_page_config(
    page_title="CBT Oracle - Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# 2. Rutas bases
base_dir = Path(__file__).resolve().parent
data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
models_dir = base_dir / "03_model" / "saved_models"

# 3. Estado de Tema (Claro / Oscuro)
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

def toggle_theme():
    st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"

IS_DARK = st.session_state.theme == "dark"

# 4. Paleta de colores dinámica
bg = "#09090b" if IS_DARK else "#ffffff"
bg_subtle = "#0c0c0f" if IS_DARK else "#f9fafb"
card = "#0c0c0f" if IS_DARK else "#ffffff"
card_hover = "#131316" if IS_DARK else "#f4f4f5"
border = "#1e1e24" if IS_DARK else "#e4e4e7"
border_subtle = "#16161a" if IS_DARK else "#f0f0f2"
text = "#fafafa" if IS_DARK else "#09090b"
text_muted = "#a1a1aa" if IS_DARK else "#71717a"
text_dim = "#52525b" if IS_DARK else "#a1a1aa"
shadow = "none" if IS_DARK else "0 1px 3px rgba(0,0,0,0.04), 0 1px 2px rgba(0,0,0,0.03)"
green = "#22c55e" if IS_DARK else "#16a34a"
green_muted = "rgba(34,197,94,0.12)" if IS_DARK else "rgba(22,163,74,0.08)"
red = "#ef4444" if IS_DARK else "#dc2626"
red_muted = "rgba(239,68,68,0.12)" if IS_DARK else "rgba(220,38,38,0.08)"

# 5. Inyección de CSS de diseño
css = f"""
<style>
    :root {{
        --bg: {bg};
        --bg-subtle: {bg_subtle};
        --card: {card};
        --card-hover: {card_hover};
        --border: {border};
        --border-subtle: {border_subtle};
        --text: {text};
        --text-muted: {text_muted};
        --text-dim: {text_dim};
        --accent: #2563eb;
        --shadow: {shadow};
        --radius: 10px;
        --green: {green};
        --green-muted: {green_muted};
        --red: {red};
        --red-muted: {red_muted};
    }}
    
    /* Ocultar elementos de Streamlit */
    header[data-testid="stHeader"], #MainMenu, footer, [data-testid="stToolbar"],
    [data-testid="stDecoration"], [data-testid="stStatusWidget"], .stDeployButton,
    div[data-testid="stSidebarCollapsedControl"] {{
        display: none !important;
    }}
    
    * {{
        overflow-anchor: none !important;
    }}
    
    /* Configuración global del contenedor */
    html, body, [data-testid="stAppViewContainer"], [data-testid="stApp"], .main, .block-container, section[data-testid="stMain"] {{
        background-color: var(--bg) !important;
        color: var(--text) !important;
        font-family: 'DM Sans', -apple-system, sans-serif !important;
        overflow-anchor: none !important;
    }}
    .block-container {{
        padding: 1.5rem 2rem 2rem !important;
        max-width: 1300px !important;
    }}
    
    /* Pestañas (Pill-style) */
    button[data-baseweb="tab"] {{
        background: transparent !important;
        color: var(--text-muted) !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        padding: 0.6rem 1.2rem !important;
        border: 1px solid transparent !important;
        border-radius: 8px !important;
        transition: all 0.2s ease !important;
    }}
    button[data-baseweb="tab"]:hover {{
        color: var(--text) !important;
        background: var(--card-hover) !important;
    }}
    button[data-baseweb="tab"][aria-selected="true"] {{
        color: var(--text) !important;
        background: var(--card) !important;
        border-color: var(--border) !important;
        box-shadow: var(--shadow) !important;
    }}
    [data-baseweb="tab-highlight"], [data-baseweb="tab-border"] {{
        display: none !important;
    }}
    [data-baseweb="tab-list"] {{
        gap: 6px !important;
        background: var(--bg-subtle) !important;
        border: 1px solid var(--border) !important;
        border-radius: 12px !important;
        padding: 4px !important;
        margin-bottom: 1.5rem !important;
    }}
    
    /* KPI Cards */
    .metric-card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 1.2rem 1.4rem;
        box-shadow: var(--shadow);
        display: flex;
        flex-direction: column;
        justify-content: center;
        min-height: 100px;
    }}
    .metric-label {{
        font-size: 0.76rem;
        color: var(--text-muted);
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.3rem;
    }}
    .metric-value {{
        font-size: 1.85rem;
        font-weight: 700;
        color: var(--text);
        letter-spacing: -0.03em;
        line-height: 1.2;
    }}
    .metric-delta {{
        font-size: 0.72rem;
        font-weight: 600;
        margin-top: 0.4rem;
        padding: 2px 8px;
        border-radius: 6px;
        display: inline-flex;
        align-items: center;
        gap: 3px;
        width: fit-content;
    }}
    .delta-up {{ color: var(--green); background: var(--green-muted); }}
    .delta-down {{ color: var(--red); background: var(--red-muted); }}
    
    /* Contenedores de gráficos basados en st.container */
    div[data-testid="stVerticalBlock"]:has(.chart-anchor):not(:has(div[data-testid="stVerticalBlock"])),
    div[data-testid="stVerticalBlock"]:has(.prediction-anchor):not(:has(div[data-testid="stVerticalBlock"])) {{
        background: var(--card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        padding: 1.3rem !important;
        box-shadow: var(--shadow) !important;
        margin-bottom: 1.25rem !important;
        min-height: 580px !important;
    }}
    
    div[data-testid="stVerticalBlock"]:has(.importance-anchor):not(:has(div[data-testid="stVerticalBlock"])) {{
        background: var(--card) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        padding: 1.3rem !important;
        box-shadow: var(--shadow) !important;
        margin-bottom: 1.25rem !important;
    }}
    .chart-title {{
        font-size: 0.95rem;
        font-weight: 700;
        color: var(--text);
        letter-spacing: -0.01em;
    }}
    .chart-subtitle {{
        font-size: 0.75rem;
        color: var(--text-muted);
        margin-bottom: 1.2rem;
    }}
    
    /* Encabezado */
    .header-container {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1.5rem;
        border-bottom: 1px solid var(--border);
        padding-bottom: 0.75rem;
    }}
    .brand-name {{
        font-size: 1.25rem;
        font-weight: 800;
        color: var(--text);
        letter-spacing: -0.02em;
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    .brand-sub {{
        font-size: 0.75rem;
        color: var(--text-muted);
        font-weight: 400;
        margin-left: 5px;
    }}
    
    /* Separación de columnas */
    [data-testid="stHorizontalBlock"] {{
        gap: 1.25rem !important;
    }}
    
    /* Listas y recomendaciones */
    .idea-card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        padding: 1.2rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow);
    }}
    .idea-title {{
        font-size: 0.88rem;
        font-weight: 700;
        color: var(--text);
        margin-bottom: 0.4rem;
        display: flex;
        align-items: center;
        gap: 6px;
    }}
    .idea-desc {{
        font-size: 0.78rem;
        color: var(--text-muted);
        line-height: 1.45;
    }}
    .idea-tag {{
        font-size: 0.68rem;
        font-weight: 600;
        padding: 2px 6px;
        border-radius: 4px;
        background: var(--bg-subtle);
        border: 1px solid var(--border);
        color: var(--text-muted);
        margin-left: auto;
    }}
</style>
"""
st.markdown(css, unsafe_allow_html=True)

# 6. Funciones auxiliares para componentes
def metric_card(label, value, delta=None, delta_type="up"):
    delta_html = ""
    if delta:
        cls = "delta-up" if delta_type == "up" else "delta-down"
        arrow = "↑" if delta_type == "up" else "↓"
        delta_html = f'<div class="metric-delta {cls}">{arrow} {delta}</div>'
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

# 7. Carga de datos y modelos
@st.cache_data
def load_data_and_predict():
    if not os.path.exists(data_path):
        return None, None, None, None, None, None, None
    df = pd.read_csv(data_path, sep=';')
    
    # Cargar modelos y metadata
    try:
        # 1. Cargar modelo agnóstico base
        with open(models_dir / "regressor_agnostic.pkl", "rb") as f:
            reg_model_base = pickle.load(f)
        with open(models_dir / "metadata_agnostic.pkl", "rb") as f:
            metadata_base = pickle.load(f)
        
        # Realizar predicciones históricas base
        X_base = df[metadata_base['feature_cols']]
        df['PRED_EVENTOS_BASE'] = reg_model_base.predict(X_base)
        
        # Extraer importancia base
        importances_base = reg_model_base.feature_importances_
        df_imp_base = pd.DataFrame({
            'Feature': metadata_base['feature_cols'],
            'Importance': importances_base
        }).sort_values(by='Importance', ascending=True) # Ascendente para barra horizontal
        
        # 2. Cargar modelo agnóstico aumentado
        with open(models_dir / "regressor_agnostic_augmented.pkl", "rb") as f:
            reg_model_aug = pickle.load(f)
        with open(models_dir / "metadata_agnostic_augmented.pkl", "rb") as f:
            metadata_aug = pickle.load(f)
            
        # Realizar predicciones históricas aumentadas
        X_aug = df[metadata_aug['feature_cols']]
        df['PRED_EVENTOS_AUGMENTED'] = reg_model_aug.predict(X_aug)
        
        # Extraer importancia aumentada
        importances_aug = reg_model_aug.feature_importances_
        df_imp_aug = pd.DataFrame({
            'Feature': metadata_aug['feature_cols'],
            'Importance': importances_aug
        }).sort_values(by='Importance', ascending=True)
        
        # 3. Cargar modelo agnóstico aumentado v3
        with open(models_dir / "regressor_agnostic_augmented_v3.pkl", "rb") as f:
            reg_model_v3 = pickle.load(f)
        with open(models_dir / "metadata_agnostic_augmented_v3.pkl", "rb") as f:
            metadata_v3 = pickle.load(f)
            
        # Realizar predicciones históricas aumentadas v3
        X_v3 = df[metadata_v3['feature_cols']]
        df['PRED_EVENTOS_AUGMENTED_V3'] = reg_model_v3.predict(X_v3)
        
        # Extraer importancia aumentada v3
        importances_v3 = reg_model_v3.feature_importances_
        df_imp_v3 = pd.DataFrame({
            'Feature': metadata_v3['feature_cols'],
            'Importance': importances_v3
        }).sort_values(by='Importance', ascending=True)
        
        # Extraer día del año
        df['FECHA_DT'] = pd.to_datetime(df['FECHA_DIA'])
        df['DIA_DEL_ANO'] = df['FECHA_DT'].dt.dayofyear
        
        return df, df_imp_base, df_imp_aug, df_imp_v3, metadata_base, metadata_aug, metadata_v3
    except Exception as e:
        st.error(f"Error al cargar modelos: {e}")
        return None, None, None, None, None, None, None

df, df_imp_base, df_imp_aug, df_imp_v3, metadata_base, metadata_aug, metadata_v3 = load_data_and_predict()

# Helper to fetch weather series from Open-Meteo
@st.cache_data(ttl=3600, show_spinner=False)
def get_weather_for_range(start_date, is_historical):
    lat, lon = -36.731106, -73.11023
    if is_historical:
        # Simulation mode: target range start_date to start_date + 6 days.
        # We need weather from start_date - 7 days to start_date + 6 days for lags
        q_start = start_date - datetime.timedelta(days=7)
        q_end = start_date + datetime.timedelta(days=6)
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={q_start.strftime('%Y-%m-%d')}&"
               f"end_date={q_end.strftime('%Y-%m-%d')}&"
               f"hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&"
               f"timezone=America%2FSantiago&format=json")
    else:
        # Real-time mode: start_date is tomorrow.
        # We fetch forecast API with past_days=7 to automatically get past 7 days and next 7 days
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&"
               f"hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&"
               f"timezone=America%2FSantiago&past_days=7&forecast_days=10")
               
    res = requests.get(url, timeout=30)
    if res.status_code != 200:
        raise RuntimeError(f"Error al descargar clima desde Open-Meteo: {res.text}")
    return res.json()['hourly']

# Recursive forecasting function for 7 days
def predict_7_days_recursive(start_date, is_historical, prefix="_agnostic_augmented", weather_data=None):
    # Load models and metadata
    try:
        with open(models_dir / f"regressor{prefix}.pkl", "rb") as f:
            reg_model = pickle.load(f)
        with open(models_dir / f"classifier{prefix}.pkl", "rb") as f:
            clf_model = pickle.load(f)
        with open(models_dir / f"metadata{prefix}.pkl", "rb") as f:
            metadata = pickle.load(f)
    except Exception as e:
        st.error(f"Error al cargar modelos: {e}")
        return None, 0.25, 7.0

    # Get weather
    try:
        if weather_data is None:
            weather_data = get_weather_for_range(start_date, is_historical)
    except Exception as e:
        st.error(f"Error al descargar pronóstico del clima: {e}")
        return None, 0.25, 7.0

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

    weather_hourly = pd.DataFrame(weather_data)
    weather_hourly['time'] = pd.to_datetime(weather_hourly['time'])
    weather_hourly['FECHA_DIA'] = weather_hourly['time'].dt.date

    # Group by FECHA_DIA to aggregate hourly data daily
    weather_df = weather_hourly.groupby('FECHA_DIA').agg(
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
    )
    
    # Initialize event history for previous 7 days
    event_history = {}
    for i in range(1, 8):
        prev_date = start_date - datetime.timedelta(days=i)
        match = df[df['FECHA_DIA'] == prev_date.strftime('%Y-%m-%d')]
        if not match.empty:
            event_history[prev_date] = float(match.iloc[0]['EVENTOS'])
        else:
            event_history[prev_date] = 5.46 # training mean baseline
            
    # Category lags from D1 - 1
    prev_date_1 = start_date - datetime.timedelta(days=1)
    match_1 = df[df['FECHA_DIA'] == prev_date_1.strftime('%Y-%m-%d')]
    if not match_1.empty:
        cat_lags = {
            'N_INCENDIO_ESTR_lag_1': float(match_1.iloc[0].get('N_INCENDIO_ESTR_lag_1', 0.0)),
            'N_INCENDIO_FOREST_lag_1': float(match_1.iloc[0].get('N_INCENDIO_FOREST_lag_1', 0.0)),
            'N_RESCATE_VEH_lag_1': float(match_1.iloc[0].get('N_RESCATE_VEH_lag_1', 0.0)),
            'N_RESCATE_PERS_lag_1': float(match_1.iloc[0].get('N_RESCATE_PERS_lag_1', 0.0)),
            'N_GASES_lag_1': float(match_1.iloc[0].get('N_GASES_lag_1', 0.0)),
        }
    else:
        cat_lags = {
            'N_INCENDIO_ESTR_lag_1': 0.0,
            'N_INCENDIO_FOREST_lag_1': 0.0,
            'N_RESCATE_VEH_lag_1': 0.0,
            'N_RESCATE_PERS_lag_1': 0.0,
            'N_GASES_lag_1': 0.0,
        }
        
    predictions = []
    chile_holidays = holidays.Chile(years=[start_date.year, (start_date + datetime.timedelta(days=6)).year])
    
    DIAS_ES = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
        'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    
    for j in range(7):
        d = start_date + datetime.timedelta(days=j)
        d_str = d.strftime('%Y-%m-%d')
        
        # Calendar features
        mes = d.month
        dia_semana = d.weekday()
        es_fin_semana = 1 if dia_semana in [5, 6] else 0
        es_feriado = 1 if d_str in chile_holidays else 0
        
        feriados_irrenunciables = {(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)}
        holiday_name = chile_holidays.get(d_str)
        es_feriado_irrenunciable = 0
        if (mes, d.day) in feriados_irrenunciables:
            es_feriado_irrenunciable = 1
        elif holiday_name and ("elecciones" in holiday_name.lower() or "plebiscito" in holiday_name.lower()):
            es_feriado_irrenunciable = 1
            
        dia_del_ano = d.timetuple().tm_yday
        
        # Cyclic encodings
        mes_sin = np.sin(2 * np.pi * mes / 12)
        mes_cos = np.cos(2 * np.pi * mes / 12)
        dia_sin = np.sin(2 * np.pi * dia_semana / 7)
        dia_cos = np.cos(2 * np.pi * dia_semana / 7)
        dano_sin = np.sin(2 * np.pi * dia_del_ano / 365)
        dano_cos = np.cos(2 * np.pi * dia_del_ano / 365)
        
        # Weather features
        if d not in weather_df.index:
            st.error(f"La fecha {d_str} no tiene datos de clima disponibles.")
            return None, 0.25, 7.0
        w_d = weather_df.loc[d]
        temp_max = float(w_d['TEMP_MAX'])
        temp_min = float(w_d['TEMP_MIN'])
        temp_media = float(w_d['TEMP_MEDIA'])
        temp_skew = float(w_d['TEMP_SKEW'])
        temp_kurt = float(w_d['TEMP_KURT'])
        hum_max = float(w_d['HUM_MAX'])
        hum_min = float(w_d['HUM_MIN'])
        hum_media = float(w_d['HUM_MEDIA'])
        hum_skew = float(w_d['HUM_SKEW'])
        hum_kurt = float(w_d['HUM_KURT'])
        viento_max = float(w_d['VIENTO_MAX'])
        viento_medio = float(w_d['VIENTO_MEDIO'])
        viento_skew = float(w_d['VIENTO_SKEW'])
        viento_kurt = float(w_d['VIENTO_KURT'])
        lluvia = float(w_d['LLUVIA'])
        
        # Weather lags
        viento_medio_lag_1 = float(weather_df.loc[d - datetime.timedelta(days=1)]['VIENTO_MEDIO'])
        hum_media_lag_1 = float(weather_df.loc[d - datetime.timedelta(days=1)]['HUM_MEDIA'])
        lluvia_lag_1 = float(weather_df.loc[d - datetime.timedelta(days=1)]['LLUVIA'])
        lluvia_lag_2 = float(weather_df.loc[d - datetime.timedelta(days=2)]['LLUVIA'])
        lluvia_lag_3 = float(weather_df.loc[d - datetime.timedelta(days=3)]['LLUVIA'])
        lluvia_accum_3d = lluvia_lag_1 + lluvia_lag_2 + lluvia_lag_3
        
        lluvias_7d = [float(weather_df.loc[d - datetime.timedelta(days=i)]['LLUVIA']) for i in range(1, 8)]
        lluvia_rolling_mean_7d = float(np.mean(lluvias_7d))

        # Event lags
        eventos_lag_1 = float(event_history[d - datetime.timedelta(days=1)])
        eventos_lag_2 = float(event_history[d - datetime.timedelta(days=2)])
        eventos_lag_3 = float(event_history[d - datetime.timedelta(days=3)])
        eventos_lag_7 = float(event_history[d - datetime.timedelta(days=7)])
        
        ev_rolling_3d = [float(event_history[d - datetime.timedelta(days=i)]) for i in range(1, 4)]
        eventos_rolling_mean_3d = float(np.mean(ev_rolling_3d))
        eventos_rolling_std_3d = float(np.std(ev_rolling_3d))
        eventos_rolling_max_3d = float(np.max(ev_rolling_3d))
        
        ev_rolling_7d = [float(event_history[d - datetime.timedelta(days=i)]) for i in range(1, 8)]
        eventos_rolling_mean_7d = float(np.mean(ev_rolling_7d))
        eventos_rolling_std_7d = float(np.std(ev_rolling_7d))
        eventos_rolling_max_7d = float(np.max(ev_rolling_7d))
        
        # Construct feature vector
        features = {
            'TEMP_MAX': temp_max,
            'TEMP_MIN': temp_min,
            'TEMP_MEDIA': temp_media,
            'TEMP_SKEW': temp_skew,
            'TEMP_KURT': temp_kurt,
            'HUM_MAX': hum_max,
            'HUM_MIN': hum_min,
            'HUM_MEDIA': hum_media,
            'HUM_SKEW': hum_skew,
            'HUM_KURT': hum_kurt,
            'VIENTO_MAX': viento_max,
            'VIENTO_MEDIO': viento_medio,
            'VIENTO_SKEW': viento_skew,
            'VIENTO_KURT': viento_kurt,
            'LLUVIA': lluvia,
            'EVENTOS_lag_1': eventos_lag_1,
            'EVENTOS_lag_2': eventos_lag_2,
            'EVENTOS_lag_3': eventos_lag_3,
            'EVENTOS_lag_7': eventos_lag_7,
            'N_INCENDIO_ESTR_lag_1': cat_lags['N_INCENDIO_ESTR_lag_1'],
            'N_INCENDIO_FOREST_lag_1': cat_lags['N_INCENDIO_FOREST_lag_1'],
            'N_RESCATE_VEH_lag_1': cat_lags['N_RESCATE_VEH_lag_1'],
            'N_RESCATE_PERS_lag_1': cat_lags['N_RESCATE_PERS_lag_1'],
            'N_GASES_lag_1': cat_lags['N_GASES_lag_1'],
            'EVENTOS_rolling_mean_3d': eventos_rolling_mean_3d,
            'EVENTOS_rolling_std_3d': eventos_rolling_std_3d,
            'EVENTOS_rolling_max_3d': eventos_rolling_max_3d,
            'EVENTOS_rolling_mean_7d': eventos_rolling_mean_7d,
            'EVENTOS_rolling_std_7d': eventos_rolling_std_7d,
            'EVENTOS_rolling_max_7d': eventos_rolling_max_7d,
            'LLUVIA_lag_1': lluvia_lag_1,
            'LLUVIA_lag_2': lluvia_lag_2,
            'LLUVIA_lag_3': lluvia_lag_3,
            'LLUVIA_accum_3d': lluvia_accum_3d,
            'LLUVIA_rolling_mean_7d': lluvia_rolling_mean_7d,
            'VIENTO_MEDIO_lag_1': viento_medio_lag_1,
            'HUM_MEDIA_lag_1': hum_media_lag_1,
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
        
        # DataFrame aligned with model features
        X_pred = pd.DataFrame([features])[metadata['feature_cols']]
        
        # Predict
        pred_count = float(reg_model.predict(X_pred)[0])
        prob_high = float(clf_model.predict_proba(X_pred)[0, 1])
        
        # Update event history for subsequent recursive days
        event_history[d] = pred_count
        
        predictions.append({
            'Fecha': d,
            'FechaStr': d_str,
            'Día': DIAS_ES[d.strftime('%A')],
            'Predicción': pred_count,
            'Prob_Alta': prob_high,
            'Temp_Max': temp_max,
            'Viento_Medio': viento_medio,
            'Lluvia': lluvia,
            'Es_Feriado': es_feriado
        })
        
    return predictions, float(metadata.get('classification_threshold', 0.25)), float(metadata.get('umbral_alta_actividad', 7.0))

# 8. Encabezado principal de la aplicación

st.markdown(f"""
<div class="header-container">
    <div class="brand-name">
        🧯 Central CBT Oracle <span class="brand-sub">| Panel de Diagnóstico Estacional v2.1</span>
    </div>
</div>
""", unsafe_allow_html=True)

if df is None:
    st.warning("No se pudo cargar el dataset o los modelos. Por favor asegúrate de haber ejecutado los scripts de aumentación y entrenamiento de forma exitosa.")
    st.stop()

# 9. Sección de KPIs
mae_actual = float(metadata_aug['mae'])
mae_baseline = float(metadata_base['mae'])
mejora_mae = ((mae_baseline - mae_actual) / mae_baseline) * 100

mean_real = df['EVENTOS'].mean()
mean_pred = df['PRED_EVENTOS_AUGMENTED'].mean() if 'PRED_EVENTOS_AUGMENTED' in df.columns else df['EVENTOS'].mean()

k1, k2, k3, k4 = st.columns(4)
with k1:
    metric_card("Eventos Diarios Reales (Media)", f"{mean_real:.2f}")
with k2:
    metric_card("Eventos Diarios Predichos (Media)", f"{mean_pred:.2f}")
with k3:
    metric_card("Error MAE (Clima + Inercia)", f"{mae_actual:.2f} ev", f"{mejora_mae:.1f}% vs base", "up")
with k4:
    metric_card("Error MAE (Modelo Base)", f"{mae_baseline:.2f} ev", "Modelo Base", "down")

# 10. Configuración de tema de Plotly
PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans, sans-serif", color="#fafafa" if IS_DARK else "#09090b", size=11),
    margin=dict(l=40, r=20, t=15, b=40),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    ),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        zerolinecolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        tickfont=dict(size=10),
    ),
    yaxis=dict(
        gridcolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        zerolinecolor="rgba(255,255,255,0.06)" if IS_DARK else "rgba(0,0,0,0.06)",
        tickfont=dict(size=10),
    ),
    hovermode="x",
)

# 11. Fragmento para aislar el re-run del slider y evitar saltos de scroll
@st.fragment
def render_seasonal_chart():
    # Selector de modelo para las curvas
    model_choice = st.radio(
        "Enfoque del Modelo a Graficar:",
        options=["Modelo Base", "Modelo Climático con Inercia de Actividad", "Modelo Climático"],
        index=1,
        horizontal=True,
        key="curve_model_choice",
        help="El Modelo Base excluye asimetrías/curtosis. El Modelo Climático con Inercia de Actividad incluye perfiles con lags de eventos. El Modelo Climático incluye perfiles de clima SIN lags ni rollings de eventos."
    )
    
    # Slider interactivo para ajustar la ventana de suavizado dinámicamente
    dias_suavizado = st.slider(
        "Ventana de Suavizado (Días de Media Móvil Centrada):",
        min_value=1,
        max_value=30,
        value=7,
        step=1,
        help="Seleccione 1 para ver los datos originales. Valores mayores suavizan más las curvas.",
        key="smoothing_slider_fragment"
    )
    
    if model_choice == "Modelo Climático con Inercia de Actividad":
        y_pred_col = 'PRED_EVENTOS_AUGMENTED'
    elif model_choice == "Modelo Climático":
        y_pred_col = 'PRED_EVENTOS_AUGMENTED_V3'
    else:
        y_pred_col = 'PRED_EVENTOS_BASE'

    
    # Agrupar por día del año y restringir a 365 días (excluyendo el día bisiesto 366 si existe)
    df_grouped = df.groupby('DIA_DEL_ANO')[['EVENTOS', y_pred_col]].mean().reset_index()
    df_grouped = df_grouped[df_grouped['DIA_DEL_ANO'] <= 365].copy()
    
    # Calcular límites Y fijos antes del suavizado para evitar reescalado del eje
    y_min = min(df_grouped['EVENTOS'].min(), df_grouped[y_pred_col].min())
    y_max = max(df_grouped['EVENTOS'].max(), df_grouped[y_pred_col].max())
    ymin_fixed = max(0.0, float(y_min) - 0.5)
    ymax_fixed = float(y_max) + 0.5

    # Proponer un año no bisiesto (2025) para mapear el eje X a fechas legibles
    fechas_eje = pd.date_range(start="2025-01-01", end="2025-12-31", freq="D")
    df_grouped['FECHA_EJE'] = fechas_eje
    
    # Suavizar curvas dinámicamente según la elección del slider
    if dias_suavizado > 1:
        y_real = df_grouped['EVENTOS'].rolling(dias_suavizado, min_periods=1, center=True).mean()
        y_pred = df_grouped[y_pred_col].rolling(dias_suavizado, min_periods=1, center=True).mean()
        suffix = f" (Suavizado {dias_suavizado}d)"
    else:
        y_real = df_grouped['EVENTOS']
        y_pred = df_grouped[y_pred_col]
        suffix = " (Original)"

    # Crear gráfico Plotly
    fig = go.Figure()
    
    # Línea de eventos reales
    fig.add_trace(go.Scatter(
        x=df_grouped['FECHA_EJE'],
        y=y_real,
        mode='lines',
        name=f'Eventos Reales{suffix}',
        line=dict(color='#fafafa' if IS_DARK else '#09090b', width=2.5),
        hovertemplate='Fecha: %{x|%d-%b}<br>Reales: %{y:.2f} eventos<extra></extra>'
    ))
    
    # Línea de predicción
    fig.add_trace(go.Scatter(
        x=df_grouped['FECHA_EJE'],
        y=y_pred,
        mode='lines',
        name=f'Eventos Predichos{suffix}',
        line=dict(color='#3b82f6', width=2.5, dash='dash'),
        hovertemplate='Fecha: %{x|%d-%b}<br>Predicho: %{y:.2f} eventos<extra></extra>'
    ))
    
    # Línea horizontal del error base / baseline prediction (media general de entrenamiento)
    fig.add_trace(go.Scatter(
        x=[df_grouped['FECHA_EJE'].iloc[0], df_grouped['FECHA_EJE'].iloc[-1]],
        y=[mean_real, mean_real],
        mode='lines',
        name=f'Predicción Base (Media: {mean_real:.2f})',
        line=dict(color='#ef4444', width=1.5, dash='dot'),
        hoverinfo='skip'
    ))
    
    # Línea vertical indicando el día actual (Hoy)
    today = datetime.date.today()
    try:
        today_fictional = datetime.datetime(2025, today.month, today.day)
    except ValueError:
        today_fictional = datetime.datetime(2025, 2, 28)
        
    fig.add_vline(
        x=today_fictional.strftime('%Y-%m-%d'),
        line_width=1.5,
        line_color="#ef4444",
        line_dash="dash"
    )
    
    # Anotación manual de "Hoy" para evitar el error TypeError interno de Plotly al calcular la media de strings
    fig.add_annotation(
        x=today_fictional.strftime('%Y-%m-%d'),
        y=ymax_fixed,
        text=f"Hoy ({today.strftime('%d-%b')})",
        showarrow=False,
        xanchor="center",
        yanchor="top",
        font=dict(color="#ef4444", size=10, family="DM Sans, sans-serif"),
        bgcolor="rgba(9, 9, 11, 0.85)" if IS_DARK else "rgba(255, 255, 255, 0.85)",
        bordercolor="#ef4444",
        borderwidth=1,
        borderpad=4
    )
    
    # Combinar PLOT_LAYOUT con la personalización del eje X y Y para evitar conflicto de argumentos
    layout_params = PLOT_LAYOUT.copy()
    layout_params['dragmode'] = 'zoom' # Activar modo zoom por defecto al arrastrar el mouse
    layout_params['xaxis'] = dict(
        **PLOT_LAYOUT['xaxis'],
        tickformat='%b',
        dtick="M1",
        fixedrange=False # Permitir zoom horizontal
    )
    layout_params['yaxis'] = dict(
        **PLOT_LAYOUT['yaxis'],
        range=[ymin_fixed, ymax_fixed],
        fixedrange=True # Bloquear zoom vertical (rango fijo)
    )
    
    fig.update_layout(
        **layout_params,
        xaxis_title="Mes del Año",
        yaxis_title="Cantidad de Eventos de Emergencia"
    )
    
    st.plotly_chart(
        fig, 
        use_container_width=True, 
        config={
            "displayModeBar": True,
            "displaylogo": False,
            "modeBarButtonsToRemove": ["lasso2d", "select2d", "zoomIn2d", "zoomOut2d", "autoScale2d"]
        }
    )

# 12. Pestañas de navegación
tab1, tab2, tab3, tab4 = st.tabs([
    "🔮 Predicciones Siguientes 7 Días",
    "⚡ Importancia de Variables",
    "📊 Curvas de Estacionalidad (365 días)",
    "💡 Recomendaciones de Mejora"
])

with tab1:
    st.markdown("""
    <div style="margin-bottom: 0.5rem;">
        <p style="font-size: 0.8rem; color: var(--text-muted); margin-bottom: 1rem;">
            Predicción recursiva diaria en tiempo real para planificar guardias preventivas y dotación de personal comparando los modelos.
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    is_historical_pred = False
    
    # Mañana real
    start_pred_date = datetime.date.today() + datetime.timedelta(days=1)
        
    with st.spinner("Consultando clima y simulando predicción recursiva con los modelos..."):
        try:
            shared_weather = get_weather_for_range(start_pred_date, is_historical_pred)
        except Exception as e:
            st.error(f"Error al descargar pronóstico del clima: {e}")
            shared_weather = None

        if shared_weather is None:
            pred_results = pred_results_base = pred_results_v3 = None
            clf_threshold = float(metadata_aug.get('classification_threshold', 0.25))
            umbral_alta = float(metadata_aug.get('umbral_alta_actividad', 7.0))
        else:
            pred_results, clf_threshold, umbral_alta = predict_7_days_recursive(
                start_pred_date, is_historical_pred, prefix="_agnostic_augmented", weather_data=shared_weather
            )
            pred_results_base, _, _ = predict_7_days_recursive(
                start_pred_date, is_historical_pred, prefix="_agnostic", weather_data=shared_weather
            )
            pred_results_v3, _, _ = predict_7_days_recursive(
                start_pred_date, is_historical_pred, prefix="_agnostic_augmented_v3", weather_data=shared_weather
            )
        
    if pred_results is not None and pred_results_base is not None and pred_results_v3 is not None:
        # Create container for card rendering
        with st.container():
            st.markdown('<div class="prediction-anchor"></div>', unsafe_allow_html=True)
            st.markdown('<div class="chart-title">Curva de Tendencia de Emergencias (Próximos 7 Días - Comparación de Modelos)</div>', unsafe_allow_html=True)
            
            # 1. Plotly chart for 7-day trend
            fig_7d = go.Figure()
            
            dates_7d = [p['Fecha'] for p in pred_results]
            counts_7d = [p['Predicción'] for p in pred_results]
            counts_7d_base = [p['Predicción'] for p in pred_results_base]
            counts_7d_v3 = [p['Predicción'] for p in pred_results_v3]
            
            # Hover text para modelo Climático con Inercia de Actividad
            hover_text_aug = [
                f"<b>Modelo Climático con Inercia de Actividad</b><br>"
                f"Fecha: {p['FechaStr']} ({p['Día']})<br>"
                f"Predicción: {p['Predicción']:.1f} llamadas<br>"
                f"Prob. Crítico: {p['Prob_Alta']*100:.0f}%<br>"
                f"Clima: {p['Temp_Max']:.1f}°C, {p['Viento_Medio']:.1f} km/h"
                for p in pred_results
            ]
            
            # Hover text para Modelo Base
            hover_text_base = [
                f"<b>Modelo Base</b><br>"
                f"Fecha: {p['FechaStr']} ({p['Día']})<br>"
                f"Predicción: {p['Predicción']:.1f} llamadas<br>"
                f"Prob. Crítico: {p['Prob_Alta']*100:.0f}%"
                for p in pred_results_base
            ]

            # Hover text para Modelo Climático
            hover_text_v3 = [
                f"<b>Modelo Climático (Puro)</b><br>"
                f"Fecha: {p['FechaStr']} ({p['Día']})<br>"
                f"Predicción: {p['Predicción']:.1f} llamadas<br>"
                f"Prob. Crítico: {p['Prob_Alta']*100:.0f}%"
                for p in pred_results_v3
            ]
            
            # Curva Climática con Inercia de Actividad (Esmeralda)
            fig_7d.add_trace(go.Scatter(
                x=dates_7d,
                y=counts_7d,
                mode='lines+markers',
                name='Modelo Climático con Inercia de Actividad',
                line=dict(color='#10b981', width=3),
                marker=dict(size=8, color='#10b981'),
                text=hover_text_aug,
                hoverinfo='text'
            ))
            
            # Curva Modelo Base (Azul Discontinuo)
            fig_7d.add_trace(go.Scatter(
                x=dates_7d,
                y=counts_7d_base,
                mode='lines+markers',
                name='Modelo Base',
                line=dict(color='#3b82f6', width=2, dash='dash'),
                marker=dict(size=6, color='#3b82f6'),
                text=hover_text_base,
                hoverinfo='text'
            ))

            # Curva Modelo Climático (Violeta)
            fig_7d.add_trace(go.Scatter(
                x=dates_7d,
                y=counts_7d_v3,
                mode='lines+markers',
                name='Modelo Climático (Puro)',
                line=dict(color='#8b5cf6', width=2.5, dash='dashdot'),
                marker=dict(size=7, color='#8b5cf6'),
                text=hover_text_v3,
                hoverinfo='text'
            ))
            
            # Baseline mean trace
            fig_7d.add_trace(go.Scatter(
                x=[dates_7d[0], dates_7d[-1]],
                y=[mean_real, mean_real],
                mode='lines',
                name=f'Media Histórica ({mean_real:.2f})',
                line=dict(color='#ef4444', width=1.5, dash='dot'),
                hoverinfo='skip'
            ))
            
            # Format layout
            layout_params_7d = PLOT_LAYOUT.copy()
            layout_params_7d['margin'] = dict(l=40, r=20, t=10, b=30)
            layout_params_7d['xaxis'] = dict(
                **PLOT_LAYOUT['xaxis'],
                tickformat='%d-%b',
                dtick="D1"
            )
            # Ensure Y scale starts from 0 to prevent visual distortion
            layout_params_7d['yaxis'] = dict(
                **PLOT_LAYOUT['yaxis'],
                range=[0.0, max(max(counts_7d) + 1.5, max(counts_7d_base) + 1.5, 9.0)]
            )
            
            fig_7d.update_layout(
                **layout_params_7d,
                xaxis_title="Fecha de Predicción",
                yaxis_title="Cantidad de Llamadas",
                height=320,
            )
            
            st.plotly_chart(fig_7d, use_container_width=True, config={"displayModeBar": False})
            
        # 2. Grid cards for daily details
        st.markdown('<div style="margin-top: 1.5rem; margin-bottom: 0.8rem;"><h5 style="color: var(--text);">Detalle Diario y Alertas Operativas</h5></div>', unsafe_allow_html=True)
        
        cols = st.columns(7)
        for i, p in enumerate(pred_results):
            with cols[i]:
                # Alert calculation (prob_alta >= clf_threshold or pred_count > umbral_alta)
                is_alert = (p['Prob_Alta'] >= clf_threshold) or (p['Predicción'] > umbral_alta)
                badge_class = "delta-down" if is_alert else "delta-up"
                badge_text = "🚨 ALERTA" if is_alert else "✅ NORMAL"
                
                # Render daily card
                st.markdown(f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 0.8rem; text-align: center; height: 100%;">
                    <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 700; text-transform: uppercase;">{p['Día']}</div>
                    <div style="font-size: 0.8rem; font-weight: 600; color: var(--text); margin-bottom: 0.4rem;">{p['Fecha'].strftime('%d-%b')}</div>
                    <div style="font-size: 1.3rem; font-weight: 800; color: var(--text); margin-bottom: 0.1rem;">{p['Predicción']:.1f}</div>
                    <div style="font-size: 0.62rem; color: var(--text-muted); margin-bottom: 0.5rem;">llamadas</div>
                    <div class="metric-delta {badge_class}" style="margin: 0 auto 0.6rem; font-size: 0.62rem; padding: 2px 6px;">{badge_text}</div>
                    <hr style="border-color: var(--border); margin: 0.5rem 0; opacity: 0.5;" />
                    <div style="font-size: 0.68rem; color: var(--text-muted); line-height: 1.4; text-align: left;">
                        🌡️ Máx: <strong>{p['Temp_Max']:.1f}°C</strong><br/>
                        💨 Viento: <strong>{p['Viento_Medio']:.1f} km/h</strong><br/>
                        🌧️ Lluvia: <strong>{p['Lluvia']:.1f} mm</strong><br/>
                        🔥 Prob: <strong>{p['Prob_Alta']*100:.0f}%</strong>
                    </div>
                </div>""", unsafe_allow_html=True)
                
        # Alerta general o resumen
        alert_days = [p['Fecha'].strftime('%d-%b') for p in pred_results if (p['Prob_Alta'] >= clf_threshold or p['Predicción'] > umbral_alta)]
        st.markdown("<br/>", unsafe_allow_html=True)
        if alert_days:
            st.warning(f"⚠️ **Alerta Operativa:** Se prevé alta demanda de emergencias (probabilidad $\\ge {clf_threshold*100:.0f}\\%$ o $\\ge {umbral_alta + 1:.0f}$ llamadas) para los siguientes días: **{', '.join(alert_days)}**. Se recomienda planificar guardias preventivas adicionales.")
        else:
            st.success("✅ **Estado de Alerta:** No se prevén días críticos con alta demanda en los siguientes 7 días. Guardia ordinaria suficiente.")

with tab2:
    with st.container():

        st.markdown('<div class="importance-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Desempeño Comparativo e Importancia Relativa Completa de Variables</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Resumen de métricas de precisión y porcentaje de influencia de cada variable explicativa para los tres modelos (Modelo Base, Modelo Climático con Inercia de Actividad y Modelo Climático).</div>', unsafe_allow_html=True)
    col_met1, col_met2, col_met3 = st.columns(3)
    with col_met1:
        mae_base = float(metadata_base['mae'])
        mse_base = float(metadata_base['mse'])
        r2_base = float(metadata_base['r2']) * 100
        thresh_base = float(metadata_base['classification_threshold'])
        acc_base = float(metadata_base['accuracy']) * 100
        prec_base = float(metadata_base['precision']) * 100
        rec_base = float(metadata_base['recall']) * 100
        f1_base = float(metadata_base['f1']) * 100
        st.markdown(f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #3b82f6; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Modelo Base</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_base:.2f} llamadas</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Cuadrático Medio (MSE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mse_base:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Coeficiente R² (Regresión)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{r2_base:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Umbral Clasificación Calibrado</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{thresh_base:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Exactitud (Accuracy)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{acc_base:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Precisión (Precision)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{prec_base:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Sensibilidad (Recall)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{rec_base:.1f}%</td></tr>
                <tr><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">F1-Score (Clasificador)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{f1_base:.1f}%</td></tr>
            </table>
        </div>""", unsafe_allow_html=True)
    with col_met2:
        mae_aug = float(metadata_aug['mae'])
        mse_aug = float(metadata_aug['mse'])
        r2_aug = float(metadata_aug['r2']) * 100
        thresh_aug = float(metadata_aug['classification_threshold'])
        acc_aug = float(metadata_aug['accuracy']) * 100
        prec_aug = float(metadata_aug['precision']) * 100
        rec_aug = float(metadata_aug['recall']) * 100
        f1_aug = float(metadata_aug['f1']) * 100
        st.markdown(f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #10b981; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Modelo Climático con Inercia de Actividad</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_aug:.2f} llamadas</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Cuadrático Medio (MSE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mse_aug:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Coeficiente R² (Regresión)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{r2_aug:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Umbral Clasificación Calibrado</td><td style="text-align: right; color: #10b981; font-weight: 700; padding: 0.3rem 0;">{thresh_aug:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Exactitud (Accuracy)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{acc_aug:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Precisión (Precision)</td><td style="text-align: right; color: #10b981; font-weight: 700; padding: 0.3rem 0;">{prec_aug:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Sensibilidad (Recall)</td><td style="text-align: right; color: #10b981; font-weight: 700; padding: 0.3rem 0;">{rec_aug:.1f}%</td></tr>
                <tr><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">F1-Score (Clasificador)</td><td style="text-align: right; color: #10b981; font-weight: 700; padding: 0.3rem 0;">{f1_aug:.1f}%</td></tr>
            </table>
        </div>""", unsafe_allow_html=True)
    with col_met3:
        mae_v3 = float(metadata_v3['mae'])
        mse_v3 = float(metadata_v3['mse'])
        r2_v3 = float(metadata_v3['r2']) * 100
        thresh_v3 = float(metadata_v3['classification_threshold'])
        acc_v3 = float(metadata_v3['accuracy']) * 100
        prec_v3 = float(metadata_v3['precision']) * 100
        rec_v3 = float(metadata_v3['recall']) * 100
        f1_v3 = float(metadata_v3['f1']) * 100
        st.markdown(f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-bottom: 1rem;">
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #8b5cf6; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Modelo Climático (Puro)</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_v3:.2f} llamadas</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Cuadrático Medio (MSE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mse_v3:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Coeficiente R² (Regresión)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{r2_v3:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Umbral Clasificación Calibrado</td><td style="text-align: right; color: #8b5cf6; font-weight: 700; padding: 0.3rem 0;">{thresh_v3:.2f}</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Exactitud (Accuracy)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{acc_v3:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Precisión (Precision)</td><td style="text-align: right; color: #8b5cf6; font-weight: 700; padding: 0.3rem 0;">{prec_v3:.1f}%</td></tr>
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Sensibilidad (Recall)</td><td style="text-align: right; color: #8b5cf6; font-weight: 700; padding: 0.3rem 0;">{rec_v3:.1f}%</td></tr>
                <tr><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">F1-Score (Clasificador)</td><td style="text-align: right; color: #8b5cf6; font-weight: 700; padding: 0.3rem 0;">{f1_v3:.1f}%</td></tr>
            </table>
        </div>""", unsafe_allow_html=True)
    with st.expander("📖 Guía de Interpretación y Explicación de Métricas de Evaluación"):
        st.markdown("""<div style="font-size: 0.78rem; line-height: 1.6; color: var(--text-muted); padding: 0.5rem;">
            <p style="margin-top: 0px;"><strong>1. Métricas del Modelo de Regresión (¿Cuántas llamadas exactas ocurrirán hoy?):</strong></p>
            <ul>
                <li style="margin-bottom: 0.5rem;"><strong>Error Absoluto Medio (MAE):</strong> Desviación promedio esperada de la predicción. Un valor de 2.5 indica que, en promedio, el pronóstico del modelo acierta con una diferencia de <strong>&plusmn;2.5 llamadas</strong> respecto a la realidad.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Error Cuadrático Medio (MSE):</strong> Promedio de los errores elevados al cuadrado. Al elevarlos al cuadrado, penaliza y destaca con mayor rigor las fallas graves de predicción.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Coeficiente R² (Regresión):</strong> Mide la mejora del modelo en comparación con usar un promedio histórico fijo.
                <br/><em>¿Por qué es negativo o cercano a cero?</em> La demanda de emergencias de bomberos diaria posee una aleatoriedad intrínseca extrema. Un R² ligeramente negativo o muy bajo (-0.7% o -0.3%) en la evaluación indica que predecir el número puntual exacto tiene tanto "ruido" que el promedio histórico comete un error similar, lo que demuestra la dificultad de acertar el número exacto, aunque el modelo sea excelente en capturar tendencias y días críticos.</li>
            </ul>
            <p style="margin-top: 1rem;"><strong>2. Métricas del Modelo de Clasificación (¿Será hoy un día crítico de alta demanda &gt;7 llamadas?):</strong></p>
            <ul>
                <li style="margin-bottom: 0.5rem;"><strong>Umbral de Clasificación Calibrado:</strong> El porcentaje mínimo de riesgo requerido para disparar el aviso de <code>🚨 ALERTA</code>. Usar el 50% por defecto ignoraría los días críticos por ser escasos (~15% de los días). Calibrar el umbral (a 0.20 y 0.15) maximiza la detección preventiva controlando las falsas alarmas.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Exactitud (Accuracy):</strong> El porcentaje de días totales (tanto normales como críticos) en los que el clasificador del modelo acertó el estado de alerta correcto.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Precisión (Precision):</strong> De todos los días en los que el modelo emitió una alerta de día crítico, cuántos lo fueron realmente. Un 25% indica que 1 de cada 4 alertas preventivas es un día crítico real (tasa óptima y segura para logística de bomberos).</li>
                <li style="margin-bottom: 0.5rem;"><strong>Sensibilidad (Recall):</strong> Qué porcentaje de los días críticos reales que ocurrieron logró anticipar y alertar el modelo. Un 70.4% significa que el modelo capta y advierte con éxito el 70% de las situaciones críticas reales.</li>
                <li style="margin-bottom: 0.5rem;"><strong>F1-Score:</strong> Balance de equilibrio matemático entre la Precisión y la Sensibilidad. Es la métrica estándar más robusta para evaluar modelos con datos altamente desbalanceados.</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    col_imp1, col_imp2, col_imp3 = st.columns(3)
    with col_imp1:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Modelo Base</div>', unsafe_allow_html=True)
        fig_imp = go.Figure()
        fig_imp.add_trace(go.Bar(
            y=df_imp_base['Feature'],
            x=df_imp_base['Importance'] * 100,
            orientation='h',
            marker=dict(color='#3b82f6', line=dict(color='rgba(255,255,255,0.05)', width=1)),
            hovertemplate='%{y}: %{x:.2f}% de importancia<extra></extra>',
            text=[f"{val:.1f}%" for val in df_imp_base['Importance'] * 100],
            textposition='outside',
            textfont=dict(size=9, color="#fafafa" if IS_DARK else "#09090b")
        ))
        max_val_base = max(df_imp_base['Importance'] * 100)
        layout_base = PLOT_LAYOUT.copy()
        layout_base['xaxis'] = dict(
            **PLOT_LAYOUT['xaxis'],
            range=[0, max_val_base * 1.18]
        )
        fig_imp.update_layout(
            **layout_base,
            xaxis_title="Importancia Relativa (%)",
            yaxis_title="Características",
            height=750
        )
        st.plotly_chart(fig_imp, use_container_width=True, config={"displayModeBar": False})
    with col_imp2:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Modelo Climático con Inercia de Actividad</div>', unsafe_allow_html=True)
        fig_imp_agn = go.Figure()
        fig_imp_agn.add_trace(go.Bar(
            y=df_imp_aug['Feature'],
            x=df_imp_aug['Importance'] * 100,
            orientation='h',
            marker=dict(color='#10b981', line=dict(color='rgba(255,255,255,0.05)', width=1)),
            hovertemplate='%{y}: %{x:.2f}% de importancia<extra></extra>',
            text=[f"{val:.1f}%" for val in df_imp_aug['Importance'] * 100],
            textposition='outside',
            textfont=dict(size=9, color="#fafafa" if IS_DARK else "#09090b")
        ))
        max_val_aug = max(df_imp_aug['Importance'] * 100)
        layout_aug = PLOT_LAYOUT.copy()
        layout_aug['xaxis'] = dict(
            **PLOT_LAYOUT['xaxis'],
            range=[0, max_val_aug * 1.18]
        )
        fig_imp_agn.update_layout(
            **layout_aug,
            xaxis_title="Importancia Relativa (%)",
            yaxis_title="Características",
            height=750
        )
        st.plotly_chart(fig_imp_agn, use_container_width=True, config={"displayModeBar": False})
    with col_imp3:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Modelo Climático (Puro)</div>', unsafe_allow_html=True)
        fig_imp_v3 = go.Figure()
        fig_imp_v3.add_trace(go.Bar(
            y=df_imp_v3['Feature'],
            x=df_imp_v3['Importance'] * 100,
            orientation='h',
            marker=dict(color='#8b5cf6', line=dict(color='rgba(255,255,255,0.05)', width=1)),
            hovertemplate='%{y}: %{x:.2f}% de importancia<extra></extra>',
            text=[f"{val:.1f}%" for val in df_imp_v3['Importance'] * 100],
            textposition='outside',
            textfont=dict(size=9, color="#fafafa" if IS_DARK else "#09090b")
        ))
        max_val_v3 = max(df_imp_v3['Importance'] * 100)
        layout_v3 = PLOT_LAYOUT.copy()
        layout_v3['xaxis'] = dict(
            **PLOT_LAYOUT['xaxis'],
            range=[0, max_val_v3 * 1.18]
        )
        fig_imp_v3.update_layout(
            **layout_v3,
            xaxis_title="Importancia Relativa (%)",
            yaxis_title="Características",
            height=750
        )
        st.plotly_chart(fig_imp_v3, use_container_width=True, config={"displayModeBar": False})
    st.markdown("""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1.2rem; margin-top: 1.5rem;">
        <div style="font-weight: 700; font-size: 0.95rem; color: var(--text); margin-bottom: 0.8rem; border-bottom: 1px solid var(--border); padding-bottom: 0.4rem;">📋 Glosario Completo y Explicación de las Variables de Entrada</div>
        <div style="font-size: 0.78rem; line-height: 1.6; color: var(--text-muted);">
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">1. Métricas Operativas y Lags de Emergencias (Históricos):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>EVENTOS_lag_1, EVENTOS_lag_2, EVENTOS_lag_3, EVENTOS_lag_7:</strong> Cantidad total de despachos ocurridos hace 1, 2, 3 y 7 días respectivamente. Miden la correlación serial de corta distancia (el nivel basal inmediato del sistema).</li>
                <li style="margin-bottom: 0.4rem;"><strong>EVENTOS_rolling_mean_3d, EVENTOS_rolling_mean_7d:</strong> Promedio diario móvil de incidentes en los últimos 3 y 7 días. Suaviza picos puntuales e indica el volumen de carga semanal del CBT.</li>
                <li style="margin-bottom: 0.4rem;"><strong>EVENTOS_rolling_std_3d, EVENTOS_rolling_std_7d:</strong> Desviación estándar (volatilidad) de los eventos de emergencias en las ventanas de 3 y 7 días. Ayuda a capturar la incertidumbre del comportamiento de la demanda reciente.</li>
                <li style="margin-bottom: 0.4rem;"><strong>EVENTOS_rolling_max_3d:</strong> Carga operativa máxima en un solo día durante las últimas 72 horas, detectando si venimos de un día atípicamente congestionado.</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">2. Lags Operativos por Tipo de Emergencia (Históricos):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>N_INCENDIO_ESTR_lag_1:</strong> Cantidad de llamados por incendios estructurales (edificaciones, viviendas) atendidos el día anterior.</li>
                <li style="margin-bottom: 0.4rem;"><strong>N_INCENDIO_FOREST_lag_1:</strong> Cantidad de incendios de cobertura forestal o pastizales forestales combatidos el día de ayer.</li>
                <li style="margin-bottom: 0.4rem;"><strong>N_RESCATE_VEH_lag_1:</strong> Cantidad de llamados por accidentes de tránsito y colisiones vehiculares registradas ayer.</li>
                <li style="margin-bottom: 0.4rem;"><strong>N_RESCATE_PERS_lag_1:</strong> Cantidad de llamados por rescate de personas atrapadas o en peligro físico ocurridos el día anterior.</li>
                <li style="margin-bottom: 0.4rem;"><strong>N_GASES_lag_1:</strong> Cantidad de llamados por emanación de gases atendidos el día anterior.</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">3. Variables Meteorológicas del Día Pronosticado:</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>TEMP_MAX, TEMP_MEDIA, TEMP_MIN:</strong> Temperaturas máxima, media y mínima pronosticadas para el día objetivo. Las altas temperaturas veraniegas evaporan humedad de los combustibles forestales; las bajas invernales aumentan el uso de calefacción e incendios eléctricos.</li>
                <li style="margin-bottom: 0.4rem;"><strong>TEMP_SKEW / TEMP_KURT:</strong> Asimetría y curtosis de la temperatura calculadas sobre las 24 lecturas horarias. La asimetría mide si el perfil térmico intra-diario se sesga hacia calor o frío extremo, y la curtosis mide la volatilidad y apuntamiento térmico.</li>
                <li style="margin-bottom: 0.4rem;"><strong>HUM_MAX, HUM_MIN, HUM_MEDIA:</strong> Niveles de humedad relativa máxima, mínima y media pronosticados. La baja humedad relativa (&lt;30%) es clave en la regla del 30-30-30 de propagación de incendios.</li>
                <li style="margin-bottom: 0.4rem;"><strong>HUM_SKEW / HUM_KURT:</strong> Asimetría y curtosis de la humedad relativa calculadas sobre las 24 lecturas horarias. Explican la persistencia y velocidad de desecación del combustible fino forestal, indicando si la humedad cae de golpe o se mantiene persistentemente baja.</li>
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_MAX, VIENTO_MEDIO:</strong> Velocidad máxima (rachas) y promedio del viento estimadas en km/h. Es uno de los factores de propagación y caídas de árboles más críticos en la península.</li>
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_SKEW / VIENTO_KURT:</strong> Asimetría y curtosis de la velocidad del viento calculadas sobre las 24 lecturas horarias. Capturan ráfagas o rachas extremas de viento de corta duración (picos de asimetría y colas anchas de curtosis) que las medias diarias ocultan por completo.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA:</strong> Cantidad total de precipitaciones proyectadas en milímetros (mm). Actúa como un supresor directo de incendios forestales pero incrementa accidentes y problemas en techos.</li>
                <li style="margin-bottom: 0.4rem;"><strong>Contribución al Coeficiente R² Positivo:</strong> Juntas, estas 6 variables de forma de los perfiles horarios meteorológicos acumulan el <strong>16.54%</strong> del peso total en el Modelo Climático con Inercia de Actividad. Al capturar la dinámica intra-diaria fina del clima, permiten que el modelo supere el ruido intrínseco y logre un R² de <strong>+1.3%</strong> en test (comparado con el -1.4% del modelo base).</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">4. Historial Meteorológico de Corto Plazo (Lags Climáticos):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_MEDIO_lag_1:</strong> Velocidad promedio del viento registrada el día anterior. Es el predictor individual con mayor peso en el algoritmo, debido a la persistencia del comportamiento atmosférico local.</li>
                <li style="margin-bottom: 0.4rem;"><strong>HUM_MEDIA_lag_1:</strong> Humedad relativa promedio del día anterior, capturando la desecación acumulada del suelo y aire.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_lag_1, LLUVIA_lag_2, LLUVIA_lag_3:</strong> Precipitaciones registradas ayer, hace 2 días y hace 3 días en mm. Indican si el suelo ya está húmedo (lo que reduce la posibilidad de incendios forestales incluso si hoy hay sol).</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_accum_3d:</strong> Sumatoria total de precipitaciones de los últimos 3 días (mm). Es un indicador directo del estado de saturación hídrica del terreno.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_rolling_mean_7d:</strong> Promedio diario de lluvia en la última semana, capturando la sequedad climática de mediano plazo.</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">5. Factores de Calendario y Ciclos Temporales (Descartados):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>MES, DIA_SEMANA, DANO_SIN, DANO_COS, etc.:</strong> *Nota: Estas variables de calendario y estacionalidad fija fueron descartadas por completo en favor de una aproximación puramente agnóstica basada en clima y lags de actividad, reduciendo el riesgo de sobreajuste y garantizando adaptabilidad al cambio climático.*</li>
            </ul>
        </div>
    </div>""", unsafe_allow_html=True)


with tab3:
    with st.container():
        st.markdown('<div class="chart-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Curvas Estacionales: Eventos Reales vs. Predicciones</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Promedio agrupado por día del año (1 a 365) para visualizar tendencias climáticas y estacionales en Talcahuano.</div>', unsafe_allow_html=True)
        render_seasonal_chart()
    
    # Nota explicativa
    st.info("""
    **Análisis de la Curva:**
    * **Verano (Días 1-90 y 330-365):** Se observa el mayor pico histórico y predicho de emergencias (picos de 6.5 a 7 eventos al día). Esto se correlaciona con la temporada seca y el incremento de incendios forestales/pastizales.
    * **Invierno (Días 150-250):** Hay un incremento moderado atribuido a sistemas frontales lluviosos y heladas que provocan voladuras de techos, inundaciones y emanaciones de gases (calefacción).
    * La **Predicción Base (línea roja)** asume siempre el mismo número fijo todos los días del año ({mean_real:.2f}), ignorando completamente esta estacionalidad tan marcada.
    """.format(mean_real=mean_real))

with tab4:
    st.markdown("### ¿Qué otras fuentes de datos y técnicas podemos evaluar para mejorar la proyección?")
    
    st.markdown("""
    <div class="idea-card">
        <div class="idea-title">
            📢 Alertas de SENAPRED (ex-ONEMI) <span class="idea-tag">Viabilidad: Alta</span>
        </div>
        <div class="idea-desc">
            SENAPRED emite alertas tempranas preventivas basadas en informes de CONAF o la DMC (Dirección Meteorológica de Chile) sobre altas temperaturas y sequedad. Incorporar el historial de alertas tempranas declaradas para la provincia de Concepción serviría como un potente activador de riesgo en el modelo.
        </div>
    </div>
    
    <div class="idea-card">
        <div class="idea-title">
            🚗 Scraping de Cuentas de Tránsito Costero <span class="idea-tag">Viabilidad: Media</span>
        </div>
        <div class="idea-desc">
            Los choques vehiculares y colisiones múltiples son incidentes recurrentes en la Autopista Concepción-Talcahuano o la ruta costera interportuaria. Scrapear la cuenta de la Unidad de Control de Tránsito del Biobío (@MTTBiobio) ayudaría a anticipar accidentes vinculados con alta congestión o neblina.
        </div>
    </div>
    
    <div class="idea-card">
        <div class="idea-title">
            🌊 Altura de Mareas e Inundaciones Costeras <span class="idea-tag">Viabilidad: Media</span>
        </div>
        <div class="idea-desc">
            Talcahuano tiene zonas bajas portuarias altamente vulnerables a marejadas e inundaciones por lluvias intensas. Incluir las alertas marítimas de marejadas (emitidas por la Armada) o la altura teórica de la marea diaria permitiría predecir con mayor precisión despachos por inundación estructural.
        </div>
    </div>
    
    <div class="idea-card">
        <div class="idea-title">
            🗓️ Calendario de Actividades Locales Masivas <span class="idea-tag">Viabilidad: Alta</span>
        </div>
        <div class="idea-desc">
            La Base Naval de Talcahuano y los conciertos en el Parque Bicentenario congregan a miles de personas. Agregar variables binarias específicas para los días del REC (Rock en Conce), visitas masivas al monitor Huáscar en fines de semana largos, y eventos festivos de la Municipalidad aportaría un factor correctivo humano muy valioso.
        </div>
    </div>
    """, unsafe_allow_html=True)
