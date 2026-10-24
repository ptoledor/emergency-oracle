import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import datetime
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

base_dir = Path(__file__).resolve().parent
data_utils_dir = base_dir / "02_data"
if str(data_utils_dir) not in sys.path:
    sys.path.insert(0, str(data_utils_dir))

from dedup import mark_duplicates

from activity_levels import operational_activity_level
from hourly_peak import historical_hourly_distribution
from forecasting import fetch_weather, forecast_days, load_forecast_models

# 1. Configuración de página
st.set_page_config(
    page_title="CBT Oracle - Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# 2. Rutas bases
data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
raw_messages_path = base_dir / "02_data" / "compiled_scraped_data.csv"
models_dir = base_dir / "03_model" / "saved_models"
PROJECT_TIMEZONE = ZoneInfo("America/Santiago")


def project_today():
    return datetime.datetime.now(PROJECT_TIMEZONE).date()


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
    .activity-low {{ color: #3b82f6; background: rgba(59, 130, 246, 0.14); }}
    .activity-normal {{ color: var(--green); background: var(--green-muted); }}
    .activity-high {{ color: #f59e0b; background: rgba(245, 158, 11, 0.14); }}
    .activity-alert {{ color: var(--red); background: var(--red-muted); }}
    .percentile-change {{ background: rgba(245, 158, 11, 0.16); color: var(--text); font-weight: 700; }}
    .percentile-table {{ width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-top: 0.4rem; }}
    .percentile-table th, .percentile-table td {{ border: 1px solid var(--border); padding: 0.35rem 0.45rem; text-align: right; }}
    .percentile-table th:first-child, .percentile-table td:first-child {{ text-align: left; font-weight: 700; color: var(--text); }}
    .percentile-table th {{ color: var(--text-muted); background: var(--bg-subtle); }}
    
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
    .chart-legend {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem 1rem;
        margin: 0.2rem 0 0.75rem;
        padding: 0.7rem 0.85rem;
        background: var(--bg-subtle);
        border: 1px solid var(--border);
        border-radius: 8px;
    }}
    .chart-legend-item {{
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        min-width: 0;
        color: var(--text);
        font-size: 0.78rem;
        font-weight: 700;
        line-height: 1.25;
    }}
    .chart-legend-note {{
        flex: 1 0 100%;
        color: var(--text);
        font-size: 0.76rem;
        font-weight: 700;
        opacity: 0.92;
    }}
    .chart-legend-line {{
        width: 30px;
        flex: 0 0 30px;
        border-top: 3px solid var(--legend-color);
    }}
    .chart-legend-line.dash {{ border-top-style: dashed; }}
    .chart-legend-line.dot {{ border-top-style: dotted; }}
    .chart-legend-detail {{
        color: var(--text);
        font-size: 0.74rem;
        font-weight: 600;
        opacity: 0.84;
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

    .responsive-grid {{
        display: grid;
        gap: 0.75rem;
        width: 100%;
    }}
    .responsive-grid-4 {{
        grid-template-columns: repeat(4, minmax(0, 1fr));
    }}
    .responsive-grid-6 {{
        grid-template-columns: repeat(6, minmax(0, 1fr));
    }}
    .forecast-card {{
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 0.8rem;
        text-align: center;
        min-width: 0;
    }}
    .event-level-grid {{
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.18rem;
        width: 100%;
        margin: 0.1rem 0 0.65rem;
    }}
    .event-level-cell {{ min-width: 0; text-align: center; }}
    .event-level-pill {{
        margin: 0;
        padding: 3px 2px;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 0;
        white-space: nowrap;
        width: 100%;
        box-sizing: border-box;
        border: 1px solid var(--border);
        background: var(--bg-subtle);
        color: var(--text);
    }}
    .event-level-label {{ font-size: 0.47rem; font-weight: 700; }}
    .event-level-value {{ font-size: 0.65rem; }}
    .event-level-arrow {{
        font-size: 0.7rem;
        font-weight: 900;
        margin: 0 0 0 0.12rem;
        padding: 0 0.28rem;
        min-width: 1rem;
        justify-content: center;
        border-radius: 4px;
        vertical-align: middle;
    }}
    .weather-grid {{
        display: grid;
        grid-template-columns: 1fr;
        gap: 0.08rem;
        font-size: 0.68rem;
        color: var(--text-muted);
        line-height: 1.4;
        text-align: left;
    }}
    .weather-item {{ min-width: 0; white-space: nowrap; }}

    @media (max-width: 768px) {{
        .block-container {{
            padding: 0.75rem 0.65rem 1.5rem !important;
        }}

        .header-container {{
            margin-bottom: 0.85rem;
            padding-bottom: 0.55rem;
        }}
        .brand-name {{
            font-size: 1rem;
            flex-wrap: wrap;
            gap: 4px;
        }}
        .brand-sub {{
            display: block;
            width: 100%;
            margin-left: 0;
            font-size: 0.66rem;
        }}

        [data-baseweb="tab-list"] {{
            display: flex !important;
            flex-wrap: nowrap !important;
            overflow-x: auto !important;
            scrollbar-width: thin;
            margin-bottom: 0.9rem !important;
            scroll-snap-type: x proximity;
        }}
        button[data-baseweb="tab"] {{
            flex: 0 0 auto !important;
            white-space: nowrap !important;
            font-size: 0.68rem !important;
            padding: 0.44rem 0.55rem !important;
            scroll-snap-align: start;
        }}

        .responsive-grid-6,
        .responsive-grid-4 {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }}
        .event-level-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.32rem;
        }}
        .event-level-pill {{ min-height: 37px; }}
        .event-level-label {{ font-size: 0.55rem; }}
        .event-level-value {{ font-size: 0.72rem; }}

        div[data-testid="stVerticalBlock"]:has(.chart-anchor):not(:has(div[data-testid="stVerticalBlock"])),
        div[data-testid="stVerticalBlock"]:has(.prediction-anchor):not(:has(div[data-testid="stVerticalBlock"])),
        div[data-testid="stVerticalBlock"]:has(.importance-anchor):not(:has(div[data-testid="stVerticalBlock"])) {{
            min-height: 0 !important;
            padding: 0.8rem !important;
            margin-bottom: 0.8rem !important;
        }}

        [data-testid="stHorizontalBlock"] {{
            flex-direction: column !important;
            gap: 0.7rem !important;
        }}
        [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {{
            width: 100% !important;
            flex: 1 1 100% !important;
            min-width: 0 !important;
        }}

        .chart-title {{
            font-size: 0.88rem;
        }}
        .chart-subtitle {{
            margin-bottom: 0.8rem;
        }}
        .chart-legend {{
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.55rem;
            padding: 0.65rem;
        }}
        .chart-legend-item {{
            align-items: flex-start;
            font-size: 0.72rem;
        }}
        .chart-legend-note {{
            grid-column: 1 / -1;
        }}
        .chart-legend-line {{
            width: 24px;
            flex-basis: 24px;
            margin-top: 0.35rem;
        }}
        .chart-legend-detail {{
            display: block;
            font-size: 0.66rem;
        }}
        .modebar-container {{
            display: none !important;
        }}
    }}

    @media (max-width: 420px) {{
        [data-baseweb="tab-list"] {{
            flex-wrap: wrap !important;
            overflow-x: visible !important;
            gap: 0.35rem !important;
            scrollbar-width: none;
        }}
        button[data-baseweb="tab"] {{
            flex: 1 1 calc(50% - 0.2rem) !important;
            justify-content: center !important;
            min-width: 0 !important;
            padding: 0.48rem 0.35rem !important;
            white-space: normal !important;
            text-align: center !important;
            line-height: 1.2 !important;
            min-height: 42px !important;
            font-size: 0.6rem !important;
        }}
        .responsive-grid-6 {{
            grid-template-columns: 1fr;
        }}
        .forecast-card {{
            padding: 0.9rem;
            border-radius: 12px;
        }}
        .event-level-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.4rem;
            margin-bottom: 0.8rem;
        }}
        .event-level-pill {{
            min-height: 42px;
            border-radius: 8px;
        }}
        .event-level-label {{ font-size: 0.62rem; }}
        .event-level-value {{ font-size: 0.78rem; }}
        .weather-grid {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.28rem 0.6rem;
            font-size: 0.66rem;
        }}
        .metric-card {{
            min-height: 84px;
            padding: 0.9rem 1rem;
        }}
        .metric-value {{
            font-size: 1.5rem;
        }}
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






def category_risk_level(probability):
    """Absolute, operational labels for calibrated category probabilities."""
    if probability >= 0.30:
        return "Muy Alta", "activity-alert"
    if probability >= 0.15:
        return "Alta", "activity-high"
    if probability >= 0.05:
        return "Normal", "activity-normal"
    return "Baja", "activity-low"


def category_change_level(expected_count, baseline_count):
    """Operational change status with safeguards for low-frequency groups."""
    expected = max(float(expected_count), 0.0)
    baseline = max(float(baseline_count), 0.0)
    delta = expected - baseline
    relative_delta = delta / max(baseline, 0.5)
    if delta <= -0.25 and relative_delta <= -0.15:
        return "Disminuida", "activity-low"
    if delta >= 0.75 and relative_delta >= 0.50:
        return "Muy aumentada", "activity-alert"
    if delta >= 0.25 and relative_delta >= 0.15:
        return "Aumentada", "activity-high"
    return "Normal", "activity-normal"






# 7. Carga de datos y modelos
# Versiones de los artefactos del modelo: invalidan la caché al reentrenar.
model_versions = tuple(sorted(
    (str(path.relative_to(models_dir)), path.stat().st_mtime)
    for path in models_dir.rglob("*") if path.is_file()
))


@st.cache_resource
def load_cached_forecast_models(model_versions=None):
    del model_versions
    return load_forecast_models(models_dir)


@st.cache_data
def load_data_and_predict(model_versions=None):
    """Daily dataset with the walk-forward (out-of-sample) predictions of the active model."""
    if not data_path.exists():
        return None, None, None
    df = pd.read_csv(data_path, sep=';').sort_values('FECHA_DIA').reset_index(drop=True)
    models = load_cached_forecast_models(model_versions)
    metadata = models["metadata"]
    df['PRED_EVENTOS_PRIMARY'] = np.nan
    df['PROB_ALTA_PRIMARY'] = np.nan
    oof_path = models_dir / metadata["model_name"] / "oof_predictions.csv"
    if oof_path.exists():
        oof = pd.read_csv(oof_path, sep=';').set_index('FECHA_DIA')
        df['PRED_EVENTOS_PRIMARY'] = df['FECHA_DIA'].map(oof['PRED_EVENTOS_OOF'])
        df['PROB_ALTA_PRIMARY'] = df['FECHA_DIA'].map(oof['PROB_ALTA_OOF'])
    regressor = models["regressor"]
    df_importance = pd.DataFrame({
        'Feature': regressor.feature_cols,
        'Importance': regressor.feature_importances_,
    }).sort_values(by='Importance', ascending=True)
    df['FECHA_DT'] = pd.to_datetime(df['FECHA_DIA'])
    df['DIA_DEL_ANO'] = df['FECHA_DT'].dt.dayofyear
    return df, df_importance, metadata


df, df_importance, model_metadata = load_data_and_predict(model_versions)
forecast_models = load_cached_forecast_models(model_versions)
category_risk_artifact = forecast_models["category_risk"]


# Obtener tiempos de modificacion de archivos de modelos para invalidar cache al reentrenar




















@st.cache_data
def load_historical_hourly_occurrence(raw_mtime=None, daily_mtime=None):
    del raw_mtime, daily_mtime
    if not raw_messages_path.exists() or not data_path.exists():
        return pd.DataFrame()

    raw = pd.read_csv(raw_messages_path, sep=";", usecols=["Fecha", "Texto"])
    daily = pd.read_csv(data_path, sep=";", usecols=["FECHA_DIA"])
    valid_dates = set(
        pd.to_datetime(daily["FECHA_DIA"], errors="coerce").dropna().dt.date
    )
    _, incident_flags = mark_duplicates(raw, "Fecha", "Texto")
    incidents = raw[
        raw.index.map(lambda index: incident_flags.get(index, False))
    ].copy()
    return historical_hourly_distribution(incidents["Fecha"], valid_dates)


historical_hourly_occurrence = load_historical_hourly_occurrence(
    raw_messages_path.stat().st_mtime if raw_messages_path.exists() else None,
    data_path.stat().st_mtime if data_path.exists() else None,
)


def category_risk_label(probability):
    if probability is None or np.isnan(probability):
        return "Sin modelo", "activity-low"
    return category_risk_level(float(probability))


def probability_percent(probability):
    if probability is None or np.isnan(probability):
        return "--"
    return f"{probability * 100:.0f}%"


def risk_model_details(primary_key, legacy_key=None):
    if not category_risk_artifact:
        return {}
    models = category_risk_artifact.get("models", {})
    return models.get(primary_key, models.get(legacy_key, {}))


def risk_probability_series(primary_key, legacy_key=None):
    details = risk_model_details(primary_key, legacy_key)
    reference = details.get("oof_probabilities", [])
    if reference:
        return pd.to_numeric(pd.Series(reference), errors="coerce").dropna()
    primary_col = f"PROB_{primary_key.upper()}_ALTO"
    source = df.get(primary_col)
    if source is None or pd.to_numeric(source, errors="coerce").dropna().empty:
        legacy_col = f"PROB_{legacy_key.upper()}_ALTO" if legacy_key else None
        source = (
            df.get(legacy_col, pd.Series(dtype=float))
            if legacy_col
            else pd.Series(dtype=float)
        )
    return pd.to_numeric(source, errors="coerce").dropna()




PERCENTILE_COLUMNS = [0, 10, 20, 30, 33, 40, 50, 60, 66, 70, 80, 90, 100]


def build_percentile_row(metric_name, values, as_probability=False):
    series = pd.to_numeric(values, errors='coerce').dropna()
    row = {"Metrica": metric_name}
    for percentile in PERCENTILE_COLUMNS:
        value = float(series.quantile(percentile / 100)) if not series.empty else 0.0
        row[f"p{percentile}"] = f"{value * 100:.1f}%" if as_probability else f"{value:.2f}"
    mean_value = float(series.mean()) if not series.empty else 0.0
    std_value = float(series.std()) if not series.empty else 0.0
    row["Media"] = f"{mean_value * 100:.1f}%" if as_probability else f"{mean_value:.2f}"
    row["Desvest"] = f"{std_value * 100:.1f}%" if as_probability else f"{std_value:.2f}"
    return row


def render_percentile_table(rows):
    columns = ["Metrica"] + [f"p{p}" for p in PERCENTILE_COLUMNS] + ["Media", "Desvest"]
    header_cells = []
    for column in columns:
        cell_class = ' class="percentile-change"' if column in ["p33", "p66", "p80"] else ""
        header_cells.append(f"<th{cell_class}>{column}</th>")

    body_rows = []
    for row in rows:
        cells = []
        for column in columns:
            cell_class = ' class="percentile-change"' if column in ["p33", "p66", "p80"] else ""
            cells.append(f"<td{cell_class}>{row[column]}</td>")
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    return (
        '<table class="percentile-table">'
        f"<thead><tr>{''.join(header_cells)}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


@st.cache_data(ttl=3600, show_spinner=False)
def get_weather_for_range(start_date, is_historical):
    return fetch_weather(start_date, is_historical)




def predict_6_days(start_date, is_historical, weather_data=None):
    try:
        if weather_data is None:
            weather_data = get_weather_for_range(start_date, is_historical)
    except Exception as e:
        st.error(f"Error al descargar pronóstico del clima: {e}")
        return None, None, None
    try:
        return forecast_days(start_date, weather_data, df, forecast_models)
    except ValueError as e:
        st.error(str(e))
        return None, None, None


# 8. Encabezado principal de la aplicación

st.markdown("""
<div class="header-container">
    <div class="brand-name">
        🧯 Emergencias CBT <span style="font-size: 0.65rem; font-weight: 400; color: var(--text-muted); vertical-align: middle;">v3</span>
    </div>
</div>
""", unsafe_allow_html=True)

if df is None:
    st.warning("No se pudo cargar el dataset o los modelos. Por favor asegúrate de haber ejecutado los scripts de aumentación y entrenamiento de forma exitosa.")
    st.stop()

# 9. Sección de KPIs
mean_real = df['EVENTOS'].mean()

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
        x=1,
        font=dict(
            size=12,
            color="#fafafa" if IS_DARK else "#09090b",
        ),
        bgcolor="rgba(9, 9, 11, 0.92)" if IS_DARK else "rgba(255, 255, 255, 0.94)",
        bordercolor="#3f3f46" if IS_DARK else "#d4d4d8",
        borderwidth=1,
        itemsizing="constant",
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
    y_pred_col = 'PRED_EVENTOS_PRIMARY'
    grouped_cols = ['EVENTOS', y_pred_col]

    # Reales: todo el histórico; predicción: solo días con predicción fuera de muestra.
    # Agrupar por día del año y restringir a 365 días (excluyendo el día bisiesto 366 si existe)
    df_grouped = df.groupby('DIA_DEL_ANO')[grouped_cols].mean().reset_index()
    df_grouped = df_grouped[df_grouped['DIA_DEL_ANO'] <= 365].copy()

    event_mean = float(df['EVENTOS'].mean())
    event_std = float(df['EVENTOS'].std())
    reference_levels = [
        ("Promedio histórico", event_mean, "#22c55e", "solid"),
        ("Nivel alto habitual", event_mean + event_std, "#f59e0b", "dash"),
        ("Nivel bajo habitual", max(0.0, event_mean - event_std), "#f59e0b", "dash"),
    ]
    
    # Calcular límites Y fijos antes del suavizado para evitar reescalado del eje
    reference_values = [level for _, level, _, _ in reference_levels]
    y_min = min(
        df_grouped['EVENTOS'].min(),
        df_grouped[y_pred_col].min(),
        min(reference_values),
    )
    y_max = max(
        df_grouped['EVENTOS'].max(),
        df_grouped[y_pred_col].max(),
        max(reference_values),
    )
    ymin_fixed = max(0.0, float(y_min) - 0.5)
    ymax_fixed = float(y_max) + 0.5

    # Proponer un año no bisiesto (2025) para mapear el eje X a fechas legibles
    fechas_eje = pd.date_range(start="2025-01-01", end="2025-12-31", freq="D")
    df_grouped['FECHA_EJE'] = fechas_eje
    
    # Suavizar curvas dinámicamente según la elección del slider
    if dias_suavizado > 1:
        y_real = df_grouped['EVENTOS'].rolling(dias_suavizado, min_periods=1, center=True).mean()
        y_pred = df_grouped[y_pred_col].rolling(dias_suavizado, min_periods=1, center=True).mean()
        legend_view = f"Vista: promedio móvil de {dias_suavizado} días"
    else:
        y_real = df_grouped['EVENTOS']
        y_pred = df_grouped[y_pred_col]
        legend_view = "Vista: valores diarios sin suavizado"

    # Crear gráfico Plotly
    fig = go.Figure()
    
    # Línea de eventos reales
    fig.add_trace(go.Scatter(
        x=df_grouped['FECHA_EJE'],
        y=y_real,
        mode='lines',
        name='Llamados reales',
        line=dict(color='#fafafa' if IS_DARK else '#09090b', width=2.5),
        hovertemplate='Fecha: %{x|%d-%b}<br>Reales: %{y:.2f} eventos<extra></extra>'
    ))
    
    # Línea de predicción
    fig.add_trace(go.Scatter(
        x=df_grouped['FECHA_EJE'],
        y=y_pred,
        mode='lines',
        name='Predicción fuera de muestra',
        line=dict(color='#3b82f6', width=2.5, dash='dash'),
        hovertemplate='Fecha: %{x|%d-%b}<br>Predicho: %{y:.2f} eventos<extra></extra>'
    ))
    
    # Referencias históricas de media y desviación estándar.
    for label, level, color, dash in reference_levels:
        fig.add_trace(go.Scatter(
            x=[df_grouped['FECHA_EJE'].iloc[0], df_grouped['FECHA_EJE'].iloc[-1]],
            y=[level, level],
            mode='lines',
            name=f'{label}: {level:.2f}',
            line=dict(
                color=color,
                width=1.8 if label == "Promedio histórico" else 1.2,
                dash=dash,
            ),
            opacity=0.9 if label == "Promedio histórico" else 0.75,
            hovertemplate=f'{label}: {level:.2f} eventos<extra></extra>',
        ))
    
    # Línea vertical indicando el día actual (Hoy)
    today = project_today()
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
    layout_params['legend'] = dict(
        **PLOT_LAYOUT['legend'],
        title=dict(
            text=legend_view,
            font=dict(size=13, color="#fafafa" if IS_DARK else "#18181b"),
        ),
    )
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
        },
        key="seasonal_chart_plotly"
    )


@st.fragment
def render_historical_chart():
    # Only the walk-forward validation period has out-of-sample predictions.
    historical = (
        df[['FECHA_DT', 'EVENTOS', 'PRED_EVENTOS_PRIMARY']]
        .dropna(subset=['PRED_EVENTOS_PRIMARY'])
        .sort_values('FECHA_DT')
        .copy()
    )
    historical['AÑO'] = historical['FECHA_DT'].dt.year
    historical['MES_NUM'] = historical['FECHA_DT'].dt.month
    historical['DIA_SEMANA_NUM'] = historical['FECHA_DT'].dt.dayofweek

    month_names = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
        5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
        9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
    }
    weekday_names = {
        0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
        4: "Viernes", 5: "Sábado", 6: "Domingo",
    }

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        selected_year = st.selectbox(
            "Año",
            ["Todos"] + sorted(historical['AÑO'].unique().tolist(), reverse=True),
            key="historical_year_filter",
        )
    with filter_col2:
        selected_month = st.selectbox(
            "Mes",
            ["Todos"] + list(month_names.values()),
            key="historical_month_filter",
        )
    with filter_col3:
        selected_weekday = st.selectbox(
            "Día de la semana",
            ["Todos"] + list(weekday_names.values()),
            key="historical_weekday_filter",
        )

    if selected_year != "Todos":
        historical = historical[historical['AÑO'] == selected_year]
    if selected_month != "Todos":
        selected_month_number = next(
            number for number, name in month_names.items()
            if name == selected_month
        )
        historical = historical[historical['MES_NUM'] == selected_month_number]
    if selected_weekday != "Todos":
        selected_weekday_number = next(
            number for number, name in weekday_names.items()
            if name == selected_weekday
        )
        historical = historical[
            historical['DIA_SEMANA_NUM'] == selected_weekday_number
        ]

    smoothing_days = st.slider(
        "Suavizado visual:",
        min_value=1,
        max_value=30,
        value=7,
        step=1,
        help="El valor 1 muestra los datos diarios sin suavizado.",
        key="historical_smoothing_slider",
    )

    if historical.empty:
        st.warning("No existen observaciones para la combinación seleccionada.")
        return

    real = historical['EVENTOS'].astype(float)
    predicted = historical['PRED_EVENTOS_PRIMARY'].astype(float)

    historical_metrics = [
        ("Días", f"{len(historical):,}", "observaciones"),
        ("Media real", f"{real.mean():.2f}", "llamadas/día"),
        ("Desv. real", f"{real.std():.2f}", "llamadas"),
        ("MAE fuera de muestra", f"{(real - predicted).abs().mean():.2f}", "llamadas/día"),
    ]
    historical_metrics_html = "".join(
        f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .75rem 1rem;">
            <div style="font-size: .68rem; color: var(--text-muted); text-transform: uppercase; font-weight: 700;">{label}</div>
            <div style="font-size: 1.25rem; color: var(--text); font-weight: 800;">{value}</div>
            <div style="font-size: .65rem; color: var(--text-muted);">{unit}</div>
        </div>"""
        for label, value, unit in historical_metrics
    )
    st.markdown(
        f"""<div class="responsive-grid responsive-grid-4" style="margin-bottom: 1rem;">
            {historical_metrics_html}
        </div>""",
        unsafe_allow_html=True,
    )

    if smoothing_days > 1:
        real_plot = real.rolling(smoothing_days, min_periods=1, center=True).mean()
        predicted_plot = predicted.rolling(smoothing_days, min_periods=1, center=True).mean()
        smoothing_detail = f"Promedio móvil de {smoothing_days} días"
    else:
        real_plot = real
        predicted_plot = predicted
        smoothing_detail = "Valores diarios sin suavizado"

    real_color = '#fafafa' if IS_DARK else '#09090b'
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=historical['FECHA_DT'],
        y=real_plot,
        mode='lines',
        name='Llamados reales',
        line=dict(color=real_color, width=1.8),
        hovertemplate='%{x|%d-%m-%Y}<br>Real: %{y:.2f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=historical['FECHA_DT'],
        y=predicted_plot,
        mode='lines',
        name='Predicción fuera de muestra',
        line=dict(color='#3b82f6', width=2, dash='dash'),
        hovertemplate='%{x|%d-%m-%Y}<br>Predicción: %{y:.2f}<extra></extra>',
    ))

    global_mean = float(df['EVENTOS'].mean())
    global_std = float(df['EVENTOS'].std())
    historical_reference_levels = [
        ("Promedio histórico", global_mean, "#22c55e", "solid"),
        ("Nivel alto habitual", global_mean + global_std, "#f59e0b", "dash"),
        ("Nivel bajo habitual", max(0.0, global_mean - global_std), "#f59e0b", "dash"),
    ]
    for label, level, color, dash in historical_reference_levels:
        fig.add_trace(go.Scatter(
            x=[historical['FECHA_DT'].iloc[0], historical['FECHA_DT'].iloc[-1]],
            y=[level, level],
            mode='lines',
            name=f'{label}: {level:.2f}',
            line=dict(
                color=color,
                width=1.8 if label == "Promedio histórico" else 1.2,
                dash=dash,
            ),
            opacity=0.9 if label == "Promedio histórico" else 0.75,
            hovertemplate=f'{label}: {level:.2f} llamadas<extra></extra>',
        ))

    legend_items = [
        ("Llamados reales", "", real_color, "solid"),
        ("Predicción fuera de muestra", "", "#3b82f6", "dash"),
    ]
    legend_items.extend([
        ("Promedio histórico", f"{global_mean:.2f} llamadas", "#22c55e", "solid"),
        ("Nivel alto habitual", f"+1 desviación · {global_mean + global_std:.2f}", "#f59e0b", "dash"),
        ("Nivel bajo habitual", f"−1 desviación · {max(0.0, global_mean - global_std):.2f}", "#f59e0b", "dash"),
    ])
    legend_html = "".join(
        f"""<div class="chart-legend-item">
            <span class="chart-legend-line {line_style}" style="--legend-color: {color};"></span>
            <span>{label}{f'<span class="chart-legend-detail"> · {detail}</span>' if detail else ''}</span>
        </div>"""
        for label, detail, color, line_style in legend_items
    )
    st.markdown(
        f'<div class="chart-legend" aria-label="Leyenda del gráfico">'
        f'<div class="chart-legend-note">{smoothing_detail}</div>{legend_html}</div>',
        unsafe_allow_html=True,
    )

    y_max = max(
        13.0,
        float(real.max()) + 0.5,
        float(predicted.max()) + 0.5,
        max(level for _, level, _, _ in historical_reference_levels) + 0.5,
    )
    layout = PLOT_LAYOUT.copy()
    layout['xaxis'] = dict(
        **PLOT_LAYOUT['xaxis'],
        rangeslider=dict(visible=True, thickness=0.08),
        fixedrange=False,
    )
    layout['yaxis'] = dict(**PLOT_LAYOUT['yaxis'], range=[0, y_max], fixedrange=True)
    layout['margin'] = dict(l=45, r=20, t=20, b=55)
    fig.update_layout(
        **layout,
        xaxis_title="Fecha",
        yaxis_title="Llamados diarios",
        height=560,
        showlegend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={"displayModeBar": True, "displaylogo": False},
        key="historical_chart_plotly"
    )


def render_distribution_charts():
    def render_distribution_section(
        values,
        title,
        subtitle,
        stats,
        color,
        bin_size,
        xaxis_title,
        key=None,
    ):
        values_min = float(values.min())
        values_max = float(values.max())
        values_mean = float(values.mean())
        values_std = float(values.std())
        stats_html = "".join(
            f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .75rem 1rem;">
                <div style="font-size: .68rem; color: var(--text-muted); text-transform: uppercase; font-weight: 700;">{label}</div>
                <div style="font-size: 1.25rem; color: var(--text); font-weight: 800;">{value}</div>
                <div style="font-size: .65rem; color: var(--text-muted);">{unit}</div>
            </div>"""
            for label, value, unit in stats
        )
        st.markdown(
            f"""<div style="margin-top: 1.25rem; margin-bottom: .5rem;">
                <div class="chart-title">{title}</div>
                <div class="chart-subtitle">{subtitle}</div>
                <div class="responsive-grid responsive-grid-4">{stats_html}</div>
            </div>""",
            unsafe_allow_html=True,
        )

        normal_x = np.linspace(max(0, values_min - 1), values_max + 1, 300)
        normal_y = (
            np.exp(-0.5 * ((normal_x - values_mean) / values_std) ** 2)
            / (values_std * np.sqrt(2 * np.pi))
        )
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=values,
            histnorm='probability density',
            xbins=dict(
                start=values_min - (bin_size / 2),
                end=values_max + (bin_size / 2),
                size=bin_size,
            ),
            name=title,
            marker=dict(color=color, line=dict(color=bg, width=1)),
            opacity=0.72,
            hovertemplate='%{x:.1f} llamadas<br>Densidad: %{y:.3f}<extra></extra>',
        ))
        fig.add_trace(go.Scatter(
            x=normal_x,
            y=normal_y,
            mode='lines',
            name='Campana normal de referencia',
            line=dict(color='#f59e0b', width=3),
            hovertemplate='%{x:.1f} llamadas<br>Densidad normal: %{y:.3f}<extra></extra>',
        ))
        fig.add_vline(
            x=values_mean,
            line_color='#ef4444',
            line_dash='dash',
            line_width=1.5,
        )
        layout = PLOT_LAYOUT.copy()
        layout['margin'] = dict(l=40, r=20, t=20, b=45)
        layout['barmode'] = 'overlay'
        fig.update_layout(
            **layout,
            xaxis_title=xaxis_title,
            yaxis_title='Densidad',
            xaxis_range=[0, 13],
            height=330,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displayModeBar": False},
            key=key,
        )

    historical_events = df['EVENTOS'].astype(float)
    render_distribution_section(
        historical_events,
        "Distribución Histórica de Llamados Diarios",
        "Frecuencia observada y campana normal de referencia.",
        [
            ("Mínimo histórico", f"{historical_events.min():.0f}", "llamadas/día"),
            ("Máximo histórico", f"{historical_events.max():.0f}", "llamadas/día"),
            ("Media histórica", f"{historical_events.mean():.2f}", "llamadas/día"),
            ("Desviación estándar", f"{historical_events.std():.2f}", "llamadas"),
        ],
        "#3b82f6",
        1.0,
        "Llamados por día",
        key="dist_events",
    )

    historical_predictions = df['PRED_EVENTOS_PRIMARY'].dropna().astype(float)
    render_distribution_section(
        historical_predictions,
        "Distribución de Predicciones Fuera de Muestra",
        "Predicciones walk-forward del modelo oficial y campana normal de referencia.",
        [
            ("Mínimo predicho", f"{historical_predictions.min():.2f}", "llamadas/día"),
            ("Máximo predicho", f"{historical_predictions.max():.2f}", "llamadas/día"),
            ("Media predicha", f"{historical_predictions.mean():.2f}", "llamadas/día"),
            ("Desviación estándar", f"{historical_predictions.std():.2f}", "llamadas"),
        ],
        "#8b5cf6",
        0.5,
        "Llamados predichos por día",
        key="dist_pred_opt",
    )



# 12. Pestañas de navegación
tab_forecast, tab_seasonal, tab_history, tab_stats = st.tabs([
    "🔮 Forecast",
    "📊 Estacional Histórico",
    "📈 Histórico Diario",
    "⚡ Estadísticas de Modelo",
])

with tab_forecast:
    is_historical_pred = False
    
    # Día actual según la zona horaria operacional del proyecto.
    start_pred_date = project_today()
        
    with st.spinner("Consultando clima y simulando predicción recursiva con los modelos..."):
        try:
            shared_weather = get_weather_for_range(start_pred_date, is_historical_pred)
        except Exception as e:
            st.error(f"Error al descargar pronóstico del clima: {e}")
            shared_weather = None
        weather_uses_local_fallback = (
            shared_weather is not None
            and shared_weather.get('_source', [''])[0] == 'local_fallback'
        )

        pred_results = None
        if shared_weather is not None:
            try:
                pred_results, _, umbral_alta = predict_6_days(
                    start_pred_date, is_historical_pred, weather_data=shared_weather
                )
            except Exception as e:
                st.error(f"Error en predicción: {e}")

    if pred_results is not None:
        if weather_uses_local_fallback:
            st.warning(
                "Open-Meteo no esta disponible desde este equipo. "
                "El pronostico usa clima estimado desde el historico local."
            )
        historical_mean = float(df['EVENTOS'].astype(float).mean())
        # Predicciones fuera de muestra (walk-forward), no ajustadas sobre los mismos días.
        oof_predictions = df['PRED_EVENTOS_PRIMARY'].dropna().astype(float)
        # Para category risk: usar umbrales OOF almacenados en el artefacto
        historical_rescue_probabilities = risk_probability_series(
            "rescate_vehicular", "rescate"
        )
        historical_fire_probabilities = risk_probability_series("incendio")
        historical_climate_probabilities = risk_probability_series("climaticas")


        st.markdown('<div style="margin-bottom: 0.8rem;"><h5 style="color: var(--text);">Pronóstico Diario de Llamados Talcahuano</h5></div>', unsafe_allow_html=True)
        
        forecast_cards = []
        for p in pred_results:
            activity_text, activity_class = operational_activity_level(
                p['Prediccion']
            )
            low_confidence_html = (
                ""
                if p['Confianza_Horizonte']
                else '<br><span title="En la validación este día no superó al promedio de 30 días">baja confianza</span>'
            )
            rescue_label, rescue_class = category_risk_label(
                p.get('Prob_Rescate_Alto', np.nan)
            )
            fire_label, fire_class = category_risk_label(
                p.get('Prob_Incendio_Alto', np.nan)
            )
            climate_label, climate_class = category_risk_label(
                p.get('Prob_Climaticas_Alto', np.nan)
            )
            risk_status_by_group = {
                "incendios": (
                    fire_label,
                    fire_class,
                    p.get('Prob_Incendio_Alto', np.nan),
                ),
                "rescates": (
                    rescue_label,
                    rescue_class,
                    p.get('Prob_Rescate_Alto', np.nan),
                ),
                "climaticas": (
                    climate_label,
                    climate_class,
                    p.get('Prob_Climaticas_Alto', np.nan),
                ),
            }
            category_mix_html = ""
            composition = p.get('Category_Composition')
            if composition:
                mix_labels = {
                    "incendios": ("&#128293;", "Incendio"),
                    "rescates": ("&#128663;", "Rescate"),
                    "climaticas": ("&#9928;&#65039;", "Climático"),
                    "otros": ("&#10067;", "Otro"),
                }
                status_arrows = {
                    "activity-low": "↓",
                    "activity-normal": "–",
                    "activity-high": "↑",
                    "activity-alert": "↑↑",
                }
                severity_order = {
                    "activity-low": 0,
                    "activity-normal": 1,
                    "activity-high": 2,
                    "activity-alert": 3,
                }
                severity_levels = [
                    ("Baja", "activity-low"),
                    ("Normal", "activity-normal"),
                    ("Alta", "activity-high"),
                    ("Muy Alta", "activity-alert"),
                ]
                mix_rows = []
                for group_name in ["incendios", "rescates", "climaticas", "otros"]:
                    group = composition["groups"].get(group_name)
                    if not group:
                        continue
                    count_label, count_class = category_change_level(
                        group["count"], group["baseline"]
                    )
                    if group_name in risk_status_by_group:
                        risk_label, risk_class, risk_probability = (
                            risk_status_by_group[group_name]
                        )
                        combined_severity = min(
                            severity_order[risk_class],
                            severity_order[count_class],
                        )
                        change_label, change_class = severity_levels[
                            combined_severity
                        ]
                        status_detail = (
                            f"prob. actividad alta "
                            f"{probability_percent(risk_probability)} · "
                            f"conteo {count_label.lower()} vs base "
                            f"{group['baseline']:.1f}"
                        )
                    else:
                        change_label, change_class = count_label, count_class
                        status_detail = f"base habitual {group['baseline']:.1f}"
                    icon, event_label = mix_labels[group_name]
                    status_arrow = status_arrows[change_class]
                    mix_rows.append(
                        '<div class="event-level-cell">'
                        f'<div class="event-level-pill" '
                        f'title="{group["label"]}: {change_label} · '
                        f'{status_detail}">'
                        f'<span class="event-level-label">'
                        f'{icon} {event_label}</span>'
                        f'<span class="event-level-value">'
                        f'<strong>{group["count"]:.1f}</strong> '
                        f'<span class="metric-delta event-level-arrow {change_class}">'
                        f'{status_arrow}</span></span></div></div>'
                    )
                category_mix_html = (
                    '<div class="event-level-grid" '
                    'title="Llamados esperados por tipo; el color exige coherencia entre probabilidad y conteo">'
                    + ''.join(mix_rows) + '</div>'
                )
            forecast_cards.append(
                f"""<div class="forecast-card">
                    <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 700; text-transform: uppercase;">{p['Dia']}</div>
                    <div style="font-size: 0.8rem; font-weight: 600; color: var(--text); margin-bottom: 0.4rem;">{p['Fecha'].strftime('%d-%b')}</div>
                    <div style="font-size: 1.3rem; font-weight: 800; color: var(--text); margin-bottom: 0.1rem;">{p['Prediccion']:.1f}</div>
                    <div style="font-size: 0.62rem; color: var(--text-muted); margin-bottom: 0.2rem;">llamadas</div>
                    <div style="font-size: 0.62rem; color: var(--text-muted); margin-bottom: 0.2rem;" title="En la validación, el 80% de los días cayó dentro de este rango">rango 80%: <strong>{p['Intervalo_80_Bajo']:.0f}–{p['Intervalo_80_Alto']:.0f}</strong></div>
                    <div style="font-size: 0.62rem; color: var(--text-muted); margin-bottom: 0.5rem;" title="Probabilidad calibrada para este día del forecast">P(&gt;{umbral_alta:.0f} llamados): <strong>{p['Prob_Alta'] * 100:.0f}%</strong>{low_confidence_html}</div>
                    <div style="display: flex; flex-direction: column; align-items: center; gap: 0.35rem; margin-bottom: 0.6rem;">
                        <div class="metric-delta {activity_class}" style="margin: 0; font-size: 0.62rem; padding: 2px 6px;">{activity_text}</div>
                    </div>
                    {category_mix_html}
                    <hr style="border-color: var(--border); margin: 0.5rem 0; opacity: 0.5;" />
                    <div class="weather-grid">
                        <div class="weather-item">&#127777;&#65039; Max: <strong>{p['Temp_Max']:.1f}&deg;C</strong></div>
                        <div class="weather-item">&#127777;&#65039; Media: <strong>{p['Temp_Media']:.1f}&deg;C</strong></div>
                        <div class="weather-item">&#128167; Humedad: <strong>{p['Hum_Media']:.0f}%</strong></div>
                        <div class="weather-item">&#128168; Viento: <strong>{p['Viento_Medio']:.1f} km/h</strong></div>
                        <div class="weather-item">&#128168; Ráfaga media: <strong>{p['Rafaga_Media']:.1f} km/h</strong></div>
                        <div class="weather-item">&#127783;&#65039; Lluvia: <strong>{p['Lluvia']:.1f} mm</strong></div>
                    </div>
                </div>"""
            )
        st.markdown(
            f"""<div class="responsive-grid responsive-grid-6">
                {''.join(forecast_cards)}
            </div>""",
            unsafe_allow_html=True,
        )
        if not historical_hourly_occurrence.empty:
            hourly_history = historical_hourly_occurrence.copy()
            hourly_history["percentage"] = hourly_history["probability"] * 100
            hourly_fig = go.Figure(go.Bar(
                x=hourly_history["hour"],
                y=hourly_history["percentage"],
                customdata=hourly_history[["days_with_event", "total_days", "count"]],
                marker=dict(
                    color=hourly_history["percentage"],
                    colorscale=[[0, "#bfdbfe"], [1, "#2563eb"]],
                    showscale=False,
                    line=dict(color="rgba(37, 99, 235, 0.45)", width=1),
                ),
                hovertemplate=(
                    "Hora %{x:02d}:00<br>"
                    "Probabilidad histórica: %{y:.1f}%<br>"
                    "Días con llamados: %{customdata[0]:,.0f} de %{customdata[1]:,.0f}<br>"
                    "Llamados totales: %{customdata[2]:,.0f}<extra></extra>"
                ),
            ))
            hourly_layout = dict(PLOT_LAYOUT)
            hourly_layout["margin"] = dict(l=40, r=15, t=10, b=45)
            hourly_layout["xaxis"] = dict(
                **PLOT_LAYOUT["xaxis"],
                title="Hora local (America/Santiago)",
                tickmode="array",
                tickvals=list(range(0, 24, 2)),
                ticktext=[f"{hour:02d}:00" for hour in range(0, 24, 2)],
                fixedrange=True,
            )
            hourly_layout["yaxis"] = dict(
                **PLOT_LAYOUT["yaxis"],
                title="Probabilidad histórica",
                ticksuffix="%",
                rangemode="tozero",
                fixedrange=True,
            )
            hourly_fig.update_layout(
                **hourly_layout,
                height=310,
                showlegend=False,
                bargap=0.18,
            )
            st.markdown(
                '<div class="chart-title" style="margin-top: 1.15rem;">'
                'Probabilidad histórica de ocurrencia por hora</div>',
                unsafe_allow_html=True,
            )
            st.plotly_chart(
                hourly_fig,
                use_container_width=True,
                config={"displayModeBar": False, "responsive": True},
                key="historical_hourly_probability",
            )
            st.caption(
                "Cada barra representa el porcentaje de días históricos con "
                "al menos un llamado durante esa hora local. Por ejemplo, 25% "
                "equivale a un evento en esa franja en 25 de cada 100 días."
            )
        st.markdown(
            f"**Modelo oficial:** {model_metadata['description']} · "
            f"{model_metadata['validation_protocol']}"
        )
        st.caption(
            "Nivel operacional: Baja <4 · Normal 4–<6 · Alta 6–<8 · Muy alta ≥8 llamados. "
            "El rango 80% muestra dónde cayeron 8 de cada 10 días en la validación de ese día del forecast."
        )
        lag_details = pred_results[0]
        if lag_details.get('Lag_Mode') != 'observed':
            last_observed = lag_details.get('Last_Observed_Date')
            last_observed_label = (
                last_observed.strftime('%d-%m-%Y')
                if last_observed
                else 'no disponible'
            )
            if model_metadata.get('uses_event_history'):
                st.warning(
                    "La actividad reciente no está actualizada: el último conteo "
                    f"observado es del {last_observed_label}. Los días sin conteo "
                    "observado se tratan como faltantes, igual que en el "
                    "entrenamiento."
                )
            else:
                st.caption(
                    f"Último conteo observado: {last_observed_label}. El conteo total "
                    "no usa el historial reciente; la composición por tipo trata los "
                    "días sin conteo como faltantes."
                )

        percentile_summary_html = render_percentile_table([
            build_percentile_row("Nivel de actividad (predicciones fuera de muestra)", oof_predictions, as_probability=False),
            build_percentile_row("Probabilidad de R.Vehicular", historical_rescue_probabilities, as_probability=True),
            build_percentile_row("Probabilidad de incendio", historical_fire_probabilities, as_probability=True),
            build_percentile_row("Probabilidad de climaticas", historical_climate_probabilities, as_probability=True),
        ])
        st.markdown(
            f"""<div class="responsive-grid responsive-grid-4" style="margin-top: 1rem; margin-bottom: 1rem;">
                <div class="metric-card">
                    <div class="metric-label">Media historica</div>
                    <div class="metric-value">{historical_mean:.2f}</div>
                    <div style="font-size: .68rem; color: var(--text-muted);">llamadas por dia</div>
                </div>
            </div>
            <div class="chart-subtitle" style="margin-top: -0.35rem;">
                {percentile_summary_html}
            </div>""",
            unsafe_allow_html=True,
        )
                








def render_horizon_table(metadata):
    """Per-day validation metrics against the 30-day mean."""
    rows = []
    for horizon, details in sorted(metadata.get("horizons", {}).items(), key=lambda item: int(item[0])):
        lower, upper = details["interval_80"]
        gain = details["mae_promedio_30d"] - details["mae"]
        confidence = "Sí" if details["publish"] else "Baja"
        rows.append(
            f"<tr><td>Día {horizon}</td><td>{details['mae']:.2f}</td>"
            f"<td>{details['mae_promedio_30d']:.2f}</td><td>{gain:+.2f}</td>"
            f"<td>{details['high_auc']:.2f}</td><td>{details['brier_nested']:.3f}</td>"
            f"<td>{lower:+.1f} / {upper:+.1f}</td><td>{confidence}</td></tr>"
        )
    header = "".join(
        f"<th>{label}</th>" for label in [
            "Día", "MAE modelo", "MAE promedio 30 días", "Mejora", "AUC (>7)",
            "Brier", "Rango 80%", "Confianza",
        ]
    )
    st.markdown(
        f"""<div style="overflow-x: auto; background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .75rem 1rem;">
        <table class="percentile-table" style="width: 100%; min-width: 560px;">
        <thead><tr>{header}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>""",
        unsafe_allow_html=True,
    )


def render_metric_explanations():
    st.markdown(
        """
        <div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-top: 1rem;">
            <div style="font-weight: 700; color: var(--text); margin-bottom: 0.55rem;">Guía breve de métricas</div>
            <div style="font-size: 0.78rem; line-height: 1.55; color: var(--text-muted);">
                <p><strong>MAE:</strong> error absoluto medio, en llamados por día. Se compara con el promedio de los últimos 30 días, la referencia más simple posible.</p>
                <p><strong>Mejora:</strong> cuántos llamados por día menos se equivoca el modelo que el promedio de 30 días.</p>
                <p><strong>AUC (&gt;7):</strong> capacidad de ordenar los días con más de 7 llamados por sobre los demás; 0,50 equivale a azar y 1,00 sería separación perfecta.</p>
                <p><strong>Brier:</strong> error de la probabilidad de más de 7 llamados, calibrada solo con días anteriores. Menor es mejor.</p>
                <p><strong>Rango 80%:</strong> diferencia entre lo real y lo pronosticado dentro de la cual cayeron 8 de cada 10 días.</p>
                <p><strong>Confianza:</strong> "Sí" cuando ese día del forecast le gana al promedio de 30 días con al menos 95% de probabilidad (bootstrap por bloques).</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_importance_chart(df_importance, title, color, key=None):
    if df_importance is None or df_importance.empty:
        st.info("No hay importancia de variables disponible para este modelo.")
        return
    shown = df_importance.tail(20)
    fig = go.Figure(go.Bar(
        y=shown['Feature'],
        x=shown['Importance'] * 100,
        orientation='h',
        marker=dict(color=color),
        hovertemplate='%{y}: %{x:.2f}%<extra></extra>',
        text=[f"{value:.1f}%" for value in shown['Importance'] * 100],
        textposition='auto',
        textfont=dict(size=9),
        cliponaxis=False,
    ))
    max_value = max(shown['Importance'] * 100)
    layout = PLOT_LAYOUT.copy()
    layout['xaxis'] = dict(
        **PLOT_LAYOUT['xaxis'],
        range=[0, max_value * 1.28],
        automargin=True,
    )
    layout['yaxis'] = dict(PLOT_LAYOUT['yaxis'])
    layout['yaxis'].update(
        automargin=True,
        tickfont=dict(size=9),
    )
    layout['margin'] = dict(l=10, r=15, t=55, b=45)
    fig.update_layout(
        **layout,
        title=dict(
            text=title.replace(" · ", "<br>"),
            x=0.5,
            xanchor="center",
            font=dict(size=12),
        ),
        xaxis_title="Importancia Relativa (%)",
        height=620,
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=key)


with tab_stats:
    st.markdown('<div class="importance-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Estadísticas del Modelo en Forecast</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="chart-subtitle">Validación walk-forward del modelo oficial, día por día del '
        'forecast, con el clima pronosticado con la anticipación real.</div>',
        unsafe_allow_html=True,
    )
    st.info(
        f"{model_metadata['description']} · {model_metadata['validation_protocol']} · "
        f"validación del {model_metadata['test_start_date']} al {model_metadata['test_end_date']}."
    )
    render_horizon_table(model_metadata)
    render_metric_explanations()

    st.markdown(
        '<div class="chart-title" style="margin-top: 1.25rem;">Importancia de Variables</div>',
        unsafe_allow_html=True,
    )
    render_importance_chart(
        df_importance,
        "Importancia · modelo oficial",
        "#e11d48",
        key="importance_forecast",
    )


with tab_history:
    st.markdown('<div class="chart-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Histórico Real versus Predicción</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="chart-subtitle">Llamados reales y predicción fuera de muestra del modelo oficial '
        'en el período de validación walk-forward: cada día se predijo con un modelo que no lo vio al '
        'entrenar. Los filtros de año, mes y día de la semana pueden combinarse libremente.</div>',
        unsafe_allow_html=True,
    )
    render_historical_chart()
    render_distribution_charts()


with tab_seasonal:
    with st.container():
        st.markdown('<div class="chart-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Curva Estacional del Modelo Principal</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-subtitle">Llamados reales promedio por día del año (todo el histórico) '
            'y predicción fuera de muestra del modelo oficial (período de validación).</div>',
            unsafe_allow_html=True,
        )
        render_seasonal_chart()
    
    # Nota explicativa
    st.info("""
    **Análisis de la Curva:**
    * La curva de **Eventos Reales** representa la media de los eventos históricos observados para cada día del año.
    * **Verano (Días 1-90 y 330-365):** Se observa el mayor pico histórico y predicho de emergencias (picos de 6.5 a 7 eventos al día). Esto se correlaciona con la temporada seca y el incremento de incendios forestales/pastizales.
    * **Invierno (Días 150-250):** Hay un incremento moderado atribuido a sistemas frontales lluviosos y heladas que provocan voladuras de techos, inundaciones y emanaciones de gases (calefacción).
    * Las líneas horizontales muestran la **media histórica global** y los niveles de **±1 desviación estándar**.
    """)
