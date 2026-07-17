import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import pickle
import os
import requests
import datetime
import time
import holidays
import importlib
import model_components

# Streamlit puede reejecutar dashboard.py dentro de un proceso que conserva en
# sys.modules una version anterior de este modulo. Recargar solo cuando falta
# una clase requerida evita que pickle vea el artefacto nuevo con codigo viejo.
required_model_components = (
    "HydroObjectiveEnsembleRegressor",
    "SigmoidProbabilityCalibratedClassifier",
)
if any(not hasattr(model_components, name) for name in required_model_components):
    model_components = importlib.reload(model_components)

from activity_levels import operational_activity_level
from signal_features import (
    CATEGORY_LAG_FEATURES,
    OPEN_METEO_HOURLY_QUERY,
    aggregate_weather_daily,
    event_history_features,
)
from pathlib import Path
from zoneinfo import ZoneInfo
from sklearn.metrics import brier_score_loss

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
research_results_dir = base_dir / "05_research" / "results" / "weather_ablation"
blend_results_dir = (
    base_dir / "05_research" / "results" / "category_blend_calibration"
)
PROJECT_TIMEZONE = ZoneInfo("America/Santiago")


def project_today():
    return datetime.datetime.now(PROJECT_TIMEZONE).date()


def fetch_json_with_retry(url, timeout=30, retries=3):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(2 * attempt)
    raise RuntimeError(f"Error al descargar clima desde Open-Meteo: {last_error}")


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
        }}
        button[data-baseweb="tab"] {{
            flex: 0 0 auto !important;
            white-space: nowrap !important;
            font-size: 0.74rem !important;
            padding: 0.48rem 0.7rem !important;
        }}

        .responsive-grid-6,
        .responsive-grid-4 {{
            grid-template-columns: repeat(2, minmax(0, 1fr));
        }}

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
        .modebar-container {{
            display: none !important;
        }}
    }}

    @media (max-width: 420px) {{
        .responsive-grid-6 {{
            grid-template-columns: 1fr;
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


def format_model_name(proto):
    if not proto:
        return "Modelo"
    import re
    name = str(proto)
    name = name.replace(" Cross-Validation", "")
    name = re.sub(r'\(?(\d+)\s*(?:seeds|semillas)\)?', r'(\1S)', name, flags=re.IGNORECASE)
    return name


def model_algorithm_suffix(metadata):
    """Return the UI suffix for the regressor declared in model metadata."""
    regressor_type = str((metadata or {}).get("regressor_type", "")).lower()
    if "xgb" in regressor_type or "ensemble" in regressor_type:
        return " (XG)"
    if "forest" in regressor_type or regressor_type.startswith("rf"):
        return " (RF)"
    return ""


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


def force_single_thread_model(model):
    if hasattr(model, "n_jobs"):
        model.n_jobs = 1
    return model


def add_brier_if_missing(metadata, events, probabilities):
    if metadata is None or "brier" in metadata:
        return metadata
    probs = pd.to_numeric(probabilities, errors='coerce')
    valid = probs.notna() & events.notna()
    if not valid.any():
        return metadata
    threshold = float(metadata.get('umbral_alta_actividad', events[valid].quantile(0.80)))
    target = (events.astype(float) > threshold).astype(int)
    if target[valid].nunique() < 2:
        return metadata
    metadata["brier"] = float(brier_score_loss(target[valid], probs[valid]))
    return metadata


# 7. Carga de datos y modelos
@st.cache_data
def load_data_and_predict(active_model_config=None, model_mtimes=None):
    cache_version = "model-data-v5-hydro-hot-reload-compat"
    if not os.path.exists(data_path):
        return None, None, None, None, None, None, None, None, None
    df = pd.read_csv(data_path, sep=';')
    df = df.sort_values('FECHA_DIA').reset_index(drop=True)
    signal_weather_paths = [
        base_dir / "05_research" / "data" / "historical_forecast_features.csv",
        base_dir / "05_research" / "data" / "historical_forecast_features_v2.csv",
    ]
    for signal_weather_path in signal_weather_paths:
        if not signal_weather_path.exists():
            continue
        signal_weather = pd.read_csv(signal_weather_path, sep=';')
        signal_weather['FECHA_DIA'] = pd.to_datetime(
            signal_weather['FECHA_DIA']
        ).dt.strftime('%Y-%m-%d')
        df['FECHA_DIA'] = pd.to_datetime(df['FECHA_DIA']).dt.strftime('%Y-%m-%d')
        signal_columns = [
            column for column in signal_weather.columns
            if column == 'FECHA_DIA' or column not in df.columns
        ]
        df = df.merge(
            signal_weather[signal_columns],
            on='FECHA_DIA',
            how='left',
            validate='one_to_one',
        )
    weekday_columns = {
        0: 'DIA_LUNES',
        1: 'DIA_MARTES',
        2: 'DIA_MIERCOLES',
        3: 'DIA_JUEVES',
        4: 'DIA_VIERNES',
        5: 'DIA_SABADO',
        6: 'DIA_DOMINGO',
    }
    for weekday, column in weekday_columns.items():
        df[column] = (df['DIA_SEMANA'] == weekday).astype(int)
    
    # Cargar modelos y metadata
    try:
        # 1. Modelo base original, conservado como respaldo y referencia
        reg_model_base = None
        metadata_base = None
        try:
            with open(models_dir / "regressor_climatic_augmented.pkl", "rb") as f:
                reg_model_base = pickle.load(f)
            reg_model_base = force_single_thread_model(reg_model_base)
            with open(models_dir / "metadata_climatic_augmented.pkl", "rb") as f:
                metadata_base = pickle.load(f)
            
            # Realizar predicciones históricas base (alineando features)
            base_features = [c for c in metadata_base['feature_cols'] if c in df.columns]
            if len(base_features) != len(metadata_base['feature_cols']):
                missing = len(metadata_base['feature_cols']) - len(base_features)
                if missing > 0:
                    raise KeyError(f"{missing} features missing in dataset for base model")
            X_base = df[base_features]
            df['PRED_EVENTOS_BASE'] = reg_model_base.predict(X_base)
            
            # Extraer importancia base
            importances_base = getattr(reg_model_base, 'feature_importances_', None)
            if importances_base is None:
                try:
                    from sklearn.inspection import permutation_importance
                    importances_base = permutation_importance(
                        reg_model_base, X_base, df['PRED_EVENTOS_BASE'],
                        n_repeats=5, random_state=42, n_jobs=-1
                    ).importances_mean
                except Exception:
                    from sklearn.ensemble import GradientBoostingRegressor
                    imp_model = GradientBoostingRegressor(n_estimators=100, max_depth=4, random_state=42)
                    imp_model.fit(X_base, df['PRED_EVENTOS_BASE'])
                    importances_base = imp_model.feature_importances_
            df_imp_base = pd.DataFrame({
                'Feature': base_features,
                'Importance': importances_base
            }).sort_values(by='Importance', ascending=True)
        except (KeyError, Exception) as e:
            print(f"Original base model not compatible with current data: {e}. Skipping base model comparison.")
            reg_model_base = None
            metadata_base = None
            df['PRED_EVENTOS_BASE'] = np.nan
            df_imp_base = pd.DataFrame({'Feature': [], 'Importance': []})
        
        # 2. Modelo principal optimizado por categorias (CategoryBlendRegressor)
        reg_path, clf_path, meta_path = model_components.resolve_model_path(models_dir, "_climatic_augmented")
        with open(reg_path, "rb") as f:
            reg_model_aug = pickle.load(f)
        reg_model_aug = force_single_thread_model(reg_model_aug)
        with open(meta_path, "rb") as f:
            metadata_aug = pickle.load(f)
            
        # Collect ALL features needed by blend's category sub-models
        if hasattr(reg_model_aug, 'category_models'):
            all_blend_features = set(metadata_aug['feature_cols'])
            for details in reg_model_aug.category_models.values():
                all_blend_features.update(details.get('feature_cols', []))
            aug_features = [c for c in sorted(all_blend_features) if c in df.columns]
        else:
            aug_features = [c for c in metadata_aug['feature_cols'] if c in df.columns]
        X_aug = df[aug_features]
        df['PRED_EVENTOS_PRUNED'] = reg_model_aug.predict(X_aug)
        df['PRED_EVENTOS_PRIMARY'] = df['PRED_EVENTOS_PRUNED']
        
        # Extraer importancia aumentada (ponderada del blend directo + categorías)
        importances_aug = getattr(reg_model_aug, 'feature_importances_', None)
        if importances_aug is None:
            # HistGBR no tiene feature_importances_; usar permutation importance o GBR fallback
            try:
                from sklearn.inspection import permutation_importance
                importances_aug = permutation_importance(
                    reg_model_aug, X_aug, df['PRED_EVENTOS_PRIMARY'], 
                    n_repeats=5, random_state=42, n_jobs=-1
                ).importances_mean
            except Exception:
                # Fallback: entrenar GBR auxiliar solo para importancia
                from sklearn.ensemble import GradientBoostingRegressor
                imp_model = GradientBoostingRegressor(n_estimators=100, max_depth=4, random_state=42)
                imp_model.fit(X_aug, df['PRED_EVENTOS_PRIMARY'])
                importances_aug = imp_model.feature_importances_
        df_imp_aug = pd.DataFrame({
            'Feature': aug_features,
            'Importance': importances_aug
        }).sort_values(by='Importance', ascending=True)
        df_imp_v3 = df_imp_aug
        metadata_v3 = metadata_aug
        
        # Predicciones del clasificador principal
        try:
            with open(clf_path, "rb") as f:
                clf_model_v3 = pickle.load(f)
            clf_model_v3 = force_single_thread_model(clf_model_v3)
            clf_features = list(getattr(clf_model_v3, 'feature_names_in_', metadata_aug['feature_cols']))
            X_clf = df[[c for c in clf_features if c in df.columns]]
            df['PROB_ALTA_PRIMARY'] = clf_model_v3.predict_proba(X_clf)[:, 1]
            metadata_aug = add_brier_if_missing(metadata_aug, df['EVENTOS'], df['PROB_ALTA_PRIMARY'])
            metadata_v3 = metadata_aug
            metadata_base = add_brier_if_missing(metadata_base, df['EVENTOS'], df['PROB_ALTA_PRIMARY'])
        except Exception:
            df['PROB_ALTA_PRIMARY'] = np.nan

        try:
            with open(models_dir / "category_risk_models.pkl", "rb") as f:
                category_risk_artifact = pickle.load(f)
            for group_name, details in category_risk_artifact.get("models", {}).items():
                group_model = force_single_thread_model(details["model"])
                group_features = details.get("feature_cols", metadata_v3['feature_cols'])
                df[f'PROB_{group_name.upper()}_ALTO'] = group_model.predict_proba(df[group_features])[:, 1]
        except Exception:
            pass
        for probability_col in [
            'PROB_RESCATE_VEHICULAR_ALTO',
            'PROB_RESCATE_ALTO',
            'PROB_INCENDIO_ALTO',
            'PROB_CLIMATICAS_ALTO',
        ]:
            if probability_col not in df.columns:
                df[probability_col] = np.nan

        # Extraer día del año
        df['FECHA_DT'] = pd.to_datetime(df['FECHA_DIA'])
        df['DIA_DEL_ANO'] = df['FECHA_DT'].dt.dayofyear
        
        return df, df_imp_base, df_imp_aug, df_imp_v3, metadata_base, metadata_aug, metadata_v3
    except Exception as e:
        st.error(f"Error al cargar modelos: {e}")
        return None, None, None, None, None, None, None

active_model_config = {}
try:
    import json
    config_path = models_dir / "active_models.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            active_model_config = json.load(f)
except Exception:
    pass

# Obtener tiempos de modificacion de archivos de modelos para invalidar cache al reentrenar
model_mtimes = {}
try:
    for f in models_dir.glob("*.pkl"):
        model_mtimes[f.name] = f.stat().st_mtime
except Exception:
    pass

(
    df,
    df_imp_base,
    df_imp_aug,
    df_imp_v3,
    metadata_base,
    metadata_aug,
    metadata_v3,
) = load_data_and_predict(active_model_config, model_mtimes)



@st.cache_data
def load_experimental_model_metrics():
    import json

    summary_path = research_results_dir / "experiment_summary.json"
    if not summary_path.exists():
        return None
    with summary_path.open("r", encoding="utf-8") as stream:
        summary = json.load(stream)
    winner = summary["winner"]
    baseline = summary["baseline"]
    config_path = research_results_dir / winner["name"] / "run_config.json"
    with config_path.open("r", encoding="utf-8") as stream:
        rows_used = int(json.load(stream).get("rows_used", 0))

    return {
        "mae": float(winner["mae"]),
        "mse": float(winner["rmse"]) ** 2,
        "r2": float(winner["r2"]),
        "roc_auc": float(winner["roc_auc"]),
        "brier": float(winner["brier"]),
        "accuracy": float(winner["accuracy"]),
        "precision": float(winner["precision"]),
        "recall": float(winner["recall"]),
        "f1": float(winner["f1"]),
        "classification_threshold": 0.30,
        "rows_used": rows_used,
        "experiment_name": winner["name"],
        "count_model": winner["count_model"],
        "classification_model": winner["classification_model"],
        "folds_improved": int(winner["folds_improved"]),
        "promoted": bool(winner["promoted"]),
        "mae_improvement": float(winner["mae_improvement"]),
        "roc_auc_improvement": float(winner["roc_auc_improvement"]),
        "baseline_mae": float(baseline["mae"]),
        "baseline_roc_auc": float(baseline["roc_auc"]),
        "is_primary": False,
    }


experimental_metadata = load_experimental_model_metrics()


@st.cache_data
def load_blend_calibration_summary():
    import json

    path = blend_results_dir / "summary.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


blend_calibration_summary = load_blend_calibration_summary()


@st.cache_resource
def load_category_risk_artifact(model_mtime=None):
    path = models_dir / "category_risk_models.pkl"
    if not path.exists():
        return None
    with path.open("rb") as stream:
        artifact = pickle.load(stream)
    for details in artifact.get("models", {}).values():
        details["model"] = force_single_thread_model(details["model"])
    return artifact


category_risk_artifact = load_category_risk_artifact(
    model_mtimes.get("category_risk_models.pkl")
)


@st.cache_resource
def load_category_composition_artifact(model_mtime=None):
    path = models_dir / "category_composition_models.pkl"
    if not path.exists():
        return None
    with path.open("rb") as stream:
        artifact = pickle.load(stream)
    for details in artifact.get("groups", {}).values():
        details["baseline_model"] = force_single_thread_model(
            details["baseline_model"]
        )
        details["expected_model"] = force_single_thread_model(
            details["expected_model"]
        )
    return artifact


category_composition_artifact = load_category_composition_artifact(
    model_mtimes.get("category_composition_models.pkl")
)


@st.cache_resource
def load_fifth_company_artifact():
    path = models_dir / "fifth_company_models.pkl"
    if not path.exists():
        return None
    with path.open("rb") as stream:
        artifact = pickle.load(stream)
    artifact["regressor"] = force_single_thread_model(artifact["regressor"])
    artifact["classifier"] = force_single_thread_model(artifact["classifier"])
    return artifact


fifth_company_artifact = load_fifth_company_artifact()
if df is not None and fifth_company_artifact:
    try:
        fifth_metadata = fifth_company_artifact["metadata"]
        fifth_features = fifth_metadata["feature_cols"]
        fifth_X = df[fifth_features]
        df["PRED_5TA_CIA"] = np.clip(
            fifth_company_artifact["regressor"].predict(fifth_X), 0, None
        )
        df["PROB_5TA_CIA_ALTA"] = fifth_company_artifact[
            "classifier"
        ].predict_proba(fifth_X)[:, 1]
    except Exception:
        df["PRED_5TA_CIA"] = np.nan
        df["PROB_5TA_CIA_ALTA"] = np.nan


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


def predict_category_composition(feature_frame, total_prediction):
    if not category_composition_artifact:
        return None
    baseline_values = {}
    expected_values = {}
    for group_name in category_composition_artifact.get("group_order", []):
        details = category_composition_artifact["groups"][group_name]
        baseline_features = details["baseline_feature_cols"]
        expected_features = details["expected_feature_cols"]
        baseline_values[group_name] = float(np.clip(
            details["baseline_model"].predict(
                feature_frame[baseline_features]
            )[0],
            0,
            None,
        ))
        expected_values[group_name] = float(np.clip(
            details["expected_model"].predict(
                feature_frame[expected_features]
            )[0],
            0,
            None,
        ))

    baseline_total = float(sum(baseline_values.values()))
    expected_total = float(sum(expected_values.values()))
    official_total = max(float(total_prediction), 0.0)
    if expected_total <= 1e-9:
        expected_values = dict(baseline_values)
        expected_total = max(baseline_total, 1e-9)
    scale = official_total / expected_total
    reconciled = {
        group_name: value * scale
        for group_name, value in expected_values.items()
    }
    deltas = {
        group_name: reconciled[group_name] - baseline_values[group_name]
        for group_name in reconciled
    }
    positive_delta_total = float(sum(max(value, 0.0) for value in deltas.values()))
    groups = {}
    for group_name in category_composition_artifact.get("group_order", []):
        count = reconciled[group_name]
        groups[group_name] = {
            "label": category_composition_artifact["groups"][group_name]["label"],
            "count": count,
            "share_total": count / official_total if official_total > 1e-9 else 0.0,
            "baseline": baseline_values[group_name],
            "delta": deltas[group_name],
            "share_positive_delta": (
                max(deltas[group_name], 0.0) / positive_delta_total
                if positive_delta_total > 1e-9 and official_total > baseline_total
                else 0.0
            ),
        }
    return {
        "baseline_total": baseline_total,
        "forecast_total": official_total,
        "delta_total": official_total - baseline_total,
        "groups": groups,
    }


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


# Helper to fetch weather series from Open-Meteo
@st.cache_data(ttl=3600, show_spinner=False)
def build_local_weather_fallback(start_date, is_historical):
    if is_historical:
        q_start = start_date - datetime.timedelta(days=30)
        q_end = start_date + datetime.timedelta(days=5)
    else:
        q_start = start_date - datetime.timedelta(days=30)
        q_end = start_date + datetime.timedelta(days=9)

    weather_archive_path = base_dir / "02_data" / "weather_archive_talcahuano.csv"
    archive = pd.read_csv(weather_archive_path)
    archive['FECHA_DIA'] = pd.to_datetime(archive['FECHA_DIA']).dt.date
    archive['MONTH_DAY'] = archive['FECHA_DIA'].apply(lambda value: (value.month, value.day))
    archive['MONTH'] = archive['FECHA_DIA'].apply(lambda value: value.month)
    numeric_cols = [
        'TEMP_MAX', 'TEMP_MIN', 'TEMP_MEDIA',
        'HUM_MAX', 'HUM_MIN', 'HUM_MEDIA',
        'VIENTO_MAX', 'VIENTO_MEDIO', 'LLUVIA',
    ]
    global_medians = archive[numeric_cols].median(numeric_only=True)

    rows = []
    for offset in range((q_end - q_start).days + 1):
        target_date = q_start + datetime.timedelta(days=offset)
        exact = archive[archive['FECHA_DIA'] == target_date]
        if not exact.empty:
            source_row = exact.iloc[0]
        else:
            same_day = archive[archive['MONTH_DAY'] == (target_date.month, target_date.day)]
            if not same_day.empty:
                source_row = same_day[numeric_cols].median(numeric_only=True)
            else:
                same_month = archive[archive['MONTH'] == target_date.month]
                source_row = (
                    same_month[numeric_cols].median(numeric_only=True)
                    if not same_month.empty
                    else global_medians
                )

        temp_mean = float(source_row.get('TEMP_MEDIA', global_medians['TEMP_MEDIA']))
        temp_amp = max(
            0.1,
            (float(source_row.get('TEMP_MAX', temp_mean)) - float(source_row.get('TEMP_MIN', temp_mean))) / 2,
        )
        hum_mean = float(source_row.get('HUM_MEDIA', global_medians['HUM_MEDIA']))
        wind_mean = float(source_row.get('VIENTO_MEDIO', global_medians['VIENTO_MEDIO']))
        rain_daily = max(0.0, float(source_row.get('LLUVIA', global_medians['LLUVIA'])))

        for hour in range(24):
            angle = 2 * np.pi * (hour - 15) / 24
            temp = temp_mean + temp_amp * np.cos(angle)
            humidity = float(np.clip(hum_mean - 8 * np.cos(angle), 0, 100))
            rows.append({
                'time': f"{target_date.strftime('%Y-%m-%d')}T{hour:02d}:00",
                'temperature_2m': temp,
                'relative_humidity_2m': humidity,
                'wind_speed_10m': wind_mean,
                'precipitation': rain_daily / 24,
            })

    return {
        'time': [row['time'] for row in rows],
        'temperature_2m': [row['temperature_2m'] for row in rows],
        'relative_humidity_2m': [row['relative_humidity_2m'] for row in rows],
        'wind_speed_10m': [row['wind_speed_10m'] for row in rows],
        'precipitation': [row['precipitation'] for row in rows],
        '_source': ['local_fallback'] * len(rows),
    }


@st.cache_data(ttl=3600, show_spinner=False)
def get_weather_for_range(start_date, is_historical):
    lat, lon = -36.731106, -73.11023
    if is_historical:
        # Simulation mode: target range start_date to start_date + 5 days.
        # We need weather from start_date - 30 days to start_date + 5 days for lags.
        q_start = start_date - datetime.timedelta(days=30)
        q_end = start_date + datetime.timedelta(days=5)
        url = (f"https://archive-api.open-meteo.com/v1/archive?"
               f"latitude={lat}&longitude={lon}&"
               f"start_date={q_start.strftime('%Y-%m-%d')}&"
               f"end_date={q_end.strftime('%Y-%m-%d')}&"
               f"hourly={OPEN_METEO_HOURLY_QUERY}&"
               f"timezone=America%2FSantiago&format=json")
    else:
        # Real-time mode: start_date is today in America/Santiago.
        # Se requieren 30 días previos para variables de memoria hídrica.
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={lat}&longitude={lon}&"
               f"hourly={OPEN_METEO_HOURLY_QUERY}&"
               f"timezone=America%2FSantiago&past_days=30&forecast_days=10")
               
    try:
        hourly = fetch_json_with_retry(url, timeout=30, retries=3)['hourly']
        hourly['_source'] = ['open_meteo'] * len(hourly.get('time', []))
        return hourly
    except Exception:
        return build_local_weather_fallback(start_date, is_historical)

# Forecasting function for 6 days (weather-only model)
def predict_6_days(start_date, is_historical, prefix="_climatic_augmented", weather_data=None):
    # Load models and metadata
    try:
        reg_path, clf_path, meta_path = model_components.resolve_model_path(models_dir, prefix)
        with open(reg_path, "rb") as f:
            reg_model = pickle.load(f)
        reg_model = force_single_thread_model(reg_model)
        with open(clf_path, "rb") as f:
            clf_model = pickle.load(f)
        clf_model = force_single_thread_model(clf_model)
        with open(meta_path, "rb") as f:
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
    weather_df = aggregate_weather_daily(weather_hourly)

    predictions = []
    DIAS_ES = {
        'Monday': 'Lunes', 'Tuesday': 'Martes', 'Wednesday': 'Miércoles',
        'Thursday': 'Jueves', 'Friday': 'Viernes', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }

    # Initialize last_30_events from df for rolling mean features
    last_30_events = []
    df_hist = pd.DataFrame()
    try:
        df_hist = df[df['FECHA_DIA'] < start_date.strftime('%Y-%m-%d')].sort_values('FECHA_DIA')
        if len(df_hist) >= 30:
            last_30_events = list(df_hist['EVENTOS'].tail(30).fillna(0).values)
        else:
            hist_mean = df['EVENTOS'].mean() if not df.empty else 1.5
            last_30_events = [hist_mean] * 30
    except Exception:
        last_30_events = [1.5] * 30

    category_source_by_lag = {
        "N_INCENDIO_ESTR_lag_1": "N_INCENDIO_ESTR",
        "N_INCENDIO_FOREST_lag_1": "N_INCENDIO_FOREST",
        "N_RESCATE_VEH_lag_1": "N_RESCATE_VEH",
        "N_RESCATE_PERS_lag_1": "N_RESCATE_PERS",
        "N_EMERGENCIAS_CLIMATICAS_lag_1": "N_EMERGENCIAS_CLIMATICAS",
        "N_GASES_lag_1": "N_GASES",
    }
    current_category_lags = {column: 0.0 for column in CATEGORY_LAG_FEATURES}
    category_shares = {column: 0.0 for column in CATEGORY_LAG_FEATURES}
    if not df_hist.empty:
        latest = df_hist.iloc[-1]
        recent = df_hist.tail(30)
        recent_total = float(recent['EVENTOS'].sum())
        for lag_column, source_column in category_source_by_lag.items():
            current_category_lags[lag_column] = float(latest.get(source_column, 0.0))
            if recent_total > 0:
                category_shares[lag_column] = float(recent[source_column].sum() / recent_total)

    # Initialize DIAS_DESDE_ULTIMA_LLUVIA
    try:
        prev_date_str = (start_date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        df_prev = df[df['FECHA_DIA'] == prev_date_str]
        if not df_prev.empty and not pd.isna(df_prev['DIAS_DESDE_ULTIMA_LLUVIA'].values[0]):
            current_dias_desde_lluvia = float(df_prev['DIAS_DESDE_ULTIMA_LLUVIA'].values[0])
        else:
            current_dias_desde_lluvia = 10.0
    except Exception:
        current_dias_desde_lluvia = 10.0

    direct_horizon_model = hasattr(reg_model, "predict_horizon")
    origin_event_history = list(last_30_events)
    for j in range(6):
        d = start_date + datetime.timedelta(days=j)
        d_str = d.strftime('%Y-%m-%d')

        # Weather features (the only features the model uses)
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
        rain_history = {
            i: float(weather_df.loc[d - datetime.timedelta(days=i)]['LLUVIA'])
            for i in range(1, 31)
        }

        # Determine DIAS_DESDE_ULTIMA_LLUVIA
        if j == 0:
            last_day_rain = rain_history[1]
        else:
            last_day_rain = predictions[-1]['Lluvia']
            
        if last_day_rain > 0.1:
            current_dias_desde_lluvia = 0.0
        else:
            current_dias_desde_lluvia += 1.0

        # Construct feature vector
        weekday = d.weekday()
        chile_holidays = holidays.Chile(years=[d.year, (d + datetime.timedelta(days=1)).year])
        d_str_format = d.strftime('%Y-%m-%d')
        tomorrow_str_format = (d + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        mes = d.month
        dia_del_ano = d.timetuple().tm_yday

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
            'VIENTO_MEDIO_lag_1': viento_medio_lag_1,
            'HUM_MEDIA_lag_1': hum_media_lag_1,
            'ES_FERIADO': 1 if d_str_format in chile_holidays else 0,
            'ES_PRE_FERIADO': 1 if tomorrow_str_format in chile_holidays else 0,
            'ES_FIN_SEMANA': 1 if weekday in [5, 6] else 0,
            'MES': mes,
            'DIA_SEMANA': weekday,
            'MES_SIN': np.sin(2 * np.pi * mes / 12),
            'MES_COS': np.cos(2 * np.pi * mes / 12),
            'DIA_SIN': np.sin(2 * np.pi * weekday / 7),
            'DIA_COS': np.cos(2 * np.pi * weekday / 7),
            'DANO_SIN': np.sin(2 * np.pi * dia_del_ano / 365),
            'DANO_COS': np.cos(2 * np.pi * dia_del_ano / 365),
            'DIA_LUNES': 1 if weekday == 0 else 0,
            'DIA_MARTES': 1 if weekday == 1 else 0,
            'DIA_MIERCOLES': 1 if weekday == 2 else 0,
            'DIA_JUEVES': 1 if weekday == 3 else 0,
            'DIA_VIERNES': 1 if weekday == 4 else 0,
            'DIA_SABADO': 1 if weekday == 5 else 0,
            'DIA_DOMINGO': 1 if weekday == 6 else 0,
            'DIAS_DESDE_ULTIMA_LLUVIA': current_dias_desde_lluvia,
            'EVENTOS_rolling_mean_14d': float(np.mean((origin_event_history if direct_horizon_model else last_30_events)[-14:])),
            'EVENTOS_rolling_mean_30d': float(np.mean((origin_event_history if direct_horizon_model else last_30_events)[-30:])),
        }
        irrenunciable_dates = {(1, 1), (5, 1), (9, 18), (9, 19), (12, 25)}
        holiday_name = chile_holidays.get(d_str_format)
        features['ES_FERIADO_IRRENUNCIABLE'] = int(
            (d.month, d.day) in irrenunciable_dates
            or bool(
                holiday_name
                and (
                    "elecciones" in holiday_name.lower()
                    or "plebiscito" in holiday_name.lower()
                )
            )
        )
        history = origin_event_history if direct_horizon_model else last_30_events
        features.update(event_history_features(history))
        features.update(current_category_lags)
        for feature_name, feature_value in w_d.items():
            if str(feature_name).startswith(('WX_', 'WX2_')):
                features[str(feature_name)] = float(feature_value)

        # Vapor Pressure Deficit (VPD)
        es_media = 0.6108 * np.exp((17.27 * temp_media) / (temp_media + 237.3))
        features['VPD'] = es_media * (1 - hum_media / 100)
        
        es_max = 0.6108 * np.exp((17.27 * temp_max) / (temp_max + 237.3))
        features['VPD_MAX'] = es_max * (1 - hum_min / 100)

        for lag in [1, 2, 3, 5, 7, 10, 14]:
            features[f'LLUVIA_LAG_{lag}D'] = rain_history[lag]
        for window in [3, 7, 14, 30]:
            rain_window = np.array([rain_history[i] for i in range(1, window + 1)])
            features[f'LLUVIA_PROMEDIO_{window}D_PREV'] = float(np.mean(rain_window))
            features[f'LLUVIA_TOTAL_{window}D_PREV'] = float(np.sum(rain_window))
            features[f'LLUVIA_DESV_{window}D_PREV'] = float(np.std(rain_window, ddof=1))
            features[f'LLUVIA_MAX_{window}D_PREV'] = float(np.max(rain_window))
            features[f'DIAS_SECOS_{window}D_PREV'] = float(np.sum(rain_window <= 0.1))

        # Add simple weather interaction indices
        features['TEMP_HUM_INDEX'] = temp_media * hum_media / 100
        features['VIENTO_LLUVIA_INDEX'] = viento_medio * lluvia
        features['STORM_COMPOUND_INDEX'] = viento_max * (1 + lluvia)
        features['FIRE_DRY_INDEX_7D'] = (
            temp_max * features['DIAS_SECOS_7D_PREV']
            / (1 + features['LLUVIA_TOTAL_7D_PREV'])
        )

        # DataFrame with all features for blend model's category sub-models
        X_all = pd.DataFrame([features])
        # DataFrame aligned with classifier features
        X_pred = X_all[metadata['feature_cols']]

        # Predict
        if direct_horizon_model:
            pred_count = float(np.clip(reg_model.predict_horizon(X_pred, j + 1)[0], 0, None))
        elif hasattr(reg_model, "category_models"):
            pred_count = float(np.clip(reg_model.predict(X_all)[0], 0, None))
        else:
            pred_count = float(np.clip(reg_model.predict(X_pred)[0], 0, None))
        if hasattr(clf_model, "predict_proba_horizon"):
            prob_high = float(clf_model.predict_proba_horizon(X_pred, j + 1)[0, 1])
        else:
            prob_high = float(clf_model.predict_proba(X_pred)[0, 1])
        category_risk_probs = {}
        if category_risk_artifact:
            for group_name, details in category_risk_artifact.get("models", {}).items():
                group_features = details.get("feature_cols", metadata['feature_cols'])
                group_X = pd.DataFrame([features])[group_features]
                group_model = force_single_thread_model(details["model"])
                category_risk_probs[group_name] = float(group_model.predict_proba(group_X)[0, 1])

        category_composition = None
        try:
            category_composition = predict_category_composition(X_all, pred_count)
        except Exception as exc:
            print(f"No se pudo calcular la composicion por categoria: {exc}")

        fifth_company_count = np.nan
        fifth_company_probability = np.nan
        if fifth_company_artifact:
            fifth_metadata = fifth_company_artifact["metadata"]
            fifth_X = X_all[fifth_metadata["feature_cols"]]
            fifth_company_count = float(np.clip(
                fifth_company_artifact["regressor"].predict(fifth_X)[0], 0, None
            ))
            fifth_company_probability = float(
                fifth_company_artifact["classifier"].predict_proba(fifth_X)[0, 1]
            )

        # Update last_30_events for the next prediction day
        if not direct_horizon_model:
            last_30_events.append(pred_count)
            last_30_events.pop(0)
            current_category_lags = {
                column: float(pred_count * category_shares[column])
                for column in CATEGORY_LAG_FEATURES
            }

        predictions.append({
            'Fecha': d,
            'FechaStr': d_str,
            'Dia': DIAS_ES[d.strftime('%A')],
            'Prediccion': pred_count,
            'Prob_Alta': prob_high,
            'Prob_RVehicular_Alto': category_risk_probs.get(
                "rescate_vehicular",
                category_risk_probs.get("rescate", np.nan),
            ),
            'Prob_Rescate_Alto': category_risk_probs.get(
                "rescate_vehicular",
                category_risk_probs.get("rescate", np.nan),
            ),
            'Prob_Incendio_Alto': category_risk_probs.get("incendio", np.nan),
            'Prob_Climaticas_Alto': category_risk_probs.get("climaticas", np.nan),
            'Category_Composition': category_composition,
            'Pred_5ta_Cia': fifth_company_count,
            'Prob_5ta_Cia_Alta': fifth_company_probability,
            'Temp_Max': temp_max,
            'Temp_Media': temp_media,
            'Hum_Media': hum_media,
            'Viento_Medio': viento_medio,
            'Rafaga_Media': float(w_d.get('WX_GUST_MEAN', viento_medio)),
            'Lluvia': lluvia,
        })

    return predictions, float(metadata.get('classification_threshold', 0.25)), float(metadata.get('umbral_alta_actividad', 7.0))

# 8. Encabezado principal de la aplicación

st.markdown(f"""
<div class="header-container">
    <div class="brand-name">
        🧯 Emergencias CBT <span style="font-size: 0.65rem; font-weight: 400; color: var(--text-muted); vertical-align: middle;">v0617</span>
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
    y_robust_col = 'PRED_EVENTOS_KFOLD'
    has_robust_prediction = (
        y_robust_col in df.columns
        and pd.to_numeric(df[y_robust_col], errors='coerce').notna().any()
    )
    grouped_cols = ['EVENTOS', y_pred_col]
    if has_robust_prediction:
        grouped_cols.append(y_robust_col)
    
    # Agrupar por día del año y restringir a 365 días (excluyendo el día bisiesto 366 si existe)
    df_grouped = df.groupby('DIA_DEL_ANO')[grouped_cols].mean().reset_index()
    df_grouped = df_grouped[df_grouped['DIA_DEL_ANO'] <= 365].copy()

    event_mean = float(df['EVENTOS'].mean())
    event_std = float(df['EVENTOS'].std())
    reference_levels = [
        ("Media", event_mean, "#22c55e", "solid"),
        ("Media + 1 DV", event_mean + event_std, "#f59e0b", "dash"),
        ("Media - 1 DV", max(0.0, event_mean - event_std), "#f59e0b", "dash"),
    ]
    
    # Calcular límites Y fijos antes del suavizado para evitar reescalado del eje
    reference_values = [level for _, level, _, _ in reference_levels]
    y_min = min(
        df_grouped['EVENTOS'].min(),
        df_grouped[y_pred_col].min(),
        df_grouped[y_robust_col].min() if has_robust_prediction else df_grouped[y_pred_col].min(),
        min(reference_values),
    )
    y_max = max(
        df_grouped['EVENTOS'].max(),
        df_grouped[y_pred_col].max(),
        df_grouped[y_robust_col].max() if has_robust_prediction else df_grouped[y_pred_col].max(),
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
        y_robust = (
            df_grouped[y_robust_col].rolling(dias_suavizado, min_periods=1, center=True).mean()
            if has_robust_prediction
            else None
        )
        suffix = f" (Suavizado {dias_suavizado}d)"
    else:
        y_real = df_grouped['EVENTOS']
        y_pred = df_grouped[y_pred_col]
        y_robust = df_grouped[y_robust_col] if has_robust_prediction else None
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
        name=f'Prediccion Operacional{suffix}',
        line=dict(color='#3b82f6', width=2.5, dash='dash'),
        hovertemplate='Fecha: %{x|%d-%b}<br>Predicho: %{y:.2f} eventos<extra></extra>'
    ))
    if has_robust_prediction:
        fig.add_trace(go.Scatter(
            x=df_grouped['FECHA_EJE'],
            y=y_robust,
            mode='lines',
            name=f'Prediccion Robusta{suffix}',
            line=dict(color='#f97316', width=2.3, dash='dot'),
            hovertemplate='Fecha: %{x|%d-%b}<br>Robusto: %{y:.2f} eventos<extra></extra>'
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
                width=1.8 if label == "Media" else 1.2,
                dash=dash,
            ),
            opacity=0.9 if label == "Media" else 0.75,
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
    historical_cols = ['FECHA_DT', 'EVENTOS', 'PRED_EVENTOS_PRIMARY']
    if 'PRED_EVENTOS_KFOLD' in df.columns:
        historical_cols.append('PRED_EVENTOS_KFOLD')
    historical = df[historical_cols].sort_values('FECHA_DT').copy()
    has_robust_prediction = (
        'PRED_EVENTOS_KFOLD' in historical.columns
        and pd.to_numeric(historical['PRED_EVENTOS_KFOLD'], errors='coerce').notna().any()
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
    robust_predicted = (
        pd.to_numeric(historical['PRED_EVENTOS_KFOLD'], errors='coerce')
        if has_robust_prediction
        else None
    )

    historical_metrics = [
        ("Días", f"{len(historical):,}", "observaciones"),
        ("Media real", f"{real.mean():.2f}", "llamadas/día"),
        ("Desv. real", f"{real.std():.2f}", "llamadas"),
        ("Media predicción", f"{predicted.mean():.2f}", "llamadas/día"),
    ]
    if robust_predicted is not None:
        historical_metrics.append(
            ("Media robusta", f"{robust_predicted.mean():.2f}", "llamadas/día")
        )
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
        robust_plot = (
            robust_predicted.rolling(smoothing_days, min_periods=1, center=True).mean()
            if robust_predicted is not None
            else None
        )
        suffix = f" · media móvil {smoothing_days}d"
    else:
        real_plot = real
        predicted_plot = predicted
        robust_plot = robust_predicted
        suffix = " · diario"

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=historical['FECHA_DT'],
        y=real_plot,
        mode='lines',
        name=f'Real{suffix}',
        line=dict(color='#fafafa' if IS_DARK else '#09090b', width=1.8),
        hovertemplate='%{x|%d-%m-%Y}<br>Real: %{y:.2f}<extra></extra>',
    ))
    fig.add_trace(go.Scatter(
        x=historical['FECHA_DT'],
        y=predicted_plot,
        mode='lines',
        name=f'Predicción{suffix}',
        line=dict(color='#3b82f6', width=2, dash='dash'),
        hovertemplate='%{x|%d-%m-%Y}<br>Predicción: %{y:.2f}<extra></extra>',
    ))

    if robust_plot is not None:
        fig.add_trace(go.Scatter(
            x=historical['FECHA_DT'],
            y=robust_plot,
            mode='lines',
            name=f'Robusto{suffix}',
            line=dict(color='#f97316', width=1.9, dash='dot'),
            hovertemplate='%{x|%d-%m-%Y}<br>Robusto: %{y:.2f}<extra></extra>',
        ))

    global_mean = float(df['EVENTOS'].mean())
    global_std = float(df['EVENTOS'].std())
    historical_reference_levels = [
        ("Media global", global_mean, "#22c55e", "solid"),
        ("Media global + 1 DV", global_mean + global_std, "#f59e0b", "dash"),
        ("Media global - 1 DV", max(0.0, global_mean - global_std), "#f59e0b", "dash"),
    ]
    for label, level, color, dash in historical_reference_levels:
        fig.add_trace(go.Scatter(
            x=[historical['FECHA_DT'].iloc[0], historical['FECHA_DT'].iloc[-1]],
            y=[level, level],
            mode='lines',
            name=f'{label}: {level:.2f}',
            line=dict(
                color=color,
                width=1.8 if label == "Media global" else 1.2,
                dash=dash,
            ),
            opacity=0.9 if label == "Media global" else 0.75,
            hovertemplate=f'{label}: {level:.2f} llamadas<extra></extra>',
        ))

    y_max = max(
        13.0,
        float(real.max()) + 0.5,
        float(predicted.max()) + 0.5,
        float(robust_predicted.max()) + 0.5 if robust_predicted is not None else 0.0,
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

    historical_predictions = df['PRED_EVENTOS_PRIMARY'].astype(float)
    render_distribution_section(
        historical_predictions,
        "Distribución de Predicciones Históricas · Modelo Optimizado",
        "Predicciones generadas sobre el histórico y campana normal de referencia.",
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


    robust_predictions = pd.to_numeric(
        df.get('PRED_EVENTOS_KFOLD', pd.Series(dtype=float)),
        errors='coerce',
    ).dropna()
    if not robust_predictions.empty:
        render_distribution_section(
            robust_predictions,
            "Distribución de Predicciones Históricas · Modelo Robusto",
            "Predicciones KFold generadas sobre el histórico y campana normal de referencia.",
            [
                ("Mínimo robusto", f"{robust_predictions.min():.2f}", "llamadas/día"),
                ("Máximo robusto", f"{robust_predictions.max():.2f}", "llamadas/día"),
                ("Media robusta", f"{robust_predictions.mean():.2f}", "llamadas/día"),
                ("Desviación estándar", f"{robust_predictions.std():.2f}", "llamadas"),
            ],
            "#f97316",
            0.5,
            "Llamados robustos por día",
            key="dist_pred_rob",
        )


# 12. Pestañas de navegación
tab_forecast, tab_stats, tab_history, tab_seasonal = st.tabs([
    "🔮 Forecast",
    "⚡ Estadísticas de Modelo",
    "📈 Histórico Real vs Predicción",
    "📊 Curvas de Estacionalidad (365 días)",
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

        if shared_weather is None:
            pred_results = pred_results_base = pred_results_v3 = None
            clf_threshold = float((metadata_aug or {}).get('classification_threshold', 0.25))
            umbral_alta = float((metadata_aug or {}).get('umbral_alta_actividad', 7.0))
        else:
            try:
                pred_results, clf_threshold, umbral_alta = predict_6_days(
                    start_pred_date, is_historical_pred, prefix="_climatic_augmented", weather_data=shared_weather
                )
                pred_results_base = pred_results_v3 = pred_results
            except Exception as e:
                st.error(f"Error en predicción: {e}")
                pred_results = pred_results_base = pred_results_v3 = None
                clf_threshold = float((metadata_aug or {}).get('classification_threshold', 0.25))
                umbral_alta = float((metadata_aug or {}).get('umbral_alta_actividad', 7.0))
        
    if pred_results is not None:
        if weather_uses_local_fallback:
            st.warning(
                "Open-Meteo no esta disponible desde este equipo. "
                "El pronostico usa clima estimado desde el historico local."
            )
        historical_mean = float(df['EVENTOS'].astype(float).mean())
        # Umbrales calculados solo sobre el período de entrenamiento (sin fuga de test ni in-sample del test)
        train_end_date = str((metadata_aug or {}).get('train_end_date', ''))
        train_mask = (
            df['FECHA_DIA'].astype(str) <= train_end_date
            if train_end_date
            else pd.Series([True] * len(df), index=df.index)
        )
        df_train = df[train_mask]
        train_predictions = df_train['PRED_EVENTOS_PRIMARY'].astype(float)
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
                    "incendios": "Inc.",
                    "rescates": "Res.",
                    "climaticas": "Clim.",
                    "otros": "Otros",
                }
                status_arrows = {
                    "activity-low": "↓",
                    "activity-normal": "–",
                    "activity-high": "↑",
                    "activity-alert": "↑↑",
                }
                mix_rows = []
                for group_name in ["incendios", "rescates", "climaticas", "otros"]:
                    group = composition["groups"].get(group_name)
                    if not group:
                        continue
                    if group_name in risk_status_by_group:
                        change_label, change_class, risk_probability = (
                            risk_status_by_group[group_name]
                        )
                        status_detail = (
                            f"prob. actividad alta "
                            f"{probability_percent(risk_probability)}"
                        )
                    else:
                        change_label, change_class = category_change_level(
                            group["count"], group["baseline"]
                        )
                        status_detail = f"base habitual {group['baseline']:.1f}"
                    short_label = mix_labels[group_name]
                    status_arrow = status_arrows[change_class]
                    mix_rows.append(
                        '<div style="min-width: 0; text-align: center;">'
                        f'<div class="metric-delta {change_class}" '
                        f'title="{group["label"]}: {change_label} · '
                        f'{status_detail}" style="margin: 0; font-size: 0.58rem; '
                        f'padding: 3px 3px; display: flex; align-items: center; '
                        f'justify-content: center; gap: 0.16rem; white-space: nowrap;">'
                        f'<span>{short_label}</span><strong>{group["count"]:.1f}</strong>'
                        f'<span style="font-size: 0.7rem; font-weight: 900;">'
                        f'{status_arrow}</span></div></div>'
                    )
                category_mix_html = (
                    '<div title="Llamados esperados por tipo; el color sigue la probabilidad calibrada" '
                    'style="display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); '
                    'gap: 0.18rem; width: 100%; margin: 0.1rem 0 0.65rem;">'
                    + ''.join(mix_rows) + '</div>'
                )
            forecast_cards.append(
                f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 0.8rem; text-align: center; min-width: 0;">
                    <div style="font-size: 0.72rem; color: var(--text-muted); font-weight: 700; text-transform: uppercase;">{p['Dia']}</div>
                    <div style="font-size: 0.8rem; font-weight: 600; color: var(--text); margin-bottom: 0.4rem;">{p['Fecha'].strftime('%d-%b')}</div>
                    <div style="font-size: 1.3rem; font-weight: 800; color: var(--text); margin-bottom: 0.1rem;">{p['Prediccion']:.1f}</div>
                    <div style="font-size: 0.62rem; color: var(--text-muted); margin-bottom: 0.5rem;">llamadas</div>
                    <div style="display: flex; flex-direction: column; align-items: center; gap: 0.35rem; margin-bottom: 0.6rem;">
                        <div class="metric-delta {activity_class}" style="margin: 0; font-size: 0.62rem; padding: 2px 6px;">{activity_text}</div>
                    </div>
                    {category_mix_html}
                    <hr style="border-color: var(--border); margin: 0.5rem 0; opacity: 0.5;" />
                    <div style="font-size: 0.68rem; color: var(--text-muted); line-height: 1.45; text-align: left;">
                        <div style="font-size: 0.61rem; color: var(--text); font-weight: 800; text-transform: uppercase; margin-bottom: 0.22rem;">Prob. actividad alta</div>
                        &#128663; R.Vehicular: <strong>{probability_percent(p.get('Prob_RVehicular_Alto', np.nan))}</strong> <span class="metric-delta {rescue_class}" style="margin: 0; font-size: 0.58rem; padding: 1px 5px;">{rescue_label}</span><br/>
                        &#128293; Incendio: <strong>{probability_percent(p.get('Prob_Incendio_Alto', np.nan))}</strong> <span class="metric-delta {fire_class}" style="margin: 0; font-size: 0.58rem; padding: 1px 5px;">{fire_label}</span><br/>
                        &#9928;&#65039; Climáticas: <strong>{probability_percent(p.get('Prob_Climaticas_Alto', np.nan))}</strong> <span class="metric-delta {climate_class}" style="margin: 0; font-size: 0.58rem; padding: 1px 5px;">{climate_label}</span><br/>
                        <hr style="border-color: var(--border); margin: 0.5rem 0; opacity: 0.35;" />
                        &#127777;&#65039; Max: <strong>{p['Temp_Max']:.1f}&deg;C</strong><br/>
                        &#127777;&#65039; Media: <strong>{p['Temp_Media']:.1f}&deg;C</strong><br/>
                        &#128167; Humedad: <strong>{p['Hum_Media']:.0f}%</strong><br/>
                        &#128168; Viento medio: <strong>{p['Viento_Medio']:.1f} km/h</strong><br/>
                        &#128168; Ráfaga media: <strong>{p['Rafaga_Media']:.1f} km/h</strong><br/>
                        &#127783;&#65039; Lluvia: <strong>{p['Lluvia']:.1f} mm</strong>
                    </div>
                </div>"""
            )
        st.markdown(
            f"""<div class="responsive-grid responsive-grid-6">
                {''.join(forecast_cards)}
            </div>""",
            unsafe_allow_html=True,
        )
        current_model_name = format_model_name(metadata_aug.get("validation_protocol", "Modelo Climático Aumentado"))
        suffix = model_algorithm_suffix(metadata_aug)
        st.markdown(f"**Modelo oficial:** {current_model_name}{suffix} · NUEVO")
        st.caption(
            "Nivel operacional: Baja <4 · Normal 4–<6 · "
            "Alta 6–<8 · Muy alta ≥8 llamados."
        )

        percentile_summary_html = render_percentile_table([
            build_percentile_row("Nivel de actividad (predicciones)", train_predictions, as_probability=False),
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
                
def render_model_metrics(metadata, title, color):
    if metadata is None:
        st.info(f"{title}: metricas no disponibles.")
        return

    def number_value(key):
        if key == "rmse":
            if "rmse" in metadata:
                val = metadata["rmse"]
            elif "mse" in metadata:
                val = metadata["mse"]
                try:
                    return float(val) ** 0.5
                except:
                    return None
            else:
                return None
        elif key == "baseline_rmse":
            if "baseline_rmse" in metadata:
                val = metadata["baseline_rmse"]
            elif "baseline_mse" in metadata:
                val = metadata["baseline_mse"]
                try:
                    return float(val) ** 0.5
                except:
                    return None
            else:
                return None
        
        if key not in metadata:
            return None
        try:
            value = float(metadata[key])
        except (TypeError, ValueError):
            return None
        if not np.isfinite(value):
            return None
        return value

    metric_specs = [
        ("MAE", "mae", lambda value: f"{value:.2f} eventos"),
        ("RMSE", "rmse", lambda value: f"{value:.2f}"),
        ("R2", "r2", lambda value: f"{value * 100:.1f}%"),
        ("Ratio variabilidad", "std_ratio", lambda value: f"{value * 100:.1f}%"),
        ("Salidas 4-5", "boring_4_5_share", lambda value: f"{value * 100:.1f}%"),
        ("Precision top 20%", "top20_precision", lambda value: f"{value * 100:.1f}%"),
        ("Recall top 20%", "top20_recall", lambda value: f"{value * 100:.1f}%"),
        ("Brier", "brier", lambda value: f"{value:.3f}"),
        ("ROC-AUC", "roc_auc", lambda value: f"{value * 100:.1f}%"),
        ("Accuracy", "accuracy", lambda value: f"{value * 100:.1f}%"),
        ("Precision", "precision", lambda value: f"{value * 100:.1f}%"),
        ("Recall", "recall", lambda value: f"{value * 100:.1f}%"),
        ("F1-Score", "f1", lambda value: f"{value * 100:.1f}%"),
    ]

    rows = []

    for label, key, formatter in metric_specs:
        value = number_value(key)
        rows.append((label, formatter(value) if value is not None else "N/A"))

    principal = " · PRINCIPAL" if metadata.get("is_primary") else ""
    table_rows = "".join(
        f'<tr style="border-bottom: 1px solid rgba(128,128,128,.15);">'
        f'<td style="color: var(--text-muted); padding: .3rem 0;">{label}</td>'
        f'<td style="text-align: right; font-weight: 700; padding: .3rem 0;">{value}</td></tr>'
        for label, value in rows
    )
    st.markdown(
        f"""<div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem;">
        <div style="font-weight: 700; border-bottom: 2px solid {color}; padding-bottom: .4rem; margin-bottom: .8rem;">{title}{principal}</div>
        <table style="width: 100%; border-collapse: collapse; font-size: .82rem;">{table_rows}</table>
        </div>""",
        unsafe_allow_html=True,
    )


def build_constant_regressor_metadata(df_source, source_metadata, kind):
    if df_source is None or source_metadata is None:
        return None

    value_key = "train_target_mean" if kind == "mean" else "train_target_median"
    mae_key = "baseline_mae" if kind == "mean" else "median_baseline_mae"
    mse_key = "baseline_mse" if kind == "mean" else "median_baseline_mse"

    if value_key not in source_metadata:
        return None
    prediction_value = float(source_metadata[value_key])
    baseline_metadata = {
        "validation_protocol": "Baseline regresor constante",
        "metrics_scope": "regression_only",
        "prediction_value": prediction_value,
        "mae": source_metadata.get(mae_key),
        "mse": source_metadata.get(mse_key),
        "rmse": (float(source_metadata[mse_key]) ** 0.5) if source_metadata.get(mse_key) is not None else None,
    }

    if "FECHA_DIA" in df_source.columns and "EVENTOS" in df_source.columns:
        dates = pd.to_datetime(df_source["FECHA_DIA"], errors="coerce")
        start = pd.to_datetime(source_metadata.get("test_start_date"), errors="coerce")
        end = pd.to_datetime(source_metadata.get("test_end_date"), errors="coerce")
        if pd.notna(start) and pd.notna(end):
            mask = dates.between(start, end)
        else:
            test_samples = int(source_metadata.get("test_samples", 0) or 0)
            mask = pd.Series(False, index=df_source.index)
            if test_samples > 0:
                mask.iloc[-test_samples:] = True

        y_test = df_source.loc[mask, "EVENTOS"].astype(float)
        if not y_test.empty:
            pred = np.full(len(y_test), prediction_value)
            residual = y_test.to_numpy() - pred
            baseline_metadata["mae"] = float(np.mean(np.abs(residual)))
            baseline_metadata["mse"] = float(np.mean(residual ** 2))
            baseline_metadata["rmse"] = float(np.mean(residual ** 2) ** 0.5)
            denom = float(np.sum((y_test.to_numpy() - y_test.mean()) ** 2))
            baseline_metadata["r2"] = (
                float(1 - np.sum(residual ** 2) / denom) if denom > 0 else 0.0
            )

    return baseline_metadata


def build_operational_display_metadata(df_source, source_metadata):
    if source_metadata is None:
        return None
    display_metadata = dict(source_metadata)
    display_metadata["metrics_scope"] = "regression_only"
    return display_metadata


def build_classification_metadata(source_metadata):
    if source_metadata is None:
        return None
    classification_metadata = dict(source_metadata)
    classification_metadata["metrics_scope"] = "classification_only"
    return classification_metadata


def render_metric_explanations():
    st.markdown(
        """
        <div style="background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 1rem; margin-top: 1rem;">
            <div style="font-weight: 700; color: var(--text); margin-bottom: 0.55rem;">Guia breve de metricas</div>
            <div style="font-size: 0.78rem; line-height: 1.55; color: var(--text-muted);">
                <p><strong>MAE:</strong> error absoluto medio. Indica cuantos eventos se equivoca el modelo en promedio.</p>
                <p><strong>RMSE:</strong> raíz del error cuadrático medio. Penaliza más fuerte los errores grandes, en la misma escala que la variable objetivo.</p>
                <p><strong>R2:</strong> mejora frente a usar una media historica. Cerca de 0 significa que el conteo diario sigue teniendo mucho ruido.</p>
                <p><strong>ROC-AUC:</strong> capacidad de ordenar dias riesgosos sobre dias normales; 0.50 equivale a azar y 1.00 seria separacion perfecta.</p>
                <p><strong>Brier:</strong> error de calibracion de probabilidades. Menor es mejor: castiga probabilidades confiadas que se equivocan.</p>
                <p><strong>Matriz de confusion:</strong> TP = alerta correcta, TN = normal correcto, FP = falsa alerta, FN = alerta omitida.</p>
                <p><strong>Accuracy:</strong> proporcion de aciertos totales. Formula: <code>(TP + TN) / (TP + TN + FP + FN)</code>.</p>
                <p><strong>Precision:</strong> de las alertas emitidas, cuantas fueron correctas. Formula: <code>TP / (TP + FP)</code>.</p>
                <p><strong>Recall:</strong> de los dias realmente criticos, cuantos detecto el modelo. Formula: <code>TP / (TP + FN)</code>.</p>
                <p><strong>F1-Score:</strong> equilibrio entre precision y recall. Formula: <code>2 * Precision * Recall / (Precision + Recall)</code>.</p>
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
        '<div class="chart-subtitle">Desempeño y estructura del modelo oficial utilizado actualmente en el Forecast diario.</div>',
        unsafe_allow_html=True,
    )
    stats_model_name = (
        f"{format_model_name((metadata_aug or {}).get('validation_protocol', 'Modelo oficial'))}"
        f"{model_algorithm_suffix(metadata_aug)} · OFICIAL"
    )
    st.info("Ensemble hidrometeorológico validado con seis bloques temporales walk-forward de 120 días.")

    col_stats_forecast = st.columns(1)[0]
    with col_stats_forecast:
        if metadata_aug is not None:
            render_model_metrics(
                metadata_aug,
                stats_model_name,
                "#e11d48",
            )
        else:
            st.info("Métricas del modelo oficial no disponibles.")

    render_metric_explanations()

    st.markdown(
        '<div class="chart-title" style="margin-top: 1.25rem;">Importancia de Variables</div>',
        unsafe_allow_html=True,
    )
    col_imp_forecast = st.columns(1)[0]
    with col_imp_forecast:
        if df_imp_aug is not None and not df_imp_aug.empty:
            render_importance_chart(
                df_imp_aug,
                f"Importancia · {stats_model_name}",
                "#e11d48",
                key="importance_forecast",
            )
        else:
            st.info("No hay importancia de variables disponible.")


if False:  # Vista comparativa antigua, conservada temporalmente como referencia.
    with st.container():

        st.markdown('<div class="importance-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Desempeño Comparativo e Importancia Relativa Completa de Variables</div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Comparación entre Modelo Climático, Modelo Climático con Inercia y Modelo Climático Aumentado.</div>', unsafe_allow_html=True)
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
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #3b82f6; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Modelo Climático</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_base:.2f} eventos</td></tr>
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
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #10b981; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Modelo Climático con Inercia</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_aug:.2f} eventos</td></tr>
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
            <div style="font-weight: 700; font-size: 0.9rem; color: var(--text); border-bottom: 2px solid #8b5cf6; padding-bottom: 0.4rem; margin-bottom: 0.8rem;">Métricas Climático Aumentado (Principal)</div>
            <table style="width: 100%; border-collapse: collapse; font-size: 0.82rem; line-height: 1.6;">
                <tr style="border-bottom: 1px solid rgba(128, 128, 128, 0.15);"><td style="color: var(--text-muted); font-weight: 500; padding: 0.3rem 0;">Error Absoluto Medio (MAE)</td><td style="text-align: right; color: var(--text); font-weight: 700; padding: 0.3rem 0;">{mae_v3:.2f} eventos</td></tr>
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
                <li style="margin-bottom: 0.5rem;"><strong>Error Absoluto Medio (MAE):</strong> Desviación promedio esperada de la predicción. Un valor de 2.5 indica que, en promedio, el pronóstico del modelo acierta con una diferencia de <strong>&plusmn;2.5 eventos</strong> respecto a la realidad.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Error Cuadrático Medio (MSE):</strong> Promedio de los errores elevados al cuadrado. Al elevarlos al cuadrado, penaliza y destaca con mayor rigor las fallas graves de predicción.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Coeficiente R² (Regresión):</strong> Mide la mejora del modelo en comparación con usar un promedio histórico fijo.
                <br/><em>¿Por qué es negativo o cercano a cero?</em> La demanda de emergencias de bomberos diaria posee una aleatoriedad intrínseca extrema. Un R² ligeramente negativo o muy bajo (-0.7% o -0.3%) en la evaluación indica que predecir el número puntual exacto tiene tanto "ruido" que el promedio histórico comete un error similar, lo que demuestra la dificultad de acertar el número exacto, aunque el modelo sea excelente en capturar tendencias y días críticos.</li>
            </ul>
            <p style="margin-top: 1rem;"><strong>2. Metricas del Modelo de Clasificacion (probabilidad de alerta por percentiles historicos):</strong></p>
            <ul>
                <li style="margin-bottom: 0.5rem;"><strong>Umbral de Clasificacion Calibrado:</strong> La comunicacion operacional usa percentiles historicos de probabilidad: <strong>Bajo bajo p33</strong>, <strong>Normal p33-p66</strong>, <strong>Alto sobre p66</strong> y <strong>Alerta sobre p80</strong>.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Exactitud (Accuracy):</strong> El porcentaje de días totales (tanto normales como críticos) en los que el clasificador del modelo acertó el estado de alerta correcto.</li>
                <li style="margin-bottom: 0.5rem;"><strong>Precisión (Precision):</strong> De todos los días en los que el modelo emitió una alerta de día crítico, cuántos lo fueron realmente. Un 25% indica que 1 de cada 4 alertas preventivas es un día crítico real (tasa óptima y segura para logística de bomberos).</li>
                <li style="margin-bottom: 0.5rem;"><strong>Sensibilidad (Recall):</strong> Qué porcentaje de los días críticos reales que ocurrieron logró anticipar y alertar el modelo. Un 70.4% significa que el modelo capta y advierte con éxito el 70% de las situaciones críticas reales.</li>
                <li style="margin-bottom: 0.5rem;"><strong>F1-Score:</strong> Balance de equilibrio matemático entre la Precisión y la Sensibilidad. Es la métrica estándar más robusta para evaluar modelos con datos altamente desbalanceados.</li>
            </ul>
        </div>""", unsafe_allow_html=True)
    col_imp1, col_imp2, col_imp3 = st.columns(3)
    with col_imp1:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Modelo Climático</div>', unsafe_allow_html=True)
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
        st.plotly_chart(fig_imp, use_container_width=True, config={"displayModeBar": False}, key="stats_imp_base")
    with col_imp2:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Modelo Climático con Inercia</div>', unsafe_allow_html=True)
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
        st.plotly_chart(fig_imp_agn, use_container_width=True, config={"displayModeBar": False}, key="stats_imp_agn")
    with col_imp3:
        st.markdown('<div style="text-align: center; margin-bottom: 0.5rem; font-weight: 700; font-size: 0.85rem; color: var(--text); margin-top: 1rem;">Importancia Relativa - Climático Aumentado (Principal)</div>', unsafe_allow_html=True)
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
        st.plotly_chart(fig_imp_v3, use_container_width=True, config={"displayModeBar": False}, key="stats_imp_v3")
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
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_MAX, VIENTO_MEDIO, WX_GUST_MAX, WX_GUST_MEAN:</strong> Velocidad sostenida máxima, promedio, ráfaga máxima y ráfaga media estimadas en km/h. La tarjeta de Forecast muestra la ráfaga media por separado; es uno de los factores de propagación y caídas de árboles más críticos en la península.</li>
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_SKEW / VIENTO_KURT:</strong> Asimetría y curtosis de la velocidad del viento calculadas sobre las 24 lecturas horarias. Capturan ráfagas o rachas extremas de viento de corta duración (picos de asimetría y colas anchas de curtosis) que las medias diarias ocultan por completo.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA:</strong> Cantidad total de precipitaciones proyectadas en milímetros (mm). Actúa como un supresor directo de incendios forestales pero incrementa accidentes y problemas en techos.</li>
                <li style="margin-bottom: 0.4rem;"><strong>Contribución al Coeficiente R² Positivo:</strong> Juntas, estas 6 variables de forma de los perfiles horarios meteorológicos acumulan el <strong>16.54%</strong> del peso total en el Modelo Climático con Inercia de Actividad. Al capturar la dinámica intra-diaria fina del clima, permiten que el modelo supere el ruido intrínseco y logre un R² de <strong>+1.3%</strong> en test (comparado con el -1.4% del modelo base).</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">4. Historial Meteorológico de Corto Plazo (Lags Climáticos):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>VIENTO_MEDIO_lag_1:</strong> Velocidad promedio del viento registrada el día anterior. Es el predictor individual con mayor peso en el algoritmo, debido a la persistencia del comportamiento atmosférico local.</li>
                <li style="margin-bottom: 0.4rem;"><strong>HUM_MEDIA_lag_1:</strong> Humedad relativa promedio del día anterior, capturando la desecación acumulada del suelo y aire.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_LAG_1D, LLUVIA_LAG_2D, LLUVIA_LAG_3D:</strong> Precipitación de ayer, hace 2 días y hace 3 días.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_TOTAL_3D_PREV:</strong> Lluvia total de los 3 días anteriores.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_PROMEDIO_7D_PREV:</strong> Promedio diario de lluvia durante los 7 días anteriores.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_MAX_30D_PREV:</strong> Mayor precipitación diaria registrada durante los 30 días anteriores.</li>
                <li style="margin-bottom: 0.4rem;"><strong>LLUVIA_DESV_30D_PREV:</strong> Variabilidad de la precipitación diaria durante los 30 días anteriores.</li>
                <li style="margin-bottom: 0.4rem;"><strong>DIAS_SECOS_30D_PREV:</strong> Cantidad de días con lluvia menor o igual a 0.1 mm durante los 30 días anteriores.</li>
            </ul>
            <div style="font-weight: 700; margin-top: 0.6rem; color: var(--text);">5. Factores de Calendario y Ciclos Temporales (Descartados):</div>
            <ul style="margin: 0; padding-left: 1.2rem;">
                <li style="margin-bottom: 0.4rem;"><strong>MES, DIA_SEMANA, DANO_SIN, DANO_COS, etc.:</strong> *Nota: Estas variables de calendario y estacionalidad fija fueron descartadas por completo en favor de una aproximación puramente agnóstica basada en clima y lags de actividad, reduciendo el riesgo de sobreajuste y garantizando adaptabilidad al cambio climático.*</li>
            </ul>
        </div>
    </div>""", unsafe_allow_html=True)


with tab_history:
    st.markdown('<div class="chart-anchor"></div>', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Histórico Real versus Predicción</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="chart-subtitle">Serie cronológica del modelo principal v0617. '
        'Los filtros de año, mes y día de la semana pueden combinarse libremente.</div>',
        unsafe_allow_html=True,
    )
    render_historical_chart()
    render_distribution_charts()


with tab_seasonal:
    with st.container():
        principal_variant = {
            'full': 'Full',
            'pruned': 'Optimizado',
            'category_blend': 'Optimizado por categorías',
        }.get(metadata_v3.get('selected_variant'), 'Principal')
        st.markdown('<div class="chart-anchor"></div>', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Curva Estacional del Modelo Principal</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div class="chart-subtitle">Eventos reales versus predicciones históricas del modelo {principal_variant} v0617, seleccionado automáticamente por desempeño.</div>',
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
