"""
Prediccion de incidentes diarios - CBT Talcahuano
Features: dia_semana, mes, tmax, tmin, humedad, viento
"""
import sys
import os
import warnings

os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('OMP_NUM_THREADS', '1')
os.environ.setdefault('MKL_NUM_THREADS', '1')

if sys.platform == 'win32':
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pandas as pd
import numpy as np
import requests
import joblib
import warnings
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from feriados_cl import cargar_feriados

warnings.filterwarnings('ignore')

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'diario_log.txt')
_log_fh = open(LOG_FILE, 'w', encoding='utf-8')

def log(msg=''):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', errors='replace').decode())
    _log_fh.write(msg + '\n')
    _log_fh.flush()

try:
    log(f"XGBoost {'disponible ' + xgb.__version__ if HAS_XGB else 'NO disponible'}")

    # ─── 1. Carga de emergencias ──────────────────────────────────────────────────
    log("\n=== 1. Carga y agregacion diaria de emergencias ===")
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '02_data')

    tweets = pd.read_csv(os.path.join(DATA_DIR, 'tweets_procesados.csv'), sep=';')
    tweets['Fecha'] = pd.to_datetime(tweets['Fecha'], utc=True)
    tweets['fecha_local'] = tweets['Fecha'].dt.tz_convert('America/Santiago')
    tweets['fecha'] = tweets['fecha_local'].dt.date

    emerg_diaria = (
        tweets.groupby('fecha')
        .size()
        .rename('n_incidentes')
        .reset_index()
    )
    emerg_diaria['fecha'] = pd.to_datetime(emerg_diaria['fecha'])

    # Rellenar dias sin emergencias
    idx_dias = pd.date_range(
        start=emerg_diaria['fecha'].min(),
        end=emerg_diaria['fecha'].max(),
        freq='D'
    )
    df = (
        pd.DataFrame({'fecha': idx_dias})
        .merge(emerg_diaria, on='fecha', how='left')
        .fillna({'n_incidentes': 0})
    )
    df['n_incidentes'] = df['n_incidentes'].astype(int)

    log(f"Rango: {df['fecha'].min().date()} -> {df['fecha'].max().date()}")
    log(f"Dias totales:          {len(df):,}")
    log(f"Dias con incidentes:   {(df['n_incidentes'] > 0).sum():,} ({(df['n_incidentes'] > 0).mean()*100:.1f}%)")
    log(f"Media incidentes/dia:  {df['n_incidentes'].mean():.2f}")
    log(f"Mediana:               {df['n_incidentes'].median():.0f}")
    log(f"Max incidentes/dia:    {df['n_incidentes'].max()}")
    log(f"Distribucion:")
    for v, cnt in df['n_incidentes'].value_counts().sort_index().items():
        if v <= 15:
            log(f"  {v:>3} incidentes: {cnt:>4} dias  {'█' * int(cnt/5)}")

    # ─── 2. Clima diario (Open-Meteo) ────────────────────────────────────────────
    log("\n=== 2. Datos climaticos diarios (Open-Meteo) ===")

    def fetch_weather_daily(start_date, end_date, lat=-36.731106, lon=-73.11023):
        from io import StringIO
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,"
            f"relative_humidity_2m_mean,wind_speed_10m_max,"
            f"wind_gusts_10m_max,precipitation_sum,wind_speed_10m_mean"
            f"&timezone=America%2FSantiago&format=csv"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df_w = pd.read_csv(StringIO(resp.text), skiprows=3)
        df_w.columns = ['fecha', 'tmax', 'tmin', 'hum_media',
                        'viento_max', 'racha_max', 'precip_total', 'viento_medio']
        df_w['fecha'] = pd.to_datetime(df_w['fecha'])
        return df_w

    fecha_ini = df['fecha'].min().strftime('%Y-%m-%d')
    fecha_fin = (pd.Timestamp.now() - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    log(f"Descargando clima diario: {fecha_ini} -> {fecha_fin} ...")
    try:
        weather_d = fetch_weather_daily(fecha_ini, fecha_fin)
        log(f"OK: {len(weather_d):,} dias de clima")
        log(f"  tmax:   {weather_d['tmax'].min():.1f} - {weather_d['tmax'].max():.1f} °C")
        log(f"  tmin:   {weather_d['tmin'].min():.1f} - {weather_d['tmin'].max():.1f} °C")
        log(f"  hum:    {weather_d['hum_media'].min():.0f} - {weather_d['hum_media'].max():.0f} %")
        log(f"  viento: {weather_d['viento_max'].min():.1f} - {weather_d['viento_max'].max():.1f} km/h")
    except Exception as e:
        log(f"Error clima: {e} — usando solo features temporales.")
        weather_d = pd.DataFrame(columns=['fecha', 'tmax', 'tmin', 'hum_media',
                                          'viento_max', 'racha_max', 'precip_total', 'viento_medio'])

    df = df.merge(weather_d, on='fecha', how='left')

    # ─── 3. Feriados ─────────────────────────────────────────────────────────────
    log("\n=== 3. Feriados ===")
    fechas_feriado = cargar_feriados(
        os.path.join(DATA_DIR, 'independent_variables', 'feriados.xlsx'),
        anios=range(2021, 2027),
        log_fn=log,
    )

    # ─── 4. Feature Engineering ──────────────────────────────────────────────────
    log("\n=== 4. Feature Engineering ===")

    df['dia_semana']    = df['fecha'].dt.dayofweek      # 0=lunes, 6=domingo
    df['mes']           = df['fecha'].dt.month
    df['anio']          = df['fecha'].dt.year
    df['semana_anio']   = df['fecha'].dt.isocalendar().week.astype(int)
    df['trimestre']     = df['fecha'].dt.quarter
    df['dia_mes']       = df['fecha'].dt.day
    df['es_fin_semana'] = (df['dia_semana'] >= 5).astype(int)
    df['es_verano']     = df['mes'].isin([12, 1, 2]).astype(int)
    df['es_feriado']    = df['fecha'].dt.date.astype(str).isin(
                            {d.isoformat() for d in fechas_feriado}).astype(int)

    # Encodings ciclicos
    df['dia_sem_sin']   = np.sin(2 * np.pi * df['dia_semana'] / 7)
    df['dia_sem_cos']   = np.cos(2 * np.pi * df['dia_semana'] / 7)
    df['mes_sin']       = np.sin(2 * np.pi * df['mes'] / 12)
    df['mes_cos']       = np.cos(2 * np.pi * df['mes'] / 12)

    # Interacciones clave
    df['tamp_termica']  = df['tmax'] - df['tmin']       # amplitud termica diaria
    df['lluvia_bin']    = (df['precip_total'] > 1).astype(int)

    COLS_CLIMA = ['tmax', 'tmin', 'hum_media', 'viento_max', 'racha_max',
                  'precip_total', 'viento_medio', 'tamp_termica', 'lluvia_bin']
    COLS_TEMP  = ['dia_semana', 'mes', 'anio', 'semana_anio', 'trimestre', 'dia_mes',
                  'es_fin_semana', 'es_verano', 'es_feriado',
                  'dia_sem_sin', 'dia_sem_cos', 'mes_sin', 'mes_cos']
    TARGET = 'n_incidentes'

    df = df.sort_values('fecha').reset_index(drop=True)

    # Lags de incidentes
    df['lag_1d']         = df[TARGET].shift(1)
    df['lag_7d']         = df[TARGET].shift(7)
    df['roll_mean_7d']   = df[TARGET].shift(1).rolling(7, min_periods=4).mean()
    df['roll_mean_30d']  = df[TARGET].shift(1).rolling(30, min_periods=14).mean()

    # Lluvia de los N dias anteriores (1, 2, 3)
    lluvia = (df['precip_total'] > 1).astype(int)
    for n in [1, 2, 3]:
        df[f'lluvia_lag_{n}d'] = lluvia.shift(n)

    # Feriado en los proximos N dias (1 a 7) — util para anticipar demanda
    fechas_feriado_str = {d.isoformat() for d in fechas_feriado}
    for n in range(1, 8):
        df[f'feriado_en_{n}d'] = (
            (df['fecha'] + pd.Timedelta(days=n))
            .dt.strftime('%Y-%m-%d')
            .isin(fechas_feriado_str)
            .astype(int)
        )

    COLS_LAG      = ['lag_1d', 'lag_7d', 'roll_mean_7d', 'roll_mean_30d',
                     'lluvia_lag_1d', 'lluvia_lag_2d', 'lluvia_lag_3d']
    COLS_FERIADO  = [f'feriado_en_{n}d' for n in range(1, 8)]
    COLS_TEMP     = COLS_TEMP + COLS_FERIADO

    all_features = COLS_TEMP + COLS_CLIMA + COLS_LAG
    features = [f for f in all_features if f in df.columns and df[f].notna().mean() > 0.30]

    log(f"Features incluidas: {len(features)}")
    for f in features:
        tag = "[LAG]  " if f in COLS_LAG else ("[CLIMA]" if f in COLS_CLIMA else "[TEMP] ")
        log(f"  {tag} {f:<22}  NaN: {df[f].isna().mean()*100:.1f}%")

    df_ml = df[['fecha', TARGET] + features].dropna()
    log(f"\nDias totales: {len(df):,}  |  tras dropna: {len(df_ml):,} ({len(df_ml)/len(df)*100:.1f}%)")

    # ─── 5. Distribucion por dia y mes ───────────────────────────────────────────
    log("\n=== 5. Patrones diarios ===")
    dias_lbl = ['Lun','Mar','Mie','Jue','Vie','Sab','Dom']
    meses_lbl = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

    log("  Media de incidentes por dia de la semana:")
    for d in range(7):
        m = df_ml['dia_semana'] == d
        media = df_ml.loc[m, TARGET].mean()
        log(f"    {dias_lbl[d]}: {media:.2f}  {'█' * int(media)}")
    log("  Media de incidentes por mes:")
    for m in range(1, 13):
        mask = df_ml['mes'] == m
        media = df_ml.loc[mask, TARGET].mean()
        log(f"    {meses_lbl[m-1]:>3}: {media:.2f}  {'█' * int(media)}")

    # ─── 6. Train / Test split ───────────────────────────────────────────────────
    log("\n=== 6. Train / Test split (80/20 temporal) ===")
    split = int(len(df_ml) * 0.80)
    df_train = df_ml.iloc[:split]
    df_test  = df_ml.iloc[split:]
    X_train, y_train = df_train[features], df_train[TARGET]
    X_test,  y_test  = df_test[features],  df_test[TARGET]
    log(f"Train: {len(X_train):,} dias  ({df_train['fecha'].min().date()} -> {df_train['fecha'].max().date()})")
    log(f"Test:  {len(X_test):,}  dias  ({df_test['fecha'].min().date()}  -> {df_test['fecha'].max().date()})")
    log(f"Media train: {y_train.mean():.2f}  |  Media test: {y_test.mean():.2f}")

    # ─── 7. Funciones de evaluacion ──────────────────────────────────────────────
    def evaluar(nombre, y_true, y_pred, lst):
        y_true = np.asarray(y_true, dtype=float)
        y_pred = np.clip(np.asarray(y_pred, dtype=float), 0, None)
        mae    = mean_absolute_error(y_true, y_pred)
        rmse   = np.sqrt(mean_squared_error(y_true, y_pred))
        r2     = r2_score(y_true, y_pred)
        # Porcentaje de dias con prediccion dentro de ±2 incidentes
        within_2 = np.mean(np.abs(y_true - y_pred) <= 2) * 100
        log(f"\n-- {nombre} --")
        log(f"  MAE:              {mae:.3f}  incidentes/dia")
        log(f"  RMSE:             {rmse:.3f}")
        log(f"  R2:               {r2:.4f}")
        log(f"  Dentro de ±2:     {within_2:.1f}%  (dias con error <= 2 incidentes)")
        lst.append({'modelo': nombre, 'MAE': round(mae, 3), 'RMSE': round(rmse, 3),
                    'R2': round(r2, 4), 'Dentro_2_pct': round(within_2, 1)})
        return y_pred

    def cv_temporal(modelo_fn, X, y, n_splits=5, label=''):
        tscv = TimeSeriesSplit(n_splits=n_splits)
        mae_lst, r2_lst = [], []
        for tr_idx, te_idx in tscv.split(X):
            m = modelo_fn()
            m.fit(X.iloc[tr_idx], y.iloc[tr_idx])
            yp = np.clip(m.predict(X.iloc[te_idx]), 0, None)
            mae_lst.append(mean_absolute_error(y.iloc[te_idx], yp))
            r2_lst.append(r2_score(y.iloc[te_idx], yp))
        log(f"  CV (5-fold TimeSeriesSplit):")
        log(f"    MAE: {np.mean(mae_lst):.3f} ± {np.std(mae_lst):.3f}  (folds: {[f'{v:.2f}' for v in mae_lst]})")
        log(f"    R2:  {np.mean(r2_lst):.4f} ± {np.std(r2_lst):.4f}")

    def log_importancia(imp, feats, top_n=20):
        log(f"\n  Top-{top_n} features:")
        top = imp.sort_values(ascending=False).head(top_n)
        for feat, val in top.items():
            tag = "[LAG]  " if feat in COLS_LAG else ("[CLIMA]" if feat in COLS_CLIMA else "[TEMP] ")
            bar = "█" * int(val * 300)
            log(f"    {tag} {feat:<22}  {val:.4f}  {bar}")

    # ─── 8. Entrenamiento ────────────────────────────────────────────────────────
    log("\n=== 8. Entrenamiento de modelos ===")
    resultados = []
    predicciones = {}

    # Baseline: media por (dia_semana x mes)
    log("\n[Baseline] Media historica por dia_semana x mes...")
    media_base = df_train.groupby(['dia_semana', 'mes'])[TARGET].mean()
    y_pred_bl = (
        df_test[['dia_semana', 'mes']]
        .apply(lambda r: media_base.get((r['dia_semana'], r['mes']),
                                        y_train.mean()), axis=1)
        .values
    )
    predicciones['Baseline'] = evaluar('Baseline (media dia×mes)', y_test, y_pred_bl, resultados)

    # Random Forest
    log("\n[Random Forest]...")
    rf = RandomForestRegressor(
        n_estimators=500, max_depth=10, min_samples_leaf=5,
        max_features='sqrt', n_jobs=1, random_state=42,
    )
    rf.fit(X_train, y_train)
    predicciones['Random Forest'] = evaluar('Random Forest', y_test, rf.predict(X_test), resultados)
    cv_temporal(lambda: RandomForestRegressor(n_estimators=200, max_depth=10,
                                              min_samples_leaf=5, n_jobs=1, random_state=42),
                df_ml[features], df_ml[TARGET])
    log_importancia(pd.Series(rf.feature_importances_, index=features), features)

    # XGBoost Poisson
    if HAS_XGB:
        log("\n[XGBoost (Poisson)]...")
        xgb_m = xgb.XGBRegressor(
            objective='count:poisson',
            n_estimators=1000, max_depth=5, learning_rate=0.03,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
            reg_alpha=0.1, reg_lambda=1.0, n_jobs=-1, random_state=42,
            early_stopping_rounds=50, eval_metric='poisson-nloglik',
        )
        xgb_m.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=200)
        predicciones['XGBoost'] = evaluar('XGBoost (Poisson)', y_test, xgb_m.predict(X_test), resultados)
        cv_temporal(lambda: xgb.XGBRegressor(
            objective='count:poisson', n_estimators=400, max_depth=5,
            learning_rate=0.03, subsample=0.8, colsample_bytree=0.8,
            min_child_weight=3, n_jobs=-1, random_state=42,
        ), df_ml[features], df_ml[TARGET])
        log_importancia(pd.Series(xgb_m.feature_importances_, index=features), features)

    # ─── 9. Comparacion ──────────────────────────────────────────────────────────
    log("\n=== 9. Comparacion de modelos ===")
    df_res = pd.DataFrame(resultados)
    log("\n" + "="*65)
    log("RESUMEN — Prediccion de incidentes DIARIOS")
    log("="*65)
    log(df_res.to_string(index=False))
    mejor = df_res.loc[df_res['MAE'].idxmin(), 'modelo']
    log(f"\n-> Mejor modelo: {mejor}")
    log(f"   MAE: {df_res.loc[df_res['MAE'].idxmin(),'MAE']:.3f} incidentes/dia")
    log(f"   R2:  {df_res.loc[df_res['MAE'].idxmin(),'R2']:.4f}")

    y_best = predicciones[mejor if mejor in predicciones else list(predicciones.keys())[-1]]

    # ─── 10. Visualizaciones ─────────────────────────────────────────────────────
    log("\n=== 10. Visualizaciones ===")

    # EDA diario
    fig_eda = make_subplots(
        rows=2, cols=3,
        subplot_titles=[
            'Incidentes por dia de la semana', 'Incidentes por mes',
            'Distribucion de incidentes/dia',
            'Serie temporal mensual', 'Incidentes vs Temperatura max',
            'Incidentes vs Viento max',
        ]
    )
    dias_avg = df_ml.groupby('dia_semana')[TARGET].mean()
    fig_eda.add_trace(go.Bar(x=dias_lbl, y=dias_avg.values, marker_color='steelblue', name='dia'), row=1, col=1)
    mes_avg = df_ml.groupby('mes')[TARGET].mean()
    fig_eda.add_trace(go.Bar(x=meses_lbl, y=mes_avg.values, marker_color='coral', name='mes'), row=1, col=2)
    vc = df_ml[TARGET].value_counts().sort_index()
    fig_eda.add_trace(go.Bar(x=vc.index, y=vc.values, marker_color='mediumpurple', name='dist'), row=1, col=3)
    mensual = df_ml.set_index('fecha').resample('ME')[TARGET].sum().reset_index()
    fig_eda.add_trace(go.Scatter(x=mensual['fecha'].astype(str), y=mensual[TARGET],
                                 mode='lines', line_color='darkorange', name='mensual'), row=2, col=1)
    if 'tmax' in df_ml.columns:
        fig_eda.add_trace(go.Scatter(x=df_ml['tmax'], y=df_ml[TARGET],
                                     mode='markers', marker=dict(size=3, opacity=0.3, color='tomato'),
                                     name='tmax'), row=2, col=2)
        fig_eda.add_trace(go.Scatter(x=df_ml['viento_max'], y=df_ml[TARGET],
                                     mode='markers', marker=dict(size=3, opacity=0.3, color='teal'),
                                     name='viento'), row=2, col=3)
    fig_eda.update_layout(title='EDA — Incidentes diarios CBT Talcahuano', height=700, showlegend=False)
    fig_eda.write_html('eda_diario.html')
    log("  Guardado: eda_diario.html")

    # Predicciones vs real
    n_viz = min(365, len(df_test))
    fechas_viz = df_test['fecha'].astype(str).values[:n_viz]
    y_real_viz = y_test.values[:n_viz]
    y_pred_viz = y_best[:n_viz]

    fig_pred = make_subplots(rows=2, cols=1,
                             subplot_titles=['Real vs Predicho — primer año test',
                                             'Scatter: real vs predicho (test completo)'])
    fig_pred.add_trace(go.Scatter(x=fechas_viz, y=y_real_viz, name='Real',
                                  line_color='steelblue', fill='tozeroy', opacity=0.6), row=1, col=1)
    fig_pred.add_trace(go.Scatter(x=fechas_viz, y=y_pred_viz, name='Prediccion',
                                  line_color='crimson'), row=1, col=1)
    lim = max(y_test.max(), y_best.max()) + 1
    fig_pred.add_trace(go.Scatter(x=y_test.tolist(), y=y_best.tolist(),
                                  mode='markers', marker=dict(size=4, opacity=0.3, color='darkorange'),
                                  name='puntos'), row=2, col=1)
    fig_pred.add_trace(go.Scatter(x=[0, lim], y=[0, lim], mode='lines',
                                  line=dict(dash='dash', color='black'), name='ideal'), row=2, col=1)
    fig_pred.update_layout(title=f'Predicciones diarias — {mejor}', height=700)
    fig_pred.write_html('predicciones_diario.html')
    log("  Guardado: predicciones_diario.html")

    # Importancia de features (mejor modelo)
    if HAS_XGB and 'XGBoost' in mejor:
        imp = pd.Series(xgb_m.feature_importances_, index=features).sort_values(ascending=True)
    else:
        imp = pd.Series(rf.feature_importances_, index=features).sort_values(ascending=True)
    colors_imp = ['#2196F3' if f in COLS_LAG else ('#FF9800' if f in COLS_CLIMA else '#4CAF50')
                  for f in imp.index]
    fig_imp = go.Figure(go.Bar(x=imp.values, y=imp.index, orientation='h', marker_color=colors_imp))
    fig_imp.update_layout(title=f'Importancia de features — {mejor} (Prediccion diaria)',
                          height=max(500, len(features) * 20),
                          annotations=[dict(x=0.98, y=0.98, xref='paper', yref='paper',
                                           text='🟦 Lag  🟧 Clima  🟩 Temporal',
                                           showarrow=False, font=dict(size=12))])
    fig_imp.write_html('feature_importance_diario.html')
    log("  Guardado: feature_importance_diario.html")

    # ─── 11. Guardar artefactos ───────────────────────────────────────────────────
    log("\n=== 11. Guardar artefactos ===")
    os.makedirs('modelos', exist_ok=True)
    if HAS_XGB:
        xgb_m.save_model('modelos/xgb_diario.json')
    joblib.dump(rf,       'modelos/rf_diario.pkl')
    joblib.dump(features, 'modelos/features_diario.pkl')
    df_res.to_csv('resultados_diario.csv', index=False)
    df_ml.to_csv('dataset_diario_ml.csv', index=False)
    log("  Artefactos guardados en modelos/")

    log("\n=== Pipeline diario completado ===")
    log(f"Mejor modelo: {mejor}  |  MAE: {df_res.loc[df_res['MAE'].idxmin(),'MAE']:.3f}  |  R2: {df_res.loc[df_res['MAE'].idxmin(),'R2']:.4f}")

finally:
    _log_fh.close()
