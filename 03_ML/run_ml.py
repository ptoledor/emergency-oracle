"""
Pipeline ML: Prediccion de Emergencias por Hora - CBT Talcahuano
"""
import sys
import os
import warnings
import asyncio

# Must be set before numpy/scipy import to avoid native BLAS threading crashes on Windows
os.environ.setdefault('OPENBLAS_NUM_THREADS', '1')
os.environ.setdefault('OMP_NUM_THREADS', '1')
os.environ.setdefault('MKL_NUM_THREADS', '1')

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import pandas as pd
import numpy as np
import requests
import joblib
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from feriados_cl import cargar_feriados

from sklearn.linear_model import PoissonRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import (mean_absolute_error, mean_squared_error, r2_score,
                             f1_score, precision_score, recall_score, roc_curve, roc_auc_score)
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit

warnings.filterwarnings('ignore')

try:
    import xgboost as xgb
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run_log.txt')
_log_fh = open(LOG_FILE, 'w', encoding='utf-8')

def log(msg=''):
    _log_fh.write(msg + '\n')
    _log_fh.flush()


try:
    log(f"XGBoost {'disponible ' + xgb.__version__ if HAS_XGB else 'NO disponible'}")
    log("Imports OK")

    # ─── 1. Carga y agregacion horaria ───────────────────────────────────────────
    log("\n=== 1. Carga de datos ===")
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '02_data')

    tweets = pd.read_csv(os.path.join(DATA_DIR, 'tweets_procesados.csv'), sep=';')
    tweets['Fecha'] = pd.to_datetime(tweets['Fecha'], utc=True)
    tweets['fecha_hora'] = tweets['Fecha'].dt.floor('h').dt.tz_convert('America/Santiago')
    tweets['Fecha_local'] = tweets['Fecha'].dt.tz_convert('America/Santiago')

    log(f"Rango: {tweets['Fecha_local'].min().date()} -> {tweets['Fecha_local'].max().date()}")
    log(f"Total registros: {len(tweets):,}")

    emergencias_h = (
        tweets.groupby('fecha_hora')
        .size()
        .rename('n_emergencias')
        .reset_index()
    )

    idx_completo = pd.date_range(
        start=emergencias_h['fecha_hora'].min().floor('D'),
        end=emergencias_h['fecha_hora'].max().ceil('D'),
        freq='h',
        tz='America/Santiago'
    )
    df = (
        pd.DataFrame({'fecha_hora': idx_completo})
        .merge(emergencias_h, on='fecha_hora', how='left')
        .fillna({'n_emergencias': 0})
    )
    df['n_emergencias'] = df['n_emergencias'].astype(int)

    log(f"Horas totales: {len(df):,}")
    log(f"Horas con >=1 emerg: {(df['n_emergencias'] > 0).sum():,} ({(df['n_emergencias'] > 0).mean()*100:.1f}%)")
    log(f"Media emerg/hora: {df['n_emergencias'].mean():.4f}")
    log(f"Max emerg/hora:   {df['n_emergencias'].max()}")

    # ─── 2. Datos climaticos ──────────────────────────────────────────────────────
    log("\n=== 2. Datos climaticos (Open-Meteo) ===")

    def fetch_weather(start_date, end_date, lat=-36.731106, lon=-73.11023):
        from io import StringIO
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={lat}&longitude={lon}"
            f"&start_date={start_date}&end_date={end_date}"
            f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
            f"wind_gusts_10m,precipitation,weather_code"
            f"&timezone=America%2FSantiago&format=csv"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df_w = pd.read_csv(StringIO(resp.text), skiprows=3)
        df_w.columns = ['fecha_hora', 'temperatura', 'humedad', 'viento_vel',
                        'viento_racha', 'precipitacion', 'cod_clima']
        df_w['fecha_hora'] = pd.to_datetime(df_w['fecha_hora']).dt.tz_localize(
            'America/Santiago', ambiguous='NaT', nonexistent='NaT'
        )
        return df_w.dropna(subset=['fecha_hora'])

    fecha_ini = df['fecha_hora'].min().strftime('%Y-%m-%d')
    fecha_fin = (pd.Timestamp.now(tz='America/Santiago') - pd.Timedelta(days=5)).strftime('%Y-%m-%d')
    log(f"Descargando clima: {fecha_ini} -> {fecha_fin} ...")
    try:
        weather = fetch_weather(fecha_ini, fecha_fin)
        log(f"OK: {len(weather):,} horas")
    except Exception as e:
        weather = pd.DataFrame(columns=['fecha_hora', 'temperatura', 'humedad',
                                        'viento_vel', 'viento_racha', 'precipitacion', 'cod_clima'])
        log(f"Error clima: {e} — continuando sin datos climaticos.")

    # ─── 3. Feriados ─────────────────────────────────────────────────────────────
    log("\n=== 3. Feriados ===")
    fechas_feriado = cargar_feriados(
        os.path.join(DATA_DIR, 'independent_variables', 'feriados.xlsx'),
        anios=range(2021, 2027),
        log_fn=log,
    )

    # ─── 4. Feature Engineering ──────────────────────────────────────────────────
    log("\n=== 4. Feature Engineering ===")

    def agregar_features_temporales(df):
        dt = df['fecha_hora']
        df['hora']          = dt.dt.hour
        df['dia_semana']    = dt.dt.dayofweek
        df['mes']           = dt.dt.month
        df['dia_mes']       = dt.dt.day
        df['anio']          = dt.dt.year
        df['trimestre']     = dt.dt.quarter
        df['semana_anio']   = dt.dt.isocalendar().week.astype(int)
        df['es_fin_semana'] = (df['dia_semana'] >= 5).astype(int)
        df['es_verano']     = df['mes'].isin([12, 1, 2]).astype(int)
        fechas_str = {d.isoformat() for d in fechas_feriado}
        df['es_feriado']    = dt.dt.strftime('%Y-%m-%d').isin(fechas_str).astype(int)
        df['es_noche']      = ((df['hora'] >= 22) | (df['hora'] < 6)).astype(int)
        # Franja horaria: 0=madrugada(0-5), 1=manana(6-11), 2=tarde(12-17), 3=noche(18-21), 4=trasnoche(22-23)
        df['franja'] = pd.cut(
            df['hora'],
            bins=[-1, 5, 11, 17, 21, 23],
            labels=[0, 1, 2, 3, 4]
        ).astype(int)
        df['hora_sin']      = np.sin(2 * np.pi * df['hora'] / 24)
        df['hora_cos']      = np.cos(2 * np.pi * df['hora'] / 24)
        df['dia_sem_sin']   = np.sin(2 * np.pi * df['dia_semana'] / 7)
        df['dia_sem_cos']   = np.cos(2 * np.pi * df['dia_semana'] / 7)
        df['mes_sin']       = np.sin(2 * np.pi * df['mes'] / 12)
        df['mes_cos']       = np.cos(2 * np.pi * df['mes'] / 12)
        return df

    df = agregar_features_temporales(df)

    COLS_CLIMA = ['temperatura', 'humedad', 'viento_vel', 'viento_racha', 'precipitacion', 'cod_clima']
    if len(weather) > 0:
        df = df.merge(weather[['fecha_hora'] + COLS_CLIMA], on='fecha_hora', how='left')
        df[COLS_CLIMA] = df.groupby('hora')[COLS_CLIMA].transform(lambda x: x.fillna(x.median()))
        nans = {c: v for c, v in df[COLS_CLIMA].isna().mean().items() if v > 0}
        log(f"NaN clima restantes: {nans if nans else 'ninguno'}")
    else:
        for col in COLS_CLIMA:
            df[col] = np.nan

    df = df.sort_values('fecha_hora').reset_index(drop=True)
    n = df['n_emergencias']
    n_shifted = n.shift(1)
    df['lag_1h']        = n.shift(1)
    df['lag_2h']        = n.shift(2)
    df['lag_3h']        = n.shift(3)
    df['lag_24h']       = n.shift(24)
    df['lag_48h']       = n.shift(48)
    df['lag_168h']      = n.shift(168)
    df['roll_mean_24h'] = n_shifted.rolling(24,  min_periods=12).mean()
    df['roll_mean_7d']  = n_shifted.rolling(168, min_periods=48).mean()
    df['roll_std_7d']   = n_shifted.rolling(168, min_periods=48).std()
    df['roll_sum_24h']  = n_shifted.rolling(24,  min_periods=12).sum()
    df['roll_max_24h']  = n_shifted.rolling(24,  min_periods=12).max()
    log("Features temporales y de lag creadas")

    # ─── 5. EDA (plotly HTML — sin rendering nativo) ─────────────────────────────
    log("\n=== 5. EDA ===")
    dias_lbl  = ['Lun','Mar','Mie','Jue','Vie','Sab','Dom']
    meses_lbl = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']

    fig_eda = make_subplots(
        rows=2, cols=3,
        subplot_titles=[
            'Promedio por hora del dia', 'Promedio por dia de la semana', 'Promedio por mes',
            'Distribucion emergencias/hora', 'Total mensual', 'Heatmap hora x dia semana',
        ]
    )

    por_hora = df.groupby('hora')['n_emergencias'].mean()
    fig_eda.add_trace(go.Bar(x=por_hora.index, y=por_hora.values, name='hora', marker_color='steelblue'), row=1, col=1)

    por_dia = df.groupby('dia_semana')['n_emergencias'].mean()
    fig_eda.add_trace(go.Bar(x=dias_lbl, y=por_dia.values, name='dia', marker_color='coral'), row=1, col=2)

    por_mes = df.groupby('mes')['n_emergencias'].mean()
    fig_eda.add_trace(go.Bar(x=meses_lbl, y=por_mes.values, name='mes', marker_color='mediumseagreen'), row=1, col=3)

    vc = df['n_emergencias'].value_counts().sort_index()
    fig_eda.add_trace(go.Bar(x=vc.index, y=vc.values, name='distrib', marker_color='mediumpurple'), row=2, col=1)

    mensual = df.set_index('fecha_hora').resample('ME')['n_emergencias'].sum().reset_index()
    fig_eda.add_trace(go.Scatter(x=mensual['fecha_hora'].astype(str), y=mensual['n_emergencias'],
                                 mode='lines', name='mensual', line_color='darkorange'), row=2, col=2)

    pivot = df.pivot_table(values='n_emergencias', index='hora', columns='dia_semana', aggfunc='mean')
    fig_eda.add_trace(go.Heatmap(z=pivot.values, x=dias_lbl, y=pivot.index,
                                 colorscale='YlOrRd', name='heatmap'), row=2, col=3)

    fig_eda.update_layout(title='Patrones de Emergencias - CBT Talcahuano',
                          height=700, showlegend=False)
    fig_eda.write_html('eda_emergencias.html')
    log("Guardado: eda_emergencias.html")

    # ─── 6. Dataset ML ───────────────────────────────────────────────────────────
    log("\n=== 6. Preparar dataset ML ===")
    FEATURES_TEMP = [
        'hora','dia_semana','mes','dia_mes','anio','trimestre','semana_anio',
        'es_fin_semana','es_verano','es_feriado','es_noche','franja',
        'hora_sin','hora_cos','dia_sem_sin','dia_sem_cos','mes_sin','mes_cos',
    ]
    FEATURES_LAG = [
        'lag_1h','lag_2h','lag_3h','lag_24h','lag_48h','lag_168h',
        'roll_mean_24h','roll_mean_7d','roll_std_7d','roll_sum_24h','roll_max_24h',
    ]
    TARGET = 'n_emergencias'

    all_features = FEATURES_TEMP + COLS_CLIMA + FEATURES_LAG
    features = [f for f in all_features
                if f in df.columns and df[f].notna().mean() > 0.30]

    log(f"Features incluidas: {len(features)}")
    for f in features:
        tag = "[LAG]" if f in FEATURES_LAG else ("[CLIMA]" if f in COLS_CLIMA else "[TEMP]")
        log(f"  {tag:7} {f:<22}  {df[f].isna().mean()*100:.1f}% NaN")

    df_ml = df[['fecha_hora', TARGET] + features].dropna()
    log(f"\nFilas totales: {len(df):,}  |  tras dropna: {len(df_ml):,} ({len(df_ml)/len(df)*100:.1f}%)")

    split = int(len(df_ml) * 0.80)
    df_train = df_ml.iloc[:split]
    df_test  = df_ml.iloc[split:]
    X_train, y_train = df_train[features], df_train[TARGET]
    X_test,  y_test  = df_test[features],  df_test[TARGET]

    log(f"Train: {len(X_train):,} ({df_train['fecha_hora'].min().date()} -> {df_train['fecha_hora'].max().date()})")
    log(f"Test:  {len(X_test):,} ({df_test['fecha_hora'].min().date()}  -> {df_test['fecha_hora'].max().date()})")
    log(f"Media emerg/h  train: {y_train.mean():.4f}  |  test: {y_test.mean():.4f}")

    # ─── 7. Entrenamiento ────────────────────────────────────────────────────────
    log("\n=== 7. Entrenamiento de modelos ===")

    def poisson_deviance(y_true, y_pred):
        eps = 1e-8
        return 2 * np.mean(
            np.where(y_true > 0, y_true * np.log((y_true + eps) / (y_pred + eps)), 0)
            - (y_true - y_pred)
        )

    def evaluar(nombre, y_true, y_pred, lst):
        y_true = np.asarray(y_true)
        y_pred = np.clip(np.asarray(y_pred), 0, None)
        mae  = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2   = r2_score(y_true, y_pred)
        dev  = poisson_deviance(y_true, y_pred)
        # Metricas para datos de conteo dispersos
        zeros_real  = (y_true == 0).mean()
        zeros_pred  = (y_pred < 0.5).mean()
        hit_nonzero = ((y_true > 0) & (y_pred >= 0.5)).sum() / max((y_true > 0).sum(), 1)
        log(f"\n-- {nombre} --")
        log(f"  MAE:                    {mae:.4f}")
        log(f"  RMSE:                   {rmse:.4f}")
        log(f"  Poisson deviance:       {dev:.4f}")
        log(f"  R2:                     {r2:.4f}")
        log(f"  % ceros real:           {zeros_real*100:.1f}%  |  % ceros pred: {zeros_pred*100:.1f}%")
        log(f"  Hit rate emergencias:   {hit_nonzero*100:.1f}%  (pred>=0.5 cuando real>0)")
        lst.append({'modelo': nombre, 'MAE': mae, 'RMSE': rmse, 'Poisson_Dev': dev, 'R2': r2,
                    'Hit_emergencias_%': round(hit_nonzero*100, 1)})
        return y_pred

    def cv_temporal(modelo_fn, X, y, n_splits=5, label=''):
        """TimeSeriesSplit k-fold CV. modelo_fn() debe devolver un objeto con .fit y .predict."""
        tscv = TimeSeriesSplit(n_splits=n_splits)
        mae_lst, rmse_lst, r2_lst = [], [], []
        for fold, (tr_idx, te_idx) in enumerate(tscv.split(X), 1):
            m = modelo_fn()
            m.fit(X.iloc[tr_idx], y.iloc[tr_idx])
            yp = np.clip(m.predict(X.iloc[te_idx]), 0, None)
            mae_lst.append(mean_absolute_error(y.iloc[te_idx], yp))
            rmse_lst.append(np.sqrt(mean_squared_error(y.iloc[te_idx], yp)))
            r2_lst.append(r2_score(y.iloc[te_idx], yp))
        log(f"  CV ({n_splits}-fold TimeSeriesSplit):")
        log(f"    MAE:  {np.mean(mae_lst):.4f} ± {np.std(mae_lst):.4f}  (folds: {[f'{v:.3f}' for v in mae_lst]})")
        log(f"    RMSE: {np.mean(rmse_lst):.4f} ± {np.std(rmse_lst):.4f}")
        log(f"    R2:   {np.mean(r2_lst):.4f} ± {np.std(r2_lst):.4f}")
        return np.mean(mae_lst), np.std(mae_lst)

    def log_feature_importance(importancias, features, top_n=20):
        log(f"\n  Top-{top_n} features mas importantes:")
        top = importancias.sort_values(ascending=False).head(top_n)
        for feat, imp in top.items():
            tag = "[LAG]  " if feat in FEATURES_LAG else ("[CLIMA]" if feat in COLS_CLIMA else "[TEMP] ")
            bar = "█" * int(imp * 200)
            log(f"    {tag} {feat:<22}  {imp:.4f}  {bar}")

    resultados = []
    predicciones = {}

    log("\n[1/3] Baseline: media historica por hora del dia...")
    media_por_hora = df_train.groupby('hora')[TARGET].mean()
    y_pred_baseline = df_test['hora'].map(media_por_hora).fillna(y_train.mean()).values
    predicciones['Baseline (media/hora)'] = evaluar('Baseline (media/hora)', y_test, y_pred_baseline, resultados)
    scaler = StandardScaler()  # guardado para compatibilidad con funcion de prediccion

    log("\n[2/3] Random Forest...")
    rf = RandomForestRegressor(
        n_estimators=300, max_depth=12, min_samples_leaf=5,
        max_features='sqrt', n_jobs=1, random_state=42,
    )
    rf.fit(X_train, y_train)
    predicciones['Random Forest'] = evaluar('Random Forest', y_test, rf.predict(X_test), resultados)
    log("\n  [CV] Random Forest en datos completos...")
    cv_temporal(
        lambda: RandomForestRegressor(n_estimators=100, max_depth=12, min_samples_leaf=5,
                                       max_features='sqrt', n_jobs=1, random_state=42),
        df_ml[features], df_ml[TARGET]
    )
    imp_rf = pd.Series(rf.feature_importances_, index=features)
    log_feature_importance(imp_rf, features)

    if HAS_XGB:
        log("\n[3/3] XGBoost (Poisson)...")
        xgb_model = xgb.XGBRegressor(
            objective='count:poisson',
            n_estimators=800, max_depth=6, learning_rate=0.04,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
            reg_alpha=0.1, reg_lambda=1.0, n_jobs=-1, random_state=42,
            early_stopping_rounds=50, eval_metric='poisson-nloglik',
        )
        xgb_model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=100)
        predicciones['XGBoost (Poisson)'] = evaluar(
            'XGBoost (Poisson)', y_test, xgb_model.predict(X_test), resultados
        )
        log("\n  [CV] XGBoost en datos completos...")
        cv_temporal(
            lambda: xgb.XGBRegressor(
                objective='count:poisson', n_estimators=300, max_depth=6,
                learning_rate=0.04, subsample=0.8, colsample_bytree=0.8,
                min_child_weight=5, reg_alpha=0.1, reg_lambda=1.0,
                n_jobs=-1, random_state=42,
            ),
            df_ml[features], df_ml[TARGET]
        )
        imp_xgb = pd.Series(xgb_model.feature_importances_, index=features)
        log_feature_importance(imp_xgb, features)

    # ─── 7b. Modelo en 2 etapas por franja horaria ───────────────────────────────
    log("\n=== 7b. Modelo en 2 etapas (Clasificador + Regresor) por franja horaria ===")
    FRANJAS = {0: 'Madrugada(0-5)', 1: 'Mañana(6-11)', 2: 'Tarde(12-17)',
               3: 'Noche(18-21)',   4: 'Trasnoche(22-23)'}

    # Estadisticas por franja en train
    log("\n  Distribucion de emergencias por franja (train):")
    for fid, fname in FRANJAS.items():
        mask = df_train['franja'] == fid
        n_total = mask.sum()
        n_emerg = (df_train.loc[mask, TARGET] > 0).sum()
        media   = df_train.loc[mask, TARGET].mean()
        log(f"    {fname:<20}  {n_total:>6} horas  |  {n_emerg:>5} con emerg ({n_emerg/n_total*100:.1f}%)  |  media={media:.3f}")

    # Modelo 2-etapas global (sin split por franja) para comparar
    if HAS_XGB:
        log("\n  [A] Modelo 2 etapas GLOBAL ...")
        scale_pos = (y_train == 0).sum() / max((y_train > 0).sum(), 1)
        clf = XGBClassifier(
            n_estimators=400, max_depth=5, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
            scale_pos_weight=scale_pos,
            eval_metric='logloss', n_jobs=-1, random_state=42,
        )
        clf.fit(X_train, (y_train > 0).astype(int))
        prob_test = clf.predict_proba(X_test)[:, 1]

        reg2 = xgb.XGBRegressor(
            objective='count:poisson', n_estimators=400, max_depth=5,
            learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
            min_child_weight=3, n_jobs=-1, random_state=42,
        )
        mask_pos_train = y_train > 0
        reg2.fit(X_train[mask_pos_train], y_train[mask_pos_train])
        pred_count_test = np.clip(reg2.predict(X_test), 0, None)

        # Umbral optimo via Youden's J en train
        from sklearn.metrics import roc_curve
        fpr, tpr, thresholds = roc_curve((y_train > 0).astype(int),
                                          clf.predict_proba(X_train)[:, 1])
        youden_j = tpr - fpr
        best_thr = float(thresholds[np.argmax(youden_j)])
        log(f"    Umbral optimo (Youden J): {best_thr:.3f}")

        y_pred_2etapas = np.where(prob_test >= best_thr, pred_count_test, 0.0)
        predicciones['2-Etapas Global'] = evaluar('2-Etapas Global', y_test, y_pred_2etapas, resultados)

        # CV del modelo 2 etapas global
        log("\n  [CV] Modelo 2 etapas GLOBAL (5-fold TimeSeriesSplit)...")
        tscv = TimeSeriesSplit(n_splits=5)
        X_all, y_all = df_ml[features], df_ml[TARGET]
        mae_cv, hit_cv = [], []
        for fold, (tr_i, te_i) in enumerate(tscv.split(X_all), 1):
            Xtr, ytr = X_all.iloc[tr_i], y_all.iloc[tr_i]
            Xte, yte = X_all.iloc[te_i], y_all.iloc[te_i]
            sp = (ytr == 0).sum() / max((ytr > 0).sum(), 1)
            c = XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                              subsample=0.8, colsample_bytree=0.8, scale_pos_weight=sp,
                              eval_metric='logloss', n_jobs=-1, random_state=42)
            c.fit(Xtr, (ytr > 0).astype(int))
            pr = c.predict_proba(Xte)[:, 1]
            fpr_c, tpr_c, thr_c = roc_curve((ytr > 0).astype(int),
                                             c.predict_proba(Xtr)[:, 1])
            thr_best = float(thr_c[np.argmax(tpr_c - fpr_c)])
            r = xgb.XGBRegressor(objective='count:poisson', n_estimators=200,
                                  max_depth=5, learning_rate=0.05, n_jobs=-1, random_state=42)
            mp = ytr > 0
            r.fit(Xtr[mp], ytr[mp])
            pc = np.clip(r.predict(Xte), 0, None)
            yp = np.where(pr >= thr_best, pc, 0.0)
            mae_cv.append(mean_absolute_error(yte, yp))
            hit_cv.append(((yte > 0) & (yp >= 0.5)).sum() / max((yte > 0).sum(), 1))
        log(f"    MAE  CV: {np.mean(mae_cv):.4f} ± {np.std(mae_cv):.4f}  (folds: {[f'{v:.3f}' for v in mae_cv]})")
        log(f"    Hit% CV: {np.mean(hit_cv)*100:.1f}% ± {np.std(hit_cv)*100:.1f}%")

        # Modelos 2 etapas POR FRANJA
        log("\n  [B] Modelos 2 etapas POR FRANJA HORARIA ...")
        y_pred_pf = np.zeros(len(y_test))
        imp_clf_pf  = pd.Series(0.0, index=features)
        imp_reg_pf  = pd.Series(0.0, index=features)
        resultados_franja = []

        for fid, fname in FRANJAS.items():
            mask_tr = df_train['franja'] == fid
            mask_te = df_test['franja']  == fid
            if mask_tr.sum() < 50 or mask_te.sum() < 5:
                log(f"    {fname}: datos insuficientes, saltando.")
                continue
            Xtr_f, ytr_f = X_train[mask_tr], y_train[mask_tr]
            Xte_f, yte_f = X_test[mask_te],  y_test[mask_te]

            sp_f = (ytr_f == 0).sum() / max((ytr_f > 0).sum(), 1)
            clf_f = XGBClassifier(
                n_estimators=300, max_depth=4, learning_rate=0.06,
                subsample=0.8, colsample_bytree=0.8, scale_pos_weight=sp_f,
                eval_metric='logloss', n_jobs=-1, random_state=42,
            )
            clf_f.fit(Xtr_f, (ytr_f > 0).astype(int))
            prob_f = clf_f.predict_proba(Xte_f)[:, 1]

            mp_f = ytr_f > 0
            if mp_f.sum() < 20:
                pred_c_f = np.full(len(Xte_f), ytr_f[mp_f].mean() if mp_f.any() else 1.0)
            else:
                reg_f = xgb.XGBRegressor(
                    objective='count:poisson', n_estimators=300, max_depth=4,
                    learning_rate=0.06, subsample=0.8, colsample_bytree=0.8,
                    n_jobs=-1, random_state=42,
                )
                reg_f.fit(Xtr_f[mp_f], ytr_f[mp_f])
                pred_c_f = np.clip(reg_f.predict(Xte_f), 0, None)
                imp_reg_pf += pd.Series(reg_f.feature_importances_, index=features) / len(FRANJAS)

            fpr_f, tpr_f, thr_f = roc_curve((ytr_f > 0).astype(int),
                                              clf_f.predict_proba(Xtr_f)[:, 1])
            thr_best_f = float(thr_f[np.argmax(tpr_f - fpr_f)])
            yp_f = np.where(prob_f >= thr_best_f, pred_c_f, 0.0)
            y_pred_pf[mask_te.values] = yp_f

            mae_f  = mean_absolute_error(yte_f, yp_f)
            hit_f  = ((yte_f > 0) & (yp_f >= 0.5)).sum() / max((yte_f > 0).sum(), 1)
            pct0_f = (yte_f == 0).mean()
            log(f"    {fname:<22} MAE={mae_f:.3f}  hit%={hit_f*100:.1f}%  "
                f"umbral={thr_best_f:.2f}  (ceros: {pct0_f*100:.0f}%)")
            resultados_franja.append({'franja': fname, 'MAE': mae_f, 'hit%': round(hit_f*100,1),
                                      'n_test': int(mask_te.sum())})
            imp_clf_pf += pd.Series(clf_f.feature_importances_, index=features) / len(FRANJAS)

        predicciones['2-Etapas x Franja'] = evaluar('2-Etapas x Franja', y_test, y_pred_pf, resultados)
        log("\n  Features importantes (clasificador, promedio franjas):")
        log_feature_importance(imp_clf_pf, features, top_n=10)

    # ─── 7c. Modelo en 2 etapas por hora (24 modelos individuales) ───────────────
    if HAS_XGB:
        log("\n=== 7c. Modelo en 2 etapas POR HORA (24 modelos individuales) ===")

        HORA_DROP  = {'hora', 'hora_sin', 'hora_cos', 'franja', 'es_noche'}
        features_ph = [f for f in features if f not in HORA_DROP]
        log(f"  Features por hora: {len(features_ph)} (eliminadas {len(features)-len(features_ph)} relacionadas con hora)")

        # Distribucion por hora en train
        log("\n  Distribucion de emergencias por hora (train):")
        log(f"  {'hora':>4} | {'n_total':>7} | {'n_pos':>6} | {'%pos':>6} | {'media':>6}")
        log("  " + "-"*45)
        for h in range(24):
            m = df_train['hora'] == h
            nt = m.sum(); np_ = (df_train.loc[m, TARGET] > 0).sum()
            log(f"  {h:>4} | {nt:>7} | {np_:>6} | {np_/nt*100:>5.1f}% | {df_train.loc[m,TARGET].mean():>6.3f}")

        # Inicializar colectores
        y_pred_ph      = np.zeros(len(y_test))
        resultados_ph  = []
        imp_clf_ph     = pd.Series(0.0, index=features_ph)
        imp_reg_ph     = pd.Series(0.0, index=features_ph)
        horas_modelo   = 0
        horas_fallback = 0
        ph_config      = {'features_ph': features_ph, 'hora_drop': list(HORA_DROP),
                          'thresholds': {}, 'fallback_means': {},
                          'auc_roc_per_hour': {}, 'mae_per_hour': {}, 'n_pos_per_hour': {}}

        log("\n  Entrenando 24 modelos...")
        for h in range(24):
            mask_tr = df_train['hora'] == h
            mask_te = df_test['hora']  == h
            n_tr = mask_tr.sum(); n_te = mask_te.sum()

            if n_tr < 50 or n_te < 5:
                log(f"    hora={h:02d}: datos insuficientes (train={n_tr}, test={n_te}), saltando.")
                continue

            Xtr_h = X_train.loc[mask_tr, features_ph]
            ytr_h = y_train[mask_tr]
            Xte_h = X_test.loc[mask_te, features_ph]
            yte_h = y_test[mask_te]
            n_pos_tr = (ytr_h > 0).sum()

            if n_pos_tr < 2:
                log(f"    hora={h:02d}: sin suficientes positivos para clasificador ({n_pos_tr}), saltando.")
                continue

            # ── Stage 1: Clasificador ──────────────────────────────────────────
            spw = (ytr_h == 0).sum() / max(n_pos_tr, 1)
            clf_h = XGBClassifier(
                n_estimators=300, max_depth=4, learning_rate=0.06,
                subsample=0.8, colsample_bytree=0.8,
                scale_pos_weight=spw, eval_metric='logloss',
                n_jobs=-1, random_state=42, verbosity=0,
            )
            clf_h.fit(Xtr_h, (ytr_h > 0).astype(int))
            prob_tr_h = clf_h.predict_proba(Xtr_h)[:, 1]
            prob_te_h = clf_h.predict_proba(Xte_h)[:, 1]

            fpr_h, tpr_h, thr_h = roc_curve((ytr_h > 0).astype(int), prob_tr_h)
            best_idx_h  = np.argmax(tpr_h - fpr_h)
            thr_best_h  = float(np.clip(thr_h[best_idx_h], 0.01, 0.99))
            auc_h       = roc_auc_score((yte_h > 0).astype(int), prob_te_h)
            imp_clf_ph += pd.Series(clf_h.feature_importances_, index=features_ph)

            # ── Stage 2: Regresor ──────────────────────────────────────────────
            mask_pos_h   = ytr_h > 0
            use_fallback = mask_pos_h.sum() < 20
            if use_fallback:
                fallback_mean = float(ytr_h[mask_pos_h].mean()) if mask_pos_h.any() else 1.0
                pred_count_h  = np.full(len(Xte_h), fallback_mean)
                ph_config['fallback_means'][h] = fallback_mean
                horas_fallback += 1
            else:
                reg_h = xgb.XGBRegressor(
                    objective='count:poisson', n_estimators=300, max_depth=4,
                    learning_rate=0.06, subsample=0.8, colsample_bytree=0.8,
                    min_child_weight=3, n_jobs=-1, random_state=42, verbosity=0,
                )
                reg_h.fit(Xtr_h[mask_pos_h], ytr_h[mask_pos_h])
                pred_count_h = np.clip(reg_h.predict(Xte_h), 0, None)
                imp_reg_ph  += pd.Series(reg_h.feature_importances_, index=features_ph)
                reg_h.save_model(f'modelos/ph_reg_{h:02d}.json')
                horas_modelo += 1

            clf_h.save_model(f'modelos/ph_clf_{h:02d}.json')

            # ── Prediccion final ───────────────────────────────────────────────
            yp_h = np.where(prob_te_h >= thr_best_h, pred_count_h, 0.0)
            y_pred_ph[mask_te.values] = yp_h

            # ── Metricas por hora ──────────────────────────────────────────────
            mae_h    = mean_absolute_error(yte_h, yp_h)
            mae_nz_h = (mean_absolute_error(yte_h[yte_h > 0], yp_h[yte_h > 0])
                        if (yte_h > 0).any() else np.nan)
            hit_h    = ((yte_h > 0) & (yp_h >= 0.5)).sum() / max((yte_h > 0).sum(), 1)
            y_bin_te = (yte_h > 0).astype(int)
            y_hat_b  = (prob_te_h >= thr_best_h).astype(int)
            f1_h     = f1_score(y_bin_te, y_hat_b, zero_division=0)
            prec_h   = precision_score(y_bin_te, y_hat_b, zero_division=0)
            rec_h    = recall_score(y_bin_te, y_hat_b, zero_division=0)

            resultados_ph.append({
                'hora': h, 'n_test': int(n_te),
                'pct_zeros': round((yte_h == 0).mean() * 100, 1),
                'umbral': round(thr_best_h, 3),
                'auc_roc': round(auc_h, 3),
                'f1': round(f1_h, 3),
                'prec': round(prec_h, 3),
                'recall': round(rec_h, 3),
                'hit_pct': round(hit_h * 100, 1),
                'mae': round(mae_h, 4),
                'mae_nonzero': round(mae_nz_h, 4) if not np.isnan(mae_nz_h) else None,
                'fallback': use_fallback,
            })
            ph_config['thresholds'][h]      = thr_best_h
            ph_config['auc_roc_per_hour'][h] = round(auc_h, 3)
            ph_config['mae_per_hour'][h]     = round(mae_h, 4)
            ph_config['n_pos_per_hour'][h]   = int(n_pos_tr)

        # Tabla de resultados por hora
        log("\n  Resultados por hora (test set):")
        log(f"  {'hora':>4} | {'n_test':>6} | {'%zer':>5} | {'umbral':>6} | {'AUC':>5} | {'F1':>5} | {'prec':>5} | {'rec':>5} | {'hit%':>5} | {'MAE':>6} | {'MAE_nz':>7}")
        log("  " + "-"*80)
        for r in resultados_ph:
            mnz = f"{r['mae_nonzero']:.4f}" if r['mae_nonzero'] else "  N/A "
            fb  = " *" if r['fallback'] else ""
            log(f"  {r['hora']:>4} | {r['n_test']:>6} | {r['pct_zeros']:>4.1f}% | "
                f"{r['umbral']:>6.3f} | {r['auc_roc']:>5.3f} | {r['f1']:>5.3f} | "
                f"{r['prec']:>5.3f} | {r['recall']:>5.3f} | {r['hit_pct']:>4.1f}% | "
                f"{r['mae']:>6.4f} | {mnz}{fb}")

        # Promedios
        aucs  = [r['auc_roc'] for r in resultados_ph]
        f1s   = [r['f1']      for r in resultados_ph]
        hits  = [r['hit_pct'] for r in resultados_ph]
        maes  = [r['mae']     for r in resultados_ph]
        maenz = [r['mae_nonzero'] for r in resultados_ph if r['mae_nonzero'] is not None]
        log(f"\n  Promedios ({len(resultados_ph)} horas):")
        log(f"    AUC-ROC:      {np.mean(aucs):.3f}  ±  {np.std(aucs):.3f}")
        log(f"    F1:           {np.mean(f1s):.3f}  ±  {np.std(f1s):.3f}")
        log(f"    hit%:         {np.mean(hits):.1f}%  ±  {np.std(hits):.1f}%")
        log(f"    MAE:          {np.mean(maes):.4f}  ±  {np.std(maes):.4f}")
        log(f"    MAE_nonzero:  {np.mean(maenz):.4f}  ±  {np.std(maenz):.4f}  (solo horas con emergencias reales)")
        log(f"    Horas con modelo completo: {horas_modelo}/24  |  con fallback regresor: {horas_fallback}")

        # Feature importance promediada
        if horas_modelo > 0:
            log("\n  Features importantes — Clasificador (promedio 24h):")
            log_feature_importance(imp_clf_ph / len(resultados_ph), features_ph, top_n=15)
            log("\n  Features importantes — Regresor (promedio horas con modelo):")
            log_feature_importance(imp_reg_ph / horas_modelo, features_ph, top_n=15)

        # Evaluacion global
        predicciones['2-Etapas x Hora'] = evaluar('2-Etapas x Hora', y_test, y_pred_ph, resultados)

        # CV temporal — 5-fold sobre predicciones combinadas
        log("\n  [CV] Modelo 2 etapas x Hora (5-fold TimeSeriesSplit — combinado)...")
        tscv_ph = TimeSeriesSplit(n_splits=5)
        X_all_ph = df_ml[features_ph + ['hora']]
        y_all_ph = df_ml[TARGET]
        mae_cv_ph, hit_cv_ph = [], []
        for fold, (tr_i, te_i) in enumerate(tscv_ph.split(X_all_ph), 1):
            df_fold_tr = df_ml.iloc[tr_i]
            df_fold_te = df_ml.iloc[te_i]
            yp_fold    = np.zeros(len(te_i))
            for h in range(24):
                mtr_cv = df_fold_tr['hora'] == h
                mte_cv = df_fold_te['hora'] == h
                if mtr_cv.sum() < 50 or mte_cv.sum() < 5:
                    continue
                Xtr_cv = df_fold_tr.loc[mtr_cv, features_ph]
                ytr_cv = df_fold_tr.loc[mtr_cv, TARGET]
                Xte_cv = df_fold_te.loc[mte_cv, features_ph]
                yte_cv = df_fold_te.loc[mte_cv, TARGET]
                if (ytr_cv > 0).sum() < 2:
                    continue
                spw_cv = (ytr_cv == 0).sum() / max((ytr_cv > 0).sum(), 1)
                c = XGBClassifier(n_estimators=150, max_depth=4, learning_rate=0.06,
                                  scale_pos_weight=spw_cv, eval_metric='logloss',
                                  n_jobs=-1, random_state=42, verbosity=0)
                c.fit(Xtr_cv, (ytr_cv > 0).astype(int))
                pr_cv  = c.predict_proba(Xte_cv)[:, 1]
                fpr_cv, tpr_cv, thr_cv = roc_curve((ytr_cv > 0).astype(int),
                                                    c.predict_proba(Xtr_cv)[:, 1])
                thr_cv_b = float(np.clip(thr_cv[np.argmax(tpr_cv - fpr_cv)], 0.01, 0.99))
                mp_cv = ytr_cv > 0
                if mp_cv.sum() < 20:
                    pc_cv = np.full(len(Xte_cv), float(ytr_cv[mp_cv].mean()) if mp_cv.any() else 1.0)
                else:
                    r_cv = xgb.XGBRegressor(objective='count:poisson', n_estimators=150,
                                            max_depth=4, learning_rate=0.06,
                                            n_jobs=-1, random_state=42, verbosity=0)
                    r_cv.fit(Xtr_cv[mp_cv], ytr_cv[mp_cv])
                    pc_cv = np.clip(r_cv.predict(Xte_cv), 0, None)
                local_idx = np.where(df_fold_te['hora'].values == h)[0]
                yp_fold[local_idx] = np.where(pr_cv >= thr_cv_b, pc_cv, 0.0)
            yte_fold = df_fold_te[TARGET].values
            mae_cv_ph.append(mean_absolute_error(yte_fold, yp_fold))
            hit_cv_ph.append(((yte_fold > 0) & (yp_fold >= 0.5)).sum() / max((yte_fold > 0).sum(), 1))
        log(f"    MAE  CV: {np.mean(mae_cv_ph):.4f} ± {np.std(mae_cv_ph):.4f}  (folds: {[f'{v:.3f}' for v in mae_cv_ph]})")
        log(f"    Hit% CV: {np.mean(hit_cv_ph)*100:.1f}% ± {np.std(hit_cv_ph)*100:.1f}%")

        # Guardar ph_config y CSV
        joblib.dump(ph_config, 'modelos/ph_config.pkl')
        pd.DataFrame(resultados_ph).to_csv('modelos/resultados_por_hora.csv', index=False)
        log("  Guardados: modelos/ph_config.pkl  |  modelos/resultados_por_hora.csv")

        # Visualizacion por hora
        df_ph = pd.DataFrame(resultados_ph)
        mae_avg = df_ph['mae'].mean()
        colors_mae = ['crimson' if v > mae_avg else 'steelblue' for v in df_ph['mae']]

        # Heatmap de importancias por hora (clasificador)
        imp_matrix = np.zeros((15, len(resultados_ph)))
        imp_sorted = imp_clf_ph.sort_values(ascending=False)
        top15_feats = imp_sorted.head(15).index.tolist()
        for i, h_r in enumerate(resultados_ph):
            h = h_r['hora']
            clf_imp = pd.Series(np.load(f'modelos/ph_clf_{h:02d}.json', allow_pickle=True)
                                if False else np.zeros(len(features_ph)), index=features_ph)
        # Reconstruir importancias por hora leyendo los modelos guardados
        imp_by_hour = {}
        for h_r in resultados_ph:
            h = h_r['hora']
            tmp_clf = XGBClassifier()
            tmp_clf.load_model(f'modelos/ph_clf_{h:02d}.json')
            imp_by_hour[h] = pd.Series(tmp_clf.feature_importances_, index=features_ph)
        imp_top15 = imp_sorted.head(15).index.tolist()
        imp_mat   = np.array([[imp_by_hour[r['hora']][f] for r in resultados_ph]
                               for f in imp_top15])

        fig_ph = make_subplots(
            rows=3, cols=1,
            subplot_titles=[
                'MAE por hora del día (modelo 2-etapas)',
                'AUC-ROC y Hit% por hora',
                'Importancia de features por hora — Clasificador (top 15)',
            ],
            row_heights=[0.22, 0.22, 0.56],
            vertical_spacing=0.08,
        )
        horas_lbl = [str(r['hora']) for r in resultados_ph]
        fig_ph.add_trace(go.Bar(x=horas_lbl, y=df_ph['mae'],
                                marker_color=colors_mae, name='MAE'), row=1, col=1)
        fig_ph.add_trace(go.Scatter(x=horas_lbl, y=df_ph['auc_roc'],
                                    mode='lines+markers', name='AUC-ROC',
                                    line=dict(color='steelblue')), row=2, col=1)
        fig_ph.add_trace(go.Scatter(x=horas_lbl, y=df_ph['hit_pct'],
                                    mode='lines+markers', name='Hit%',
                                    line=dict(color='darkorange'), yaxis='y4'), row=2, col=1)
        fig_ph.add_trace(go.Heatmap(z=imp_mat, x=horas_lbl, y=imp_top15,
                                    colorscale='Blues', name='Importancia'), row=3, col=1)
        fig_ph.update_layout(title='Análisis por hora — Modelo 2 etapas (CBT Talcahuano)',
                             height=1000, showlegend=True)
        fig_ph.write_html('modelos_por_hora.html')
        log("  Guardado: modelos_por_hora.html")

    # ─── 8. Comparacion ──────────────────────────────────────────────────────────
    log("\n=== 8. Comparacion de modelos ===")
    df_res = pd.DataFrame(resultados)
    log("\n" + "="*70)
    log("RESUMEN DE RESULTADOS")
    log("="*70)
    log(df_res.to_string(index=False))
    mejor = df_res.loc[df_res['MAE'].idxmin(), 'modelo']
    log(f"\n-> Mejor modelo (menor MAE): {mejor}")
    log(f"\nNOTA: R2 bajo es esperado con datos de conteo dispersos (82.9% ceros).")
    log(f"  Usar MAE y Poisson deviance como metricas principales.")
    log(f"  Mejora sobre baseline: {(df_res.loc[df_res['modelo']=='Baseline (media/hora)','MAE'].values[0] - df_res.loc[df_res['MAE'].idxmin(),'MAE']) / df_res.loc[df_res['modelo']=='Baseline (media/hora)','MAE'].values[0] * 100:.1f}% reduccion MAE")

    y_pred_best = predicciones[mejor]

    # Grafico predicciones (2 semanas del test)
    n_viz = 24 * 14
    test_fechas = df_test['fecha_hora'].astype(str).values[:n_viz]
    y_real_viz  = y_test.values[:n_viz]
    y_pred_viz  = y_pred_best[:n_viz]

    fig_pred = make_subplots(rows=2, cols=1,
                             subplot_titles=[f'Real vs Predicho ({mejor}) — 2 semanas', 'Scatter test completo'])
    fig_pred.add_trace(go.Scatter(x=test_fechas, y=y_real_viz, fill='tozeroy',
                                  name='Real', line_color='steelblue', opacity=0.6), row=1, col=1)
    fig_pred.add_trace(go.Scatter(x=test_fechas, y=y_pred_viz,
                                  name='Prediccion', line_color='crimson'), row=1, col=1)
    fig_pred.add_trace(go.Scatter(x=y_test.tolist(), y=y_pred_best.tolist(),
                                  mode='markers', marker=dict(size=4, opacity=0.15, color='darkorange'),
                                  name='puntos'), row=2, col=1)
    lim = max(y_test.max(), y_pred_best.max()) + 0.5
    fig_pred.add_trace(go.Scatter(x=[0, lim], y=[0, lim], mode='lines',
                                  line=dict(dash='dash', color='black'), name='ideal'), row=2, col=1)
    fig_pred.update_layout(height=700, title=f'Predicciones — {mejor}')
    fig_pred.write_html('predicciones.html')
    log("Guardado: predicciones.html")

    # Grafico importancia de features
    if mejor.startswith('XGBoost') and HAS_XGB:
        importancias = pd.Series(xgb_model.feature_importances_, index=features)
    else:
        importancias = pd.Series(rf.feature_importances_, index=features)
    importancias = importancias.sort_values(ascending=True)
    colors = ['steelblue' if f in FEATURES_LAG else ('darkorange' if f in COLS_CLIMA else 'mediumseagreen')
              for f in importancias.index]
    fig_imp = go.Figure(go.Bar(x=importancias.values, y=importancias.index,
                               orientation='h', marker_color=colors))
    fig_imp.update_layout(title=f'Importancia de features — {mejor}', height=max(500, len(features) * 18))
    fig_imp.write_html('feature_importance.html')
    log("Guardado: feature_importance.html")

    # ─── 9. Guardar artefactos ────────────────────────────────────────────────────
    log("\n=== 9. Guardar artefactos ===")
    os.makedirs('modelos', exist_ok=True)
    if mejor.startswith('XGBoost') and HAS_XGB:
        xgb_model.save_model('modelos/xgb_emergencias_hora.json')
        log("  xgb guardado: modelos/xgb_emergencias_hora.json")
    joblib.dump(rf,       'modelos/rf_emergencias_hora.pkl')
    joblib.dump(scaler,   'modelos/scaler_poisson.pkl')
    joblib.dump(features, 'modelos/features.pkl')
    joblib.dump(mejor,    'modelos/mejor_modelo.pkl')
    df_ml.to_csv('dataset_final_ml.csv', index=False)
    df_res.to_csv('resultados_modelos.csv', index=False)
    log("  Todos los artefactos guardados en 03_ML/modelos/")

    log("\n=== Pipeline completado con exito ===")
    log(f"Mejor modelo: {mejor}")
    log(f"MAE: {df_res.loc[df_res['modelo']==mejor,'MAE'].values[0]:.4f}")

finally:
    _log_fh.close()
