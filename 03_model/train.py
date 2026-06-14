import pandas as pd
import numpy as np
import os
import pickle
from pathlib import Path
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix
)

RANDOM_STATE = 42
MODEL_PARAMS = {
    'n_estimators': 150,
    'max_depth': 4,
    'min_samples_leaf': 5,
    'learning_rate': 0.05,
    'subsample': 0.8,
    'random_state': RANDOM_STATE,
}
CLASSIFIER_PARAMS = {
    'n_estimators': 400,
    'max_depth': 8,
    'min_samples_leaf': 4,
    'class_weight': 'balanced_subsample',
    'n_jobs': -1,
    'random_state': RANDOM_STATE,
}


def train_model_pipeline(df, exclude_cols, models_dir, prefix=""):
    print(f"\n==================================================")
    print(f" Entrenando Pipeline del Modelo: {'Agnóstico (Sin Fecha)' if prefix else 'Estacional (Con Fecha)'}")
    print(f"==================================================")

    # 1. Definir características e importancia base de forma segura (sin fuga de datos)
    initial_feature_cols = [c for c in df.columns if c not in ['FECHA_DIA', 'EVENTOS'] + exclude_cols]
    split_idx = int(len(df) * 0.8)

    X_initial = df[initial_feature_cols]
    y_reg_initial = df['EVENTOS']
    X_train_initial = X_initial.iloc[:split_idx]
    y_reg_train_initial = y_reg_initial.iloc[:split_idx]

    # Entrenar regresor base para calcular la importancia de características
    print("Calculando la importancia de variables en el set de entrenamiento...")
    base_reg = GradientBoostingRegressor(**MODEL_PARAMS)
    base_reg.fit(X_train_initial, y_reg_train_initial)

    # Prunar variables que aporten menos del 0.8% para evitar sobreajuste y limpiar la importancia
    IMPORTANCE_THRESHOLD = 0.008
    importances = base_reg.feature_importances_
    feature_cols = [
        col for col, imp in zip(initial_feature_cols, importances)
        if imp >= IMPORTANCE_THRESHOLD
    ]

    print(f"Features iniciales: {len(initial_feature_cols)}")
    print(f"Features seleccionadas (importancia >= {IMPORTANCE_THRESHOLD*100:.1f}%): {len(feature_cols)}")
    pruned_cols = list(set(initial_feature_cols) - set(feature_cols))
    print(f"Features podadas ({len(pruned_cols)}): {pruned_cols}")
    
    # Umbral para Alta Actividad (más de 7 eventos diarios)
    UMBRAL_ALTA_ACTIVIDAD = 7

    X = df[feature_cols]
    y_reg = df['EVENTOS']
    y_clf = (df['EVENTOS'] > UMBRAL_ALTA_ACTIVIDAD).astype(int)

    # División temporal final para entrenamiento y prueba
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_reg_train, y_reg_test = y_reg.iloc[:split_idx], y_reg.iloc[split_idx:]
    y_clf_train, y_clf_test = y_clf.iloc[:split_idx], y_clf.iloc[split_idx:]

    print(f"Datos de Entrenamiento: {X_train.shape[0]} días")
    print(f"Datos de Prueba: {X_test.shape[0]} días")

    # Calibración temporal: cada fold valida únicamente con fechas posteriores
    # a las usadas para entrenar, evitando mezclar pasado y futuro.
    print("\nBuscando el umbral óptimo con validación temporal de 5 pliegues...")
    time_cv = TimeSeriesSplit(n_splits=5)
    oof_probs = np.full(len(X_train), np.nan)

    for fold, (train_idx, val_idx) in enumerate(time_cv.split(X_train), start=1):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_clf_train.iloc[train_idx], y_clf_train.iloc[val_idx]

        if y_tr.nunique() < 2:
            print(f"Fold {fold}: omitido porque el bloque de entrenamiento tiene una sola clase.")
            continue

        clf_fold = RandomForestClassifier(**CLASSIFIER_PARAMS)
        clf_fold.fit(X_tr, y_tr)
        oof_probs[val_idx] = clf_fold.predict_proba(X_val)[:, 1]

    valid_oof = ~np.isnan(oof_probs)
    if not valid_oof.any():
        raise RuntimeError("No fue posible generar probabilidades OOF para calibrar el clasificador.")

    y_clf_oof = y_clf_train.iloc[np.flatnonzero(valid_oof)]
    oof_probs_valid = oof_probs[valid_oof]
    best_threshold = 0.5
    best_youden = -1.0

    for t in np.arange(0.05, 0.85, 0.01):
        preds = (oof_probs_valid >= t).astype(int)
        tn, fp, fn, tp = confusion_matrix(y_clf_oof, preds, labels=[0, 1]).ravel()
        sens = tp / (tp + fn) if (tp + fn) > 0 else 0
        spec = tn / (tn + fp) if (tn + fp) > 0 else 0
        youden = sens + spec - 1
        if youden > best_youden:
            best_youden = youden
            best_threshold = t

    print(f"Umbral de clasificación óptimo seleccionado: {best_threshold:.2f} (Índice de Youden en Train: {best_youden:.3f})")

    # Entrenar Modelo de Regresión Final
    print("\nEntrenando modelo final de Regresión...")
    reg_model = GradientBoostingRegressor(**MODEL_PARAMS)
    reg_model.fit(X_train, y_reg_train)
    
    y_reg_pred = reg_model.predict(X_test)
    
    mae = mean_absolute_error(y_reg_test, y_reg_pred)
    mse = mean_squared_error(y_reg_test, y_reg_pred)
    r2 = r2_score(y_reg_test, y_reg_pred)
    train_mean = float(y_reg_train.mean())
    baseline_pred = np.full(len(y_reg_test), train_mean)
    baseline_mae = mean_absolute_error(y_reg_test, baseline_pred)
    baseline_mse = mean_squared_error(y_reg_test, baseline_pred)
    
    print("\n--- Métricas del Modelo de Regresión (Set de Prueba) ---")
    print(f"Error Absoluto Medio (MAE): {mae:.3f} eventos")
    print(f"Error Cuadrático Medio (MSE): {mse:.3f}")
    print(f"Coeficiente de Determinación R²: {r2:.3f}")

    # Entrenar Modelo de Clasificación Final
    print("\nEntrenando modelo final de Clasificación...")
    clf_model = RandomForestClassifier(**CLASSIFIER_PARAMS)
    clf_model.fit(X_train, y_clf_train)
    
    y_clf_prob = clf_model.predict_proba(X_test)[:, 1]
    y_clf_pred = (y_clf_prob >= best_threshold).astype(int)
    
    acc = accuracy_score(y_clf_test, y_clf_pred)
    prec = precision_score(y_clf_test, y_clf_pred, zero_division=0)
    rec = recall_score(y_clf_test, y_clf_pred, zero_division=0)
    f1 = f1_score(y_clf_test, y_clf_pred, zero_division=0)
    roc_auc = roc_auc_score(y_clf_test, y_clf_prob)
    cm = confusion_matrix(y_clf_test, y_clf_pred)
    
    print("\n--- Métricas del Modelo de Clasificación (Set de Prueba) ---")
    print(f"Umbral aplicado: {best_threshold:.2f}")
    print(f"Exactitud (Accuracy): {acc:.3f}")
    print(f"Precisión (Precision): {prec:.3f}")
    print(f"Sensibilidad (Recall): {rec:.3f}")
    print(f"F1-Score: {f1:.3f}")
    print(f"Área bajo la curva ROC (ROC-AUC): {roc_auc:.3f}")
    print("Matriz de Confusión:")
    print(cm)
    
    # Importancia de las Características
    importances_final = reg_model.feature_importances_
    df_importance = pd.DataFrame({
        'Característica': feature_cols,
        'Importancia': importances_final
    }).sort_values(by='Importancia', ascending=False)
    
    print("\n--- Top 10 Importancia de las Características (Regresión) ---")
    print(df_importance.head(10).to_string(index=False))

    # Guardar los modelos entrenados y los metadatos
    print("\nGuardando modelos entrenados...")
    
    metadata = {
        'feature_cols': feature_cols,
        'umbral_alta_actividad': UMBRAL_ALTA_ACTIVIDAD,
        'classification_threshold': best_threshold,
        'threshold_metric': 'youden_j_temporal_oof',
        'threshold_score': best_youden,
        'regressor_type': 'GradientBoostingRegressor',
        'classifier_type': 'RandomForestClassifier',
        'train_end_date': str(df['FECHA_DIA'].iloc[split_idx - 1]),
        'test_start_date': str(df['FECHA_DIA'].iloc[split_idx]),
        'test_end_date': str(df['FECHA_DIA'].iloc[-1]),
        'train_samples': int(len(X_train)),
        'test_samples': int(len(X_test)),
        'train_target_mean': train_mean,
        'baseline_mae': baseline_mae,
        'baseline_mse': baseline_mse,
        'mae': mae,
        'mse': mse,
        'r2': r2,
        'accuracy': acc,
        'precision': prec,
        'recall': rec,
        'f1': f1,
        'roc_auc': roc_auc
    }
    
    with open(f"{models_dir}/regressor{prefix}.pkl", "wb") as f:
        pickle.dump(reg_model, f)
        
    with open(f"{models_dir}/classifier{prefix}.pkl", "wb") as f:
        pickle.dump(clf_model, f)
        
    with open(f"{models_dir}/metadata{prefix}.pkl", "wb") as f:
        pickle.dump(metadata, f)
        
    print(f"¡Modelos y metadatos guardados con éxito en {models_dir} con prefijo '{prefix}'!")

def main():
    print("=== Paso 3: Ajuste de Modelos CBT (Agnóstico Base vs. Agnóstico Aumentado) ===")

    # 1. Definir rutas
    base_dir = Path(__file__).resolve().parent.parent
    data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    models_dir = base_dir / "03_model" / "saved_models"
    os.makedirs(models_dir, exist_ok=True)

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"No se encontró el dataset: {data_path}")

    # 2. Cargar datos
    df = pd.read_csv(data_path, sep=';')
    print(f"Dataset cargado con {df.shape[0]} registros.")

    # 3. Columnas de calendario a excluir para el modelo agnóstico
    calendar_cols = [
        'MES', 'DIA_SEMANA', 'ES_FIN_SEMANA', 'ES_FERIADO',
        'ES_FERIADO_IRRENUNCIABLE', 'MES_SIN', 'MES_COS',
        'DIA_SIN', 'DIA_COS', 'DANO_SIN', 'DANO_COS'
    ]

    # 4. Pipeline 1: Modelo Agnóstico Base (Sin Clima Dist. / Skew y Kurt)
    exclude_cols_base = calendar_cols + ['TEMP_SKEW', 'TEMP_KURT', 'HUM_SKEW', 'HUM_KURT', 'VIENTO_SKEW', 'VIENTO_KURT']
    train_model_pipeline(df, exclude_cols=exclude_cols_base, models_dir=models_dir, prefix="_agnostic")

    # 5. Pipeline 2: Modelo Agnóstico Aumentado (Con Clima Dist. / Skew y Kurt)
    exclude_cols_aug = calendar_cols
    train_model_pipeline(df, exclude_cols=exclude_cols_aug, models_dir=models_dir, prefix="_agnostic_augmented")

    # 6. Pipeline 3: Modelo Agnóstico Aumentado v3 (Con Clima Dist. / Skew y Kurt, SIN Lags/Rollings de Eventos)
    operational_event_cols = [
        'EVENTOS_lag_1', 'EVENTOS_lag_2', 'EVENTOS_lag_3', 'EVENTOS_lag_7',
        'N_INCENDIO_ESTR_lag_1', 'N_INCENDIO_FOREST_lag_1', 'N_RESCATE_VEH_lag_1',
        'N_RESCATE_PERS_lag_1', 'N_GASES_lag_1',
        'EVENTOS_rolling_mean_3d', 'EVENTOS_rolling_std_3d', 'EVENTOS_rolling_max_3d',
        'EVENTOS_rolling_mean_7d', 'EVENTOS_rolling_std_7d', 'EVENTOS_rolling_max_7d'
    ]
    exclude_cols_aug_v3 = calendar_cols + operational_event_cols
    train_model_pipeline(df, exclude_cols=exclude_cols_aug_v3, models_dir=models_dir, prefix="_agnostic_augmented_v3")


if __name__ == "__main__":
    main()
