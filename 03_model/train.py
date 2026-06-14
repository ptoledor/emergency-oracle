import pandas as pd
import numpy as np
import os
import pickle
import shutil
from itertools import combinations
from pathlib import Path
from joblib import Parallel, delayed
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
SEARCH_MODEL_PARAMS = {
    **MODEL_PARAMS,
    'n_estimators': 120,
}
SEARCH_CLASSIFIER_PARAMS = {
    **CLASSIFIER_PARAMS,
    'n_estimators': 200,
    'n_jobs': 1,
}
SEARCH_N_JOBS = min(8, max(1, (os.cpu_count() or 2) - 1))


def select_stable_features(df, candidate_cols, split_idx, top_n):
    X_train = df[candidate_cols].iloc[:split_idx]
    y_train = df['EVENTOS'].iloc[:split_idx]
    fold_importances = []

    for train_idx, _ in TimeSeriesSplit(n_splits=5).split(X_train):
        model = GradientBoostingRegressor(**MODEL_PARAMS)
        model.fit(X_train.iloc[train_idx], y_train.iloc[train_idx])
        fold_importances.append(model.feature_importances_)

    mean_importances = np.mean(fold_importances, axis=0)
    ranking = sorted(
        zip(candidate_cols, mean_importances),
        key=lambda item: item[1],
        reverse=True,
    )
    return [name for name, _ in ranking[:top_n]], ranking


def evaluate_feature_subset_temporally(X_all, y_reg, y_clf, feature_cols, stage):
    X = X_all[list(feature_cols)]
    reg_predictions = np.full(len(X), np.nan)
    clf_probabilities = np.full(len(X), np.nan)
    fold_maes = []
    fold_aucs = []

    for train_idx, val_idx in TimeSeriesSplit(n_splits=4).split(X):
        reg_model = GradientBoostingRegressor(**SEARCH_MODEL_PARAMS)
        reg_model.fit(X.iloc[train_idx], y_reg.iloc[train_idx])
        fold_reg_predictions = reg_model.predict(X.iloc[val_idx])
        reg_predictions[val_idx] = fold_reg_predictions
        fold_maes.append(
            mean_absolute_error(y_reg.iloc[val_idx], fold_reg_predictions)
        )

        clf_model = RandomForestClassifier(**SEARCH_CLASSIFIER_PARAMS)
        clf_model.fit(X.iloc[train_idx], y_clf.iloc[train_idx])
        fold_probabilities = clf_model.predict_proba(X.iloc[val_idx])[:, 1]
        clf_probabilities[val_idx] = fold_probabilities
        fold_aucs.append(
            roc_auc_score(y_clf.iloc[val_idx], fold_probabilities)
        )

    valid = ~np.isnan(reg_predictions) & ~np.isnan(clf_probabilities)
    valid_positions = np.flatnonzero(valid)
    y_reg_valid = y_reg.iloc[valid_positions]
    y_clf_valid = y_clf.iloc[valid_positions]
    reg_valid = reg_predictions[valid]
    clf_valid = clf_probabilities[valid]

    best_f1 = 0.0
    best_threshold = 0.5
    for threshold in np.arange(0.05, 0.85, 0.01):
        f1 = f1_score(y_clf_valid, clf_valid >= threshold, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold

    return {
        'feature_cols': list(feature_cols),
        'feature_count': len(feature_cols),
        'search_stage': stage,
        'temporal_oof_mae': float(mean_absolute_error(y_reg_valid, reg_valid)),
        'temporal_oof_roc_auc': float(roc_auc_score(y_clf_valid, clf_valid)),
        'temporal_oof_f1': float(best_f1),
        'temporal_oof_f1_threshold': float(best_threshold),
        'fold_mae_std': float(np.std(fold_maes)),
        'fold_roc_auc_std': float(np.std(fold_aucs)),
    }


def rank_feature_search_results(results):
    frame = pd.DataFrame(results)
    frame['rank_mae'] = frame['temporal_oof_mae'].rank(
        method='min', ascending=True
    )
    frame['rank_roc_auc'] = frame['temporal_oof_roc_auc'].rank(
        method='min', ascending=False
    )
    frame['rank_f1'] = frame['temporal_oof_f1'].rank(
        method='min', ascending=False
    )
    frame['mean_metric_rank'] = frame[
        ['rank_mae', 'rank_roc_auc', 'rank_f1']
    ].mean(axis=1)
    return frame.sort_values(
        ['mean_metric_rank', 'temporal_oof_mae', 'feature_count'],
        ascending=[True, True, True],
    ).to_dict('records')


def staged_parallel_feature_search(df, candidate_cols, split_idx):
    X_all = df[candidate_cols].iloc[:split_idx]
    y_reg = df['EVENTOS'].iloc[:split_idx]
    y_clf = (y_reg > 7).astype(int)
    _, stability_ranking = select_stable_features(
        df,
        candidate_cols,
        split_idx=split_idx,
        top_n=len(candidate_cols),
    )
    ranked_features = [name for name, _ in stability_ranking]
    rank_position = {name: position for position, name in enumerate(ranked_features)}
    blocks = [
        ranked_features[index:index + 5]
        for index in range(0, len(ranked_features), 5)
    ]
    evaluated = {}

    def canonical(features):
        selected = set(features)
        return tuple(feature for feature in ranked_features if feature in selected)

    def evaluate(candidate_sets, stage):
        unique = []
        for features in candidate_sets:
            key = canonical(features)
            if key and key not in evaluated:
                unique.append(key)
        print(
            f"\nEtapa {stage}: {len(unique)} subconjuntos nuevos "
            f"en {SEARCH_N_JOBS} procesos paralelos"
        )
        new_results = Parallel(n_jobs=SEARCH_N_JOBS, prefer='processes')(
            delayed(evaluate_feature_subset_temporally)(
                X_all,
                y_reg,
                y_clf,
                features,
                stage,
            )
            for features in unique
        )
        for result in new_results:
            evaluated[canonical(result['feature_cols'])] = result

    initial_candidates = [ranked_features]
    initial_candidates.extend(blocks)
    initial_candidates.extend(
        ranked_features[:size]
        for size in range(5, len(ranked_features), 5)
    )
    initial_candidates.extend(
        [feature for feature in ranked_features if feature not in block]
        for block in blocks
    )
    low_blocks = blocks[len(blocks) // 2:]
    initial_candidates.extend(
        [
            feature for feature in ranked_features
            if feature not in set(first + second)
        ]
        for first, second in combinations(low_blocks, 2)
    )
    evaluate(initial_candidates, 'bloques_5')

    for batch_size, bottom_count, beam_width, stage in [
        (3, 15, 6, 'refinamiento_3'),
        (1, 10, 8, 'refinamiento_1'),
    ]:
        leaders = rank_feature_search_results(list(evaluated.values()))[:beam_width]
        candidates = []
        for leader in leaders:
            current = leader['feature_cols']
            weakest = sorted(
                current,
                key=lambda feature: rank_position[feature],
                reverse=True,
            )[:bottom_count]
            removal_groups = [
                weakest[index:index + batch_size]
                for index in range(0, len(weakest), batch_size)
            ]
            for removal_group in removal_groups:
                candidates.append([
                    feature for feature in current
                    if feature not in removal_group
                ])
        evaluate(candidates, stage)

    ranked_results = rank_feature_search_results(list(evaluated.values()))
    selected = ranked_results[0]
    print(
        f"\nMatriz completada: {len(ranked_results)} subconjuntos; "
        f"ganador con {selected['feature_count']} variables, "
        f"MAE={selected['temporal_oof_mae']:.3f}, "
        f"ROC-AUC={selected['temporal_oof_roc_auc']:.3f}, "
        f"F1={selected['temporal_oof_f1']:.3f}"
    )
    return selected['feature_cols'], ranked_results, selected, stability_ranking


def train_model_pipeline(
    df,
    exclude_cols,
    models_dir,
    prefix="",
    forced_feature_cols=None,
    selection_metadata=None,
):
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
    if forced_feature_cols is None:
        print("Calculando la importancia de variables en el set de entrenamiento...")
        base_reg = GradientBoostingRegressor(**MODEL_PARAMS)
        base_reg.fit(X_train_initial, y_reg_train_initial)
        importance_threshold = 0.008
        importances = base_reg.feature_importances_
        feature_cols = [
            col for col, imp in zip(initial_feature_cols, importances)
            if imp >= importance_threshold
        ]
        selection_method = f"single_train_importance_ge_{importance_threshold}"
    else:
        feature_cols = list(forced_feature_cols)
        selection_method = "temporal_stability_top_n"

    print(f"Features iniciales: {len(initial_feature_cols)}")
    print(f"Features seleccionadas: {len(feature_cols)}")
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
    oof_reg_predictions = np.full(len(X_train), np.nan)

    for fold, (train_idx, val_idx) in enumerate(time_cv.split(X_train), start=1):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_clf_train.iloc[train_idx], y_clf_train.iloc[val_idx]

        if y_tr.nunique() < 2:
            print(f"Fold {fold}: omitido porque el bloque de entrenamiento tiene una sola clase.")
            continue

        clf_fold = RandomForestClassifier(**CLASSIFIER_PARAMS)
        clf_fold.fit(X_tr, y_tr)
        oof_probs[val_idx] = clf_fold.predict_proba(X_val)[:, 1]

        reg_fold = GradientBoostingRegressor(**MODEL_PARAMS)
        reg_fold.fit(X_tr, y_reg_train.iloc[train_idx])
        oof_reg_predictions[val_idx] = reg_fold.predict(X_val)

    valid_oof = ~np.isnan(oof_probs)
    if not valid_oof.any():
        raise RuntimeError("No fue posible generar probabilidades OOF para calibrar el clasificador.")

    y_clf_oof = y_clf_train.iloc[np.flatnonzero(valid_oof)]
    oof_probs_valid = oof_probs[valid_oof]
    y_reg_oof = y_reg_train.iloc[np.flatnonzero(valid_oof)]
    reg_oof_valid = np.clip(oof_reg_predictions[valid_oof], 0.05, None)
    negative_binomial_alpha = float(max(
        np.sum((y_reg_oof.to_numpy() - reg_oof_valid) ** 2 - reg_oof_valid)
        / np.sum(reg_oof_valid ** 2),
        1e-4,
    ))
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
    operational_threshold = 0.50
    operational_precision_target = 0.30
    operational_precision = 0.0
    operational_recall = -1.0
    for t in np.arange(0.05, 0.85, 0.01):
        preds = (oof_probs_valid >= t).astype(int)
        precision = precision_score(y_clf_oof, preds, zero_division=0)
        recall = recall_score(y_clf_oof, preds, zero_division=0)
        if precision >= operational_precision_target and recall > operational_recall:
            operational_threshold = t
            operational_precision = precision
            operational_recall = recall

    print(
        f"Umbral operativo de refuerzo: {operational_threshold:.2f} "
        f"(precision OOF={operational_precision:.3f}, recall OOF={operational_recall:.3f})"
    )

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
    y_operational_pred = (y_clf_prob >= operational_threshold).astype(int)
    
    acc = accuracy_score(y_clf_test, y_clf_pred)
    prec = precision_score(y_clf_test, y_clf_pred, zero_division=0)
    rec = recall_score(y_clf_test, y_clf_pred, zero_division=0)
    f1 = f1_score(y_clf_test, y_clf_pred, zero_division=0)
    roc_auc = roc_auc_score(y_clf_test, y_clf_prob)
    cm = confusion_matrix(y_clf_test, y_clf_pred)
    operational_test_precision = precision_score(
        y_clf_test, y_operational_pred, zero_division=0
    )
    operational_test_recall = recall_score(
        y_clf_test, y_operational_pred, zero_division=0
    )
    
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
        'selection_method': selection_method,
        'umbral_alta_actividad': UMBRAL_ALTA_ACTIVIDAD,
        'classification_threshold': best_threshold,
        'operational_reinforcement_threshold': operational_threshold,
        'operational_precision_target': operational_precision_target,
        'operational_oof_precision': operational_precision,
        'operational_oof_recall': operational_recall,
        'operational_test_precision': operational_test_precision,
        'operational_test_recall': operational_test_recall,
        'threshold_metric': 'youden_j_temporal_oof',
        'threshold_score': best_youden,
        'negative_binomial_alpha': negative_binomial_alpha,
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
    if selection_metadata:
        metadata.update(selection_metadata)
    
    with open(f"{models_dir}/regressor{prefix}.pkl", "wb") as f:
        pickle.dump(reg_model, f)
        
    with open(f"{models_dir}/classifier{prefix}.pkl", "wb") as f:
        pickle.dump(clf_model, f)
        
    with open(f"{models_dir}/metadata{prefix}.pkl", "wb") as f:
        pickle.dump(metadata, f)

    print(f"¡Modelos y metadatos guardados con éxito en {models_dir} con prefijo '{prefix}'!")
    return metadata

def main():
    print("=== Paso 3: Modelos climaticos aumentados Full vs. Pruneado ===")

    # 1. Definir rutas
    base_dir = Path(__file__).resolve().parent.parent
    data_path = base_dir / "02_data" / "augmented_emergency_data.csv"
    models_dir = base_dir / "03_model" / "saved_models"
    os.makedirs(models_dir, exist_ok=True)

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"No se encontró el dataset: {data_path}")

    # 2. Cargar datos
    df = pd.read_csv(data_path, sep=';')
    weekday_columns = {
        0: 'DIA_LUNES',
        1: 'DIA_MARTES',
        2: 'DIA_MIERCOLES',
        3: 'DIA_JUEVES',
        4: 'DIA_VIERNES',
        5: 'DIA_SABADO',
        6: 'DIA_DOMINGO',
    }
    include_weekday = os.getenv('INCLUDE_WEEKDAY', '0') == '1'
    if include_weekday:
        for weekday, column in weekday_columns.items():
            df[column] = (df['DIA_SEMANA'] == weekday).astype(int)
    print(f"Dataset cargado con {df.shape[0]} registros.")

    # 3. Variables que no pueden entrar en los modelos climaticos.
    calendar_cols = [
        'MES', 'DIA_SEMANA', 'ES_FIN_SEMANA', 'ES_FERIADO',
        'ES_FERIADO_IRRENUNCIABLE', 'MES_SIN', 'MES_COS',
        'DIA_SIN', 'DIA_COS', 'DANO_SIN', 'DANO_COS'
    ]
    operational_event_cols = [
        'EVENTOS_lag_1', 'EVENTOS_lag_2', 'EVENTOS_lag_3', 'EVENTOS_lag_7',
        'N_INCENDIO_ESTR_lag_1', 'N_INCENDIO_FOREST_lag_1', 'N_RESCATE_VEH_lag_1',
        'N_RESCATE_PERS_lag_1', 'N_GASES_lag_1',
        'EVENTOS_rolling_mean_3d', 'EVENTOS_rolling_std_3d', 'EVENTOS_rolling_max_3d',
        'EVENTOS_rolling_mean_7d', 'EVENTOS_rolling_std_7d', 'EVENTOS_rolling_max_7d'
    ]
    category_target_cols = [
        'N_INCENDIO_ESTR', 'N_INCENDIO_FOREST', 'N_RESCATE_VEH',
        'N_RESCATE_PERS', 'N_GASES', 'N_OTROS',
    ]
    exclude_cols_climatic_augmented = (
        calendar_cols + operational_event_cols + category_target_cols
    )
    climatic_candidates = [
        col for col in df.columns
        if col not in ['FECHA_DIA', 'EVENTOS'] + exclude_cols_climatic_augmented
    ]

    # Full usa todas las variables climaticas aumentadas disponibles.
    metadata_full = train_model_pipeline(
        df,
        exclude_cols=exclude_cols_climatic_augmented,
        models_dir=models_dir,
        prefix="_climatic_augmented_full",
        forced_feature_cols=climatic_candidates,
        selection_metadata={
            'variant': 'full',
            'candidate_feature_count': len(climatic_candidates),
            'weekday_encoding': list(weekday_columns.values()) if include_weekday else [],
        },
    )

    # El modelo seleccionado busca el numero de variables mediante validacion
    # temporal, favoreciendo el conjunto mas amplio que conserve rendimiento.
    (
        climatic_selected_features,
        feature_count_results,
        selected_count_result,
        climatic_ranking,
    ) = staged_parallel_feature_search(
        df,
        climatic_candidates,
        split_idx=int(len(df) * 0.8),
    )
    search_export = pd.DataFrame(feature_count_results).copy()
    search_export['feature_cols'] = search_export['feature_cols'].apply(
        lambda features: '|'.join(features)
    )
    search_export.to_csv(
        models_dir / "feature_subset_search.csv",
        index=False,
        sep=';',
    )
    metadata_pruned = train_model_pipeline(
        df,
        exclude_cols=exclude_cols_climatic_augmented,
        models_dir=models_dir,
        prefix="_climatic_augmented_pruned",
        forced_feature_cols=climatic_selected_features,
        selection_metadata={
            'variant': 'pruned',
            'pruning_top_n': len(climatic_selected_features),
            'pruning_cv': 'TimeSeriesSplit(n_splits=5)',
            'feature_stability_ranking': climatic_ranking,
            'feature_count_search': feature_count_results,
            'selected_feature_count_result': selected_count_result,
            'selection_method': 'staged_parallel_subset_search',
            'feature_count_selection_rule': 'lowest_mean_rank_oof_mae_auc_f1',
            'search_parallel_jobs': SEARCH_N_JOBS,
            'search_candidate_count': len(feature_count_results),
            'weekday_encoding': list(weekday_columns.values()) if include_weekday else [],
        },
    )
    top_15_features = [name for name, _ in climatic_ranking[:15]]
    train_model_pipeline(
        df,
        exclude_cols=exclude_cols_climatic_augmented,
        models_dir=models_dir,
        prefix="_climatic_augmented_15",
        forced_feature_cols=top_15_features,
        selection_metadata={
            'variant': 'top15',
            'pruning_top_n': 15,
            'pruning_cv': 'TimeSeriesSplit(n_splits=5)',
            'feature_stability_ranking': climatic_ranking,
            'is_primary': False,
            'comparison_role': 'compact_positive_r2',
            'weekday_encoding': [],
        },
    )

    candidates = {'full': metadata_full, 'pruned': metadata_pruned}
    winner = 'pruned'
    selection_rule = 'temporal_oof_feature_count_search'

    for name, metadata in candidates.items():
        metadata['is_primary'] = name == winner
        metadata['selection_rule'] = selection_rule
        with open(models_dir / f"metadata_climatic_augmented_{name}.pkl", "wb") as f:
            pickle.dump(metadata, f)

    winner_prefix = f"_climatic_augmented_{winner}"
    shutil.copy2(
        models_dir / f"regressor{winner_prefix}.pkl",
        models_dir / "regressor_climatic_augmented.pkl",
    )
    shutil.copy2(
        models_dir / f"classifier{winner_prefix}.pkl",
        models_dir / "classifier_climatic_augmented.pkl",
    )
    primary_metadata = candidates[winner].copy()
    primary_metadata['selected_variant'] = winner
    with open(models_dir / "metadata_climatic_augmented.pkl", "wb") as f:
        pickle.dump(primary_metadata, f)

    print(
        f"Modelo principal: {winner.upper()} "
        f"(MAE={primary_metadata['mae']:.3f}, "
        f"ROC-AUC={primary_metadata['roc_auc']:.3f}, "
        f"F1={primary_metadata['f1']:.3f})"
    )

    if os.getenv("TRAIN_CATEGORY_BLEND", "1") == "1":
        print("\nOptimizando modelos separados por tipo de emergencia...")
        from optimize_category_models import main as optimize_category_models

        optimize_category_models()


if __name__ == "__main__":
    main()
