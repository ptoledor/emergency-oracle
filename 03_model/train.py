import pandas as pd
import numpy as np
import os
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix
)

def main():
    print("=== Paso 3: Ajuste del Modelo ===")

    # 1. Definir rutas
    base_dir = "c:/Users/ptole/Desktop/Pitters-Git/emergency-oracle"
    data_path = f"{base_dir}/02_data/augmented_emergency_data.csv"
    models_dir = f"{base_dir}/03_model/saved_models"
    os.makedirs(models_dir, exist_ok=True)

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"No se encontró el dataset: {data_path}")

    # 2. Cargar datos
    df = pd.read_csv(data_path, sep=';')
    print(f"Dataset cargado con {df.shape[0]} registros.")

    # 3. Definir características y variables objetivo
    # Excluimos FECHA_DIA y la variable objetivo EVENTOS
    feature_cols = [
        'TEMP_MAX', 'TEMP_MIN', 'TEMP_MEDIA',
        'HUM_MAX', 'HUM_MIN', 'HUM_MEDIA',
        'VIENTO_MAX', 'VIENTO_MEDIO',
        'LLUVIA', 
        'EVENTOS_lag_1', 'EVENTOS_lag_2', 'EVENTOS_lag_3',
        'EVENTOS_rolling_mean_3d',
        'LLUVIA_lag_1', 'LLUVIA_lag_2', 'LLUVIA_lag_3',
        'LLUVIA_accum_3d',
        'MES', 'DIA_SEMANA', 'ES_FIN_SEMANA', 'ES_FERIADO'
    ]
    
    # Umbral para Alta Actividad (más de 7 eventos diarios)
    UMBRAL_ALTA_ACTIVIDAD = 7

    X = df[feature_cols]
    y_reg = df['EVENTOS']
    y_clf = (df['EVENTOS'] > UMBRAL_ALTA_ACTIVIDAD).astype(int)

    # 4. División temporal (Time-Series Split) para evitar fuga de datos
    # 80% entrenamiento, 20% prueba en base al orden cronológico de las filas
    split_idx = int(len(df) * 0.8)
    
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_reg_train, y_reg_test = y_reg.iloc[:split_idx], y_reg.iloc[split_idx:]
    y_clf_train, y_clf_test = y_clf.iloc[:split_idx], y_clf.iloc[split_idx:]

    print(f"Datos de Entrenamiento: {X_train.shape[0]} días (desde {df['FECHA_DIA'].iloc[0]} hasta {df['FECHA_DIA'].iloc[split_idx-1]})")
    print(f"Datos de Prueba: {X_test.shape[0]} días (desde {df['FECHA_DIA'].iloc[split_idx]} hasta {df['FECHA_DIA'].iloc[-1]})")

    # 5. Entrenar Modelo de Regresión (Random Forest Regressor)
    print("\nEntrenando modelo de Regresión (Random Forest)...")
    reg_model = RandomForestRegressor(
        n_estimators=150,
        max_depth=10,
        min_samples_leaf=4,
        random_state=42
    )
    reg_model.fit(X_train, y_reg_train)
    
    # Predicciones de Regresión
    y_reg_pred = reg_model.predict(X_test)
    
    # Métricas de Regresión
    mae = mean_absolute_error(y_reg_test, y_reg_pred)
    mse = mean_squared_error(y_reg_test, y_reg_pred)
    r2 = r2_score(y_reg_test, y_reg_pred)
    
    print("\n--- Métricas del Modelo de Regresión ---")
    print(f"Error Absoluto Medio (MAE): {mae:.3f} eventos")
    print(f"Error Cuadrático Medio (MSE): {mse:.3f}")
    print(f"Coeficiente de Determinación R²: {r2:.3f}")

    # 6. Entrenar Modelo de Clasificación (Random Forest Classifier)
    print("\nEntrenando modelo de Clasificación (Random Forest)...")
    clf_model = RandomForestClassifier(
        n_estimators=150,
        max_depth=8,
        min_samples_leaf=4,
        class_weight='balanced', # Balancear pesos de clases (Alta Actividad es ~15-20% de los datos)
        random_state=42
    )
    clf_model.fit(X_train, y_clf_train)
    
    # Predicciones de Clasificación
    y_clf_pred = clf_model.predict(X_test)
    y_clf_prob = clf_model.predict_proba(X_test)[:, 1]
    
    # Métricas de Clasificación
    acc = accuracy_score(y_clf_test, y_clf_pred)
    prec = precision_score(y_clf_test, y_clf_pred)
    rec = recall_score(y_clf_test, y_clf_pred)
    f1 = f1_score(y_clf_test, y_clf_pred)
    roc_auc = roc_auc_score(y_clf_test, y_clf_prob)
    cm = confusion_matrix(y_clf_test, y_clf_pred)
    
    print("\n--- Métricas del Modelo de Clasificación ---")
    print(f"Exactitud (Accuracy): {acc:.3f}")
    print(f"Precisión (Precision): {prec:.3f}")
    print(f"Sensibilidad (Recall): {rec:.3f}")
    print(f"F1-Score: {f1:.3f}")
    print(f"Área bajo la curva ROC (ROC-AUC): {roc_auc:.3f}")
    print("Matriz de Confusión:")
    print(cm)
    
    # Importancia de las Características (Feature Importance)
    importances = reg_model.feature_importances_
    df_importance = pd.DataFrame({
        'Característica': feature_cols,
        'Importancia': importances
    }).sort_values(by='Importancia', ascending=False)
    
    print("\n--- Importancia de las Características (Modelo Regresión) ---")
    print(df_importance.head(10).to_string(index=False))

    # 7. Guardar los modelos entrenados y los metadatos
    print("\nGuardando modelos entrenados...")
    
    metadata = {
        'feature_cols': feature_cols,
        'umbral_alta_actividad': UMBRAL_ALTA_ACTIVIDAD
    }
    
    with open(f"{models_dir}/regressor.pkl", "wb") as f:
        pickle.dump(reg_model, f)
        
    with open(f"{models_dir}/classifier.pkl", "wb") as f:
        pickle.dump(clf_model, f)
        
    with open(f"{models_dir}/metadata.pkl", "wb") as f:
        pickle.dump(metadata, f)
        
    print(f"¡Modelos y metadatos guardados con éxito en {models_dir}!")

if __name__ == "__main__":
    main()
