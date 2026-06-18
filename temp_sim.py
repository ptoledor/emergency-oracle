import pandas as pd, pickle, numpy as np, sys
sys.path.insert(0, 'C:/Trabajo/742_oracle')
from model_components import CategoryBlendRegressor

# ---- Replicate load_data_and_predict ----
df = pd.read_csv('C:/Trabajo/742_oracle/02_data/augmented_emergency_data.csv', sep=';')
df = df.sort_values('FECHA_DIA').reset_index(drop=True)

weekday_columns = {0:'DIA_LUNES',1:'DIA_MARTES',2:'DIA_MIERCOLES',3:'DIA_JUEVES',4:'DIA_VIERNES',5:'DIA_SABADO',6:'DIA_DOMINGO'}
for weekday, column in weekday_columns.items():
    df[column] = (df['DIA_SEMANA'] == weekday).astype(int)

model = pickle.load(open('C:/Trabajo/742_oracle/03_model/saved_models/regressor_climatic_augmented.pkl','rb'))
meta = pickle.load(open('C:/Trabajo/742_oracle/03_model/saved_models/metadata_climatic_augmented.pkl','rb'))

all_blend_features = set(meta['feature_cols'])
for details in getattr(model, 'category_models', {}).values():
    all_blend_features.update(details.get('feature_cols', []))
aug_features = [c for c in sorted(all_blend_features) if c in df.columns]
X_aug = df[aug_features]
df['PRED_EVENTOS_PRIMARY'] = model.predict(X_aug)

# ---- Replicate activity level logic ----
train_end_date = str(meta.get('train_end_date', ''))
train_mask = df['FECHA_DIA'].astype(str) <= train_end_date if train_end_date else pd.Series([True]*len(df), index=df.index)
df_train = df[train_mask]
train_predictions = df_train['PRED_EVENTOS_PRIMARY'].astype(float)

# NEW thresholds (raw EVENTOS)
train_events = df_train['EVENTOS'].astype(float)
activity_p33 = float(train_events.quantile(0.33))
activity_p66 = float(train_events.quantile(0.66))
activity_p80 = float(train_events.quantile(0.80))

print("=== ACTIVITY THRESHOLDS (from raw EVENTOS) ===")
print(f"p33={activity_p33:.1f}  p66={activity_p66:.1f}  p80={activity_p80:.1f}")
print()

def activity_level(pred, p33, p66, p80):
    if pred < p33:
        return "BAJA"
    if pred < p66:
        return "NORMAL"
    if pred < p80:
        return "ALTA"
    return "MUY ALTA"

# ---- Show last 20 days ----
print("=== LAST 20 DAYS: actual vs predicted vs activity ===")
print(f"{'FECHA':12s} {'ACT':>4s} {'PRED':>6s} {'NIVEL':>10s}")
for _, row in df.tail(20).iterrows():
    pred = row['PRED_EVENTOS_PRIMARY']
    actual = row['EVENTOS']
    level = activity_level(pred, activity_p33, activity_p66, activity_p80)
    print(f"{row['FECHA_DIA']}  {actual:4.0f}  {pred:6.2f}  {level:>10s}")

# ---- Simulate 6-day forecast predictions ----
print()
print("=== SIMULATED FORECAST (using recent weather pattern) ===")
last_row = df.iloc[-1].copy()
for day_offset in range(1, 7):
    pred_val = float(model.predict(pd.DataFrame([last_row[aug_features]]))[0])
    level = activity_level(pred_val, activity_p33, activity_p66, activity_p80)
    print(f"Day +{day_offset}: pred={pred_val:.2f} -> {level}")
