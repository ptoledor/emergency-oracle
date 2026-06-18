import pandas as pd, numpy as np, pickle, os, sys
from pathlib import Path

os.chdir('C:/Trabajo/742_oracle')
sys.path.insert(0, 'C:/Trabajo/742_oracle')
from model_components import CategoryBlendRegressor

models_dir = Path('03_model/saved_models')
data_path = Path('02_data/augmented_emergency_data.csv')

# === Replicate load_data_and_predict ===
df = pd.read_csv(data_path, sep=';')
df = df.sort_values('FECHA_DIA').reset_index(drop=True)

weekday_columns = {0:'DIA_LUNES',1:'DIA_MARTES',2:'DIA_MIERCOLES',3:'DIA_JUEVES',4:'DIA_VIERNES',5:'DIA_SABADO',6:'DIA_DOMINGO'}
for weekday, column in weekday_columns.items():
    df[column] = (df['DIA_SEMANA'] == weekday).astype(int)

# Load models
reg_model = pickle.load(open(models_dir / "regressor_climatic_augmented.pkl", "rb"))
metadata = pickle.load(open(models_dir / "metadata_climatic_augmented.pkl", "rb"))

# Blend features
all_blend_features = set(metadata['feature_cols'])
for details in getattr(reg_model, 'category_models', {}).values():
    all_blend_features.update(details.get('feature_cols', []))
aug_features = [c for c in sorted(all_blend_features) if c in df.columns]
X_aug = df[aug_features]
df['PRED_EVENTOS_PRIMARY'] = reg_model.predict(X_aug)

# Classifier
try:
    clf_model = pickle.load(open(models_dir / "classifier_climatic_augmented.pkl", "rb"))
    clf_features = list(getattr(clf_model, 'feature_names_in_', metadata['feature_cols']))
    X_clf = df[[c for c in clf_features if c in df.columns]]
    df['PROB_ALTA_PRIMARY'] = clf_model.predict_proba(X_clf)[:, 1]
except Exception as e:
    print('Classifier error:', e)
    df['PROB_ALTA_PRIMARY'] = np.nan

# === Compute thresholds (as dashboard does now) ===
train_end_date = str(metadata.get('train_end_date', ''))
train_mask = df['FECHA_DIA'].astype(str) <= train_end_date
df_train = df[train_mask]

train_events = df_train['EVENTOS'].astype(float)
activity_p33 = float(train_events.quantile(0.33))
activity_p66 = float(train_events.quantile(0.66))
activity_p80 = float(train_events.quantile(0.80))

print('=== THRESHOLDS ===')
print('train_end_date:', train_end_date)
print('p33=%.1f p66=%.1f p80=%.1f' % (activity_p33, activity_p66, activity_p80))
print('train rows:', len(df_train))
print('all rows:', len(df))
print()

# === Show percentile table ===
print('=== PERCENTILE TABLE (EVENTOS) ===')
PERCENTILE_COLUMNS = [0, 10, 20, 30, 33, 40, 50, 60, 66, 70, 80, 90, 100]
vals = []
for p in PERCENTILE_COLUMNS:
    v = float(df['EVENTOS'].astype(float).quantile(p / 100))
    mark = ' <--' if p in [33, 66, 80] else ''
    vals.append('p%d=%.1f%s' % (p, v, mark))
    print('  p%3d: %.1f%s' % (p, v, mark))
print('  mean=%.2f std=%.2f' % (df['EVENTOS'].mean(), df['EVENTOS'].std()))
print()

# === Show forecast card activity levels for last 6 days ===
print('=== LAST 6 DAYS (as forecast cards would show) ===')
for _, row in df.tail(6).iterrows():
    pred = float(row['PRED_EVENTOS_PRIMARY'])
    if pred < activity_p33:
        level = 'BAJA'
    elif pred < activity_p66:
        level = 'NORMAL'
    elif pred < activity_p80:
        level = 'ALTA'
    else:
        level = 'MUY ALTA'
    print('%s  pred=%.2f  -> %s' % (row['FECHA_DIA'], pred, level))

print()
print('=== DISTRIBUTION OF ACTIVITY LEVELS (all predictions) ===')
preds = df['PRED_EVENTOS_PRIMARY'].astype(float)
baja = (preds < activity_p33).sum()
normal = ((preds >= activity_p33) & (preds < activity_p66)).sum()
alta = ((preds >= activity_p66) & (preds < activity_p80)).sum()
muy_alta = (preds >= activity_p80).sum()
print('BAJA:      %4d (%.1f%%)' % (baja, baja/len(preds)*100))
print('NORMAL:    %4d (%.1f%%)' % (normal, normal/len(preds)*100))
print('ALTA:      %4d (%.1f%%)' % (alta, alta/len(preds)*100))
print('MUY ALTA:  %4d (%.1f%%)' % (muy_alta, muy_alta/len(preds)*100))
