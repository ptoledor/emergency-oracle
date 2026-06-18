import sys, os, types
os.chdir('C:/Trabajo/742_oracle')
sys.path.insert(0, 'C:/Trabajo/742_oracle')

# Patch streamlit
st = types.ModuleType('streamlit')
st.cache_data = lambda *a, **kw: (lambda f: f)
st.cache_resource = lambda *a, **kw: (lambda f: f)
st.markdown = lambda *a, **kw: None
st.columns = lambda n: [None]*n
st.selectbox = lambda *a, **kw: None
st.slider = lambda *a, **kw: 7
st.warning = lambda *a, **kw: None
st.error = lambda *a, **kw: None
st.metric = lambda *a, **kw: None
st.plotly_chart = lambda *a, **kw: None
st.dataframe = lambda *a, **kw: None
st.expander = lambda *a, **kw: types.SimpleNamespace(__enter__=lambda s:s, __exit__=lambda *a:None)
st.tabs = lambda *a, **kw: [types.SimpleContextMenu() for _ in a] if False else [None]*len(a)
st.subheader = lambda *a, **kw: None
st.write = lambda *a, **kw: None
st.date_input = lambda *a, **kw: None
st.checkbox = lambda *a, **kw: False
st.number_input = lambda *a, **kw: 0
st.set_page_config = lambda *a, **kw: None
st.session_state = {}
st.set_option = lambda *a, **kw: None
st.sidebar = type('S', (), {
    'selectbox': lambda *a, **kw: None,
    'markdown': lambda *a, **kw: None,
    'write': lambda *a, **kw: None,
    'checkbox': lambda *a, **kw: False,
})()
sys.modules['streamlit'] = st

# Read dashboard code up to the app header
with open('dashboard.py', 'r', encoding='utf-8') as f:
    code = f.read()

# Split at the header section
parts = code.split('# 8. Encabezado principal')
exec(parts[0])

# Call load_data_and_predict
result = load_data_and_predict()
df = result[0]
if df is not None:
    print('SUCCESS: load_data_and_predict returned data')
    print('Rows:', len(df))
    has_pred = 'PRED_EVENTOS_PRIMARY' in df.columns
    print('PRED_EVENTOS_PRIMARY exists:', has_pred)
    if has_pred:
        preds = df['PRED_EVENTOS_PRIMARY'].astype(float)
        print('Prediction stats: min=%.2f mean=%.2f max=%.2f' % (preds.min(), preds.mean(), preds.max()))
    
    # Check train thresholds
    meta = result[5]
    train_end = str(meta.get('train_end_date', ''))
    print('train_end_date:', train_end)
    train_mask = df['FECHA_DIA'].astype(str) <= train_end
    df_train = df[train_mask]
    train_events = df_train['EVENTOS'].astype(float)
    print('Thresholds: p33=%.1f p66=%.1f p80=%.1f' % (
        train_events.quantile(0.33), train_events.quantile(0.66), train_events.quantile(0.80)))
    
    # Show what activity levels the last 6 days would get
    print()
    print('=== LAST 6 DAYS ACTIVITY ===')
    for _, row in df.tail(6).iterrows():
        pred = row.get('PRED_EVENTOS_PRIMARY', 0)
        p33 = float(train_events.quantile(0.33))
        p66 = float(train_events.quantile(0.66))
        p80 = float(train_events.quantile(0.80))
        if pred < p33:
            level = 'BAJA'
        elif pred < p66:
            level = 'NORMAL'
        elif pred < p80:
            level = 'ALTA'
        else:
            level = 'MUY ALTA'
        print('%s  pred=%.2f  -> %s' % (row['FECHA_DIA'], pred, level))
else:
    print('FAILED: df is None')
