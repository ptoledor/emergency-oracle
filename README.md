# Emergency Oracle

Dashboard Streamlit para pronosticar la actividad diaria de emergencias usando clima, estacionalidad y modelos por tipo de evento.

## Modelo principal

La predicción combina:

- 53% modelo directo optimizado de 31 variables.
- 47% suma de seis modelos especializados por categoría.

Resultados en el bloque temporal de prueba:

- MAE: 2.418 llamadas.
- R²: 0.049.
- ROC-AUC de alta actividad: 0.610.

El flujo de entrenamiento está descrito en [FLUJO_ENTRENAMIENTO.md](FLUJO_ENTRENAMIENTO.md).

## Ejecución

```powershell
pip install -r requirements.txt
streamlit run dashboard.py
```
