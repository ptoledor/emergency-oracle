"""Console forecast for one day, using the same pipeline as the dashboard.

Usage:
    python 04_predict/predict_tomorrow.py --real-tomorrow   # mañana real
    python 04_predict/predict_tomorrow.py --date 2026-05-01 # fecha específica
    python 04_predict/predict_tomorrow.py                   # día siguiente al dataset
"""

import argparse
import datetime
import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from forecasting import fetch_weather, forecast_days, load_forecast_models

PROJECT_TIMEZONE = ZoneInfo("America/Santiago")
DATA_PATH = PROJECT_ROOT / "02_data" / "augmented_emergency_data.csv"


def project_today():
    return datetime.datetime.now(PROJECT_TIMEZONE).date()


def main():
    parser = argparse.ArgumentParser(description="Predictor de emergencias para Bomberos de Talcahuano")
    parser.add_argument("--date", type=str, help="Fecha a predecir (YYYY-MM-DD).")
    parser.add_argument("--real-tomorrow", action="store_true", help="Predice el día de mañana real.")
    args = parser.parse_args()

    history = pd.read_csv(DATA_PATH, sep=";").sort_values("FECHA_DIA").reset_index(drop=True)
    last_observed = datetime.date.fromisoformat(history["FECHA_DIA"].iloc[-1])
    today = project_today()

    # The dashboard issues its forecast today; tomorrow is its second day.
    if args.real_tomorrow:
        origin, step, mode = today, 1, "MAÑANA REAL"
    elif args.date:
        origin, step, mode = datetime.date.fromisoformat(args.date), 0, "FECHA ESPECÍFICA"
    else:
        origin, step, mode = last_observed + datetime.timedelta(days=1), 0, "DÍA SIGUIENTE AL DATASET"
    # Archive weather covers fully past windows; the live forecast covers today..today+9.
    is_historical = origin + datetime.timedelta(days=5) < today
    if not is_historical and not today <= origin <= today + datetime.timedelta(days=4):
        parser.error(
            "Sin clima disponible para esa fecha: usa una fecha con 6 días ya transcurridos "
            "o entre hoy y hoy + 4 días."
        )

    models = load_forecast_models()
    weather = fetch_weather(origin, is_historical)
    predictions, threshold, high_activity = forecast_days(origin, weather, history, models)
    day = predictions[step]
    reinforcement_threshold = 0.50  # P(>7) que activa refuerzo operativo

    print(f"=== Predicción de Emergencias (Modo: {mode}) ===")
    print(f"Último dato observado: {last_observed} | Historial usado: {day['Lag_Mode']}")
    if weather.get("_source", [""])[0] == "local_fallback":
        print("Open-Meteo no disponible: se usa clima estimado desde el histórico local.")
    print(
        f"Clima {day['FechaStr']}: temp. máx {day['Temp_Max']:.1f}°C, humedad {day['Hum_Media']:.0f}%, "
        f"viento {day['Viento_Medio']:.1f} km/h, lluvia {day['Lluvia']:.1f} mm"
    )

    count, probability = day["Prediccion"], day["Prob_Alta"]
    print("\n" + "=" * 50)
    print(" REPORTE DE PREDICCIÓN - BOMBEROS TALCAHUANO")
    print(f" Fecha del reporte: {today} | Fecha predicción: {day['FechaStr']} ({day['Dia']})")
    print("=" * 50)
    print(f" Cantidad esperada de emergencias: {count:.1f} llamados")
    print(
        f" Rango 80% (validación del día {day['Horizonte']} del forecast): "
        f"{day['Intervalo_80_Bajo']:.0f}–{day['Intervalo_80_Alto']:.0f} llamados"
        + ("" if day["Confianza_Horizonte"] else " · baja confianza")
    )
    print(
        f" Probabilidad de día crítico (>{high_activity:.0f} llamados): {probability * 100:.1f}% "
        f"(umbral de alerta: {threshold * 100:.1f}%)"
    )
    print("-" * 50)
    if count >= 8.0 or probability >= reinforcement_threshold:
        print("  ORDEN OPERATIVA: REFORZAR")
        print("  ACCIÓN: CONVOCAR DOTACIÓN ADICIONAL")
    elif probability >= threshold:
        print("  ORDEN OPERATIVA: PREALERTA")
        print("  ACCIÓN: CONFIRMAR DISPONIBILIDAD Y MANTENER PERSONAL LOCALIZABLE")
    else:
        print("  ORDEN OPERATIVA: GUARDIA NORMAL")
        print("  ACCIÓN: MANTENER DOTACIÓN ORDINARIA")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
