"""Leakage-safe second-cycle feature ablations for the signal model."""

from __future__ import annotations

import os
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

from search_signal_models import N_SPLITS, TEST_SIZE, load_data, metrics


ACTIVE_PARAMS = {
    "objective": "reg:squarederror", "n_estimators": 300, "max_depth": 3,
    "learning_rate": 0.03, "min_child_weight": 2, "reg_lambda": 1.0,
    "subsample": 0.85, "colsample_bytree": 0.85, "n_jobs": 1,
    "random_state": 42,
}
D2_PARAMS = {**ACTIVE_PARAMS, "n_estimators": 450, "max_depth": 2}


def add_history_features(df):
    frame = df.copy()
    y = frame["EVENTOS"].astype(float)
    shifted = y.shift(1)
    weekly = []
    for lag in [14, 21, 28, 35, 42, 49, 56]:
        column = f"EVENTOS_lag_{lag}"
        frame[column] = y.shift(lag)
        weekly.append(column)
    same_dow = pd.concat([y.shift(7 * step) for step in range(1, 9)], axis=1)
    for weeks in [4, 8]:
        columns = same_dow.iloc[:, :weeks]
        frame[f"EVENTOS_same_dow_mean_{weeks}w"] = columns.mean(axis=1)
        frame[f"EVENTOS_same_dow_max_{weeks}w"] = columns.max(axis=1)
        weekly.extend([
            f"EVENTOS_same_dow_mean_{weeks}w",
            f"EVENTOS_same_dow_max_{weeks}w",
        ])

    regime = []
    for span in [7, 14, 28]:
        column = f"EVENTOS_ewm_{span}d"
        frame[column] = shifted.ewm(span=span, adjust=False, min_periods=3).mean()
        regime.append(column)
    for window in [7, 14, 30, 56]:
        rolling = shifted.rolling(window, min_periods=min(7, window))
        frame[f"EVENTOS_rolling_median_{window}d"] = rolling.median()
        frame[f"EVENTOS_high_rate_{window}d"] = (shifted > 7).rolling(
            window, min_periods=min(7, window)
        ).mean()
        regime.extend([
            f"EVENTOS_rolling_median_{window}d",
            f"EVENTOS_high_rate_{window}d",
        ])
    frame["EVENTOS_trend_7_30"] = (
        shifted.rolling(7, min_periods=3).mean()
        - shifted.rolling(30, min_periods=7).mean()
    )
    regime.append("EVENTOS_trend_7_30")
    days_since_high = []
    last_high = None
    for index, value in enumerate(y):
        days_since_high.append(index - last_high if last_high is not None else np.nan)
        if value > 7:
            last_high = index
    frame["EVENTOS_days_since_high"] = days_since_high
    regime.append("EVENTOS_days_since_high")
    return frame, weekly, regime


def add_calendar_features(df):
    frame = df.copy()
    date = pd.to_datetime(frame["FECHA_DIA"])
    frame["DIA_MES"] = date.dt.day
    frame["DIAS_FIN_MES"] = date.dt.days_in_month - date.dt.day
    frame["ES_INICIO_MES"] = (date.dt.day <= 3).astype(int)
    frame["ES_FIN_MES"] = (frame["DIAS_FIN_MES"] <= 3).astype(int)
    frame["ES_VENTANA_PAGO"] = (
        date.dt.day.between(14, 16) | (frame["DIAS_FIN_MES"] <= 2)
    ).astype(int)
    iso_week = date.dt.isocalendar().week.astype(float)
    frame["SEMANA_ANO_SIN"] = np.sin(2 * np.pi * iso_week / 52.1775)
    frame["SEMANA_ANO_COS"] = np.cos(2 * np.pi * iso_week / 52.1775)
    columns = [
        "DIA_MES", "DIAS_FIN_MES", "ES_INICIO_MES", "ES_FIN_MES",
        "ES_VENTANA_PAGO", "SEMANA_ANO_SIN", "SEMANA_ANO_COS",
    ]
    return frame, columns


def add_partial_soil_features(base_dir, df):
    weather = pd.read_csv(
        base_dir / "05_research" / "data" / "historical_forecast_features.csv",
        sep=";",
    )
    columns = ["WX_SOIL_MOISTURE_SURFACE_MEAN", "WX_SOIL_MOISTURE_DEEP_MEAN"]
    available = [column for column in columns if column in weather]
    if not available:
        return df, []
    weather["FECHA_DIA"] = pd.to_datetime(weather["FECHA_DIA"]).dt.strftime("%Y-%m-%d")
    return df.merge(
        weather[["FECHA_DIA", *available]], on="FECHA_DIA", how="left",
        validate="one_to_one",
    ), available


def main():
    base_dir = Path(__file__).resolve().parent.parent
    output = base_dir / "05_research" / "results" / "signal_feature_v2"
    output.mkdir(parents=True, exist_ok=True)
    df, feature_sets, _ = load_data(base_dir)
    base_features = feature_sets["all_operational"]
    df, weekly, regime = add_history_features(df)
    df, calendar = add_calendar_features(df)
    df, soil = add_partial_soil_features(base_dir, df)
    history_core_5 = [
        "EVENTOS_high_rate_14d", "EVENTOS_ewm_28d",
        "EVENTOS_rolling_median_30d", "EVENTOS_ewm_14d",
        "EVENTOS_high_rate_56d",
    ]
    history_core_10 = history_core_5 + [
        "EVENTOS_ewm_7d", "EVENTOS_same_dow_mean_4w",
        "EVENTOS_high_rate_30d", "EVENTOS_lag_49",
        "EVENTOS_same_dow_mean_8w",
    ]
    history_core_15 = history_core_10 + [
        "EVENTOS_lag_28", "EVENTOS_trend_7_30", "EVENTOS_lag_56",
        "EVENTOS_lag_14", "EVENTOS_same_dow_max_8w",
    ]
    groups = {
        "base": base_features,
        "history_core5": base_features + history_core_5,
        "history_core10": base_features + history_core_10,
        "history_core15": base_features + history_core_15,
        "history_core10_calendar": base_features + history_core_10 + calendar,
        "weekly": base_features + weekly,
        "regime": base_features + regime,
        "history_all": base_features + weekly + regime,
        "calendar_v2": base_features + calendar,
        "soil": base_features + soil,
        "all_v2": base_features + weekly + regime + calendar + soil,
    }
    y = df["EVENTOS"].astype(float)
    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(df))
    predictions = None
    result_rows = []
    fold_rows = []
    for params_name, params in [("d3", ACTIVE_PARAMS), ("d2", D2_PARAMS)]:
        for group_name, features in groups.items():
            candidate = f"{params_name}_{group_name}"
            print(candidate, flush=True)
            values = []
            rows = []
            folds = []
            for fold, (train_idx, test_idx) in enumerate(splits, start=1):
                model = XGBRegressor(**params).fit(
                    df[features].iloc[train_idx], y.iloc[train_idx]
                )
                prediction = np.clip(model.predict(df[features].iloc[test_idx]), 0, None)
                values.extend(prediction)
                rows.extend(test_idx)
                folds.extend([fold] * len(test_idx))
                fold_rows.append({
                    "candidate": candidate, "fold": fold,
                    **metrics(y.iloc[test_idx], prediction),
                })
            if predictions is None:
                predictions = pd.DataFrame({
                    "row": rows, "FECHA_DIA": df["FECHA_DIA"].iloc[rows].to_numpy(),
                    "EVENTOS": y.iloc[rows].to_numpy(), "fold": folds,
                })
            predictions[candidate] = values
            overall = metrics(predictions["EVENTOS"], values)
            confirm = metrics(
                predictions.loc[predictions["fold"] >= 5, "EVENTOS"],
                np.asarray(values)[predictions["fold"].to_numpy() >= 5],
            )
            result_rows.append({
                "candidate": candidate, "feature_count": len(features),
                **overall, **{f"confirm_{key}": value for key, value in confirm.items()},
            })
            pd.DataFrame(result_rows).to_csv(output / "leaderboard_partial.csv", sep=";", index=False)
    results = pd.DataFrame(result_rows).sort_values(["rmse", "mae"])
    results.to_csv(output / "leaderboard.csv", sep=";", index=False)
    predictions.to_csv(output / "oof_predictions.csv", sep=";", index=False)
    pd.DataFrame(fold_rows).to_csv(output / "fold_metrics.csv", sep=";", index=False)
    print(results[[
        "candidate", "feature_count", "mae", "rmse", "r2", "high_auc",
        "std_ratio", "boring_4_5_share", "confirm_rmse",
    ]].to_string(index=False, float_format="%.4f"))


if __name__ == "__main__":
    main()
