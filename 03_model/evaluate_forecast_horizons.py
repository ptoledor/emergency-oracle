"""Measure 1-6 day forecast error: recursive serving vs direct horizon models.

The dashboard issues six daily forecasts from one origin. The active ensemble
was validated only one day ahead; for later days the dashboard feeds its own
predictions back as event lags and splits them into category lags with
historical shares. This script replays that recursion on the same walk-forward
folds used to validate the active model and compares it with:

- one ensemble per horizon trained on history available at the origin (direct);
- the observed 30-day mean at the origin;
- a month x weekday climatology (the dashboard's neutral history policy).

Weather scenarios for the evaluated days (models are always trained on the
dataset weather, as in production):

- historical_forecast: dataset weather (Historical Forecast API, short lead).
- previous_runs_lead0: Previous Runs API, newest run for every day. Checks that
  rebuilding the features from another source does not change the result.
- previous_runs_lead: day k of a forecast issued on origin o uses the run issued
  k-o days before (as the dashboard sees it); days before o use lead 0.
- previous_runs_lead_conservador: same with one extra day of lead, since an
  hourly `previous_dayN` value is issued N*24h before each hour, which for late
  hours is later than a morning issue.

Previous Runs scenarios need 05_research/data/previous_runs_hourly.csv
(05_research/fetch_previous_runs_weather.py). Nothing is promoted: outputs go
to 05_research/results/forecast_horizons.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from xgboost import XGBRegressor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from model_components import HydroObjectiveEnsembleRegressor
from signal_features import (
    CATEGORY_LAG_FEATURES, OPEN_METEO_HOURLY_VARIABLES,
    aggregate_weather_daily, event_history_features,
)
from build_hydro_ensemble_candidate import (
    CENTER, OFFSET, SPREAD_SCALE, WEIGHTS, load_training_frame,
)
from build_signal_candidate import block_bootstrap_improvement
from search_signal_features_v2 import ACTIVE_PARAMS, D2_PARAMS
from search_signal_models import HIGH_ACTIVITY_THRESHOLD, N_SPLITS, TEST_SIZE, safe_auc


HORIZONS = list(range(1, 7))
CATEGORY_SOURCES = {column: column[: -len("_lag_1")] for column in CATEGORY_LAG_FEATURES}
EVENT_HISTORY_COLS = list(event_history_features(np.zeros(30)))
HISTORY_COLS = EVENT_HISTORY_COLS + CATEGORY_LAG_FEATURES
METHODS = ["recursivo_activo", "directo_por_horizonte", "promedio_30d", "estacional_mes_dia"]
CALENDAR_COLS = [
    "MES", "DIA_SEMANA", "ES_FIN_SEMANA", "ES_FERIADO", "ES_FERIADO_IRRENUNCIABLE",
    "ES_PRE_FERIADO", "MES_SIN", "MES_COS", "DIA_SIN", "DIA_COS", "DANO_SIN", "DANO_COS",
    "DIA_LUNES", "DIA_MARTES", "DIA_MIERCOLES", "DIA_JUEVES", "DIA_VIERNES",
    "DIA_SABADO", "DIA_DOMINGO",
]
RAIN_LAGS = [1, 2, 3, 5, 7, 10, 14]
RAIN_WINDOWS = [3, 7, 14, 30]
PREVIOUS_RUNS_PATH = PROJECT_ROOT / "05_research" / "data" / "previous_runs_hourly.csv"
MAX_LEAD_DAYS = 6
# None: dataset weather; "lead0": newest run everywhere; int: extra lead days.
SCENARIOS = {
    "historical_forecast": None,
    "previous_runs_lead0": "lead0",
    "previous_runs_lead": 0,
    "previous_runs_lead_conservador": 1,
}


def fit_ensemble(X, y, base_features, ensemble_features):
    """Same four objectives, weights and spread transform as the active model."""
    models = {
        "active": XGBRegressor(**ACTIVE_PARAMS).fit(X[base_features], y),
        "squared": XGBRegressor(**D2_PARAMS).fit(X[ensemble_features], y),
        "poisson": XGBRegressor(**{**D2_PARAMS, "objective": "count:poisson"}).fit(
            X[ensemble_features], y
        ),
        "quantile": XGBRegressor(**{
            **D2_PARAMS, "objective": "reg:quantileerror", "quantile_alpha": 0.5,
        }).fit(X[ensemble_features], y),
    }
    return HydroObjectiveEnsembleRegressor(
        models=models,
        model_feature_cols={
            "active": base_features, "squared": ensemble_features,
            "poisson": ensemble_features, "quantile": ensemble_features,
        },
        weights=WEIGHTS, feature_cols=ensemble_features,
        center=CENTER, spread_scale=SPREAD_SCALE, offset=OFFSET,
    )


def calendar_counts(frame):
    """Daily counts on a full calendar; unknown days count as 0 like training lags."""
    dates = pd.to_datetime(frame["FECHA_DIA"])
    index = pd.date_range(dates.min(), dates.max(), freq="D")
    columns = ["EVENTOS", *CATEGORY_SOURCES.values()]
    counts = frame.set_index(dates)[columns].astype(float).reindex(index)
    observed = counts["EVENTOS"].copy()
    return counts.fillna(0.0), observed


def origin_history_table(counts):
    """History features for a forecast issued on each date (data up to day-1)."""
    shifted = counts["EVENTOS"].shift(1)
    table = pd.DataFrame(index=counts.index)
    for lag in [1, 2, 3, 7]:
        table[f"EVENTOS_lag_{lag}"] = counts["EVENTOS"].shift(lag)
    for window in [3, 7]:
        rolling = shifted.rolling(window, min_periods=window)
        table[f"EVENTOS_rolling_mean_{window}d"] = rolling.mean()
        table[f"EVENTOS_rolling_std_{window}d"] = rolling.std()
        table[f"EVENTOS_rolling_max_{window}d"] = rolling.max()
    for window in [14, 30]:
        table[f"EVENTOS_rolling_mean_{window}d"] = shifted.rolling(
            window, min_periods=window
        ).mean()
    for lag_column, source in CATEGORY_SOURCES.items():
        table[lag_column] = counts[source].shift(1)
    return table[HISTORY_COLS]


class LeadWeather:
    """Daily weather features per forecast lead, rebuilt like clean_and_augment."""

    def __init__(self, hourly, frame, feature_cols):
        self.fallback_share = {}
        daily_by_lead = []
        for lead in range(MAX_LEAD_DAYS + 1):
            lead_hourly, filled = self._lead_hourly(hourly, lead)
            self.fallback_share[lead] = filled
            daily = aggregate_weather_daily(lead_hourly)
            daily.index = pd.to_datetime(daily.index)
            daily_by_lead.append(daily)
        self.dates = daily_by_lead[0].index
        self.daily_cols = [column for column in daily_by_lead[0].columns if column in feature_cols]
        needed = list(dict.fromkeys(self.daily_cols + ["LLUVIA", "VIENTO_MEDIO", "HUM_MEDIA"]))
        self.col_pos = {column: i for i, column in enumerate(needed)}
        self.values = np.stack([daily[needed].to_numpy(dtype=float) for daily in daily_by_lead])
        self.position = {date: i for i, date in enumerate(self.dates)}
        self.days_since_rain_lead0 = self._days_since_rain(frame)

    @staticmethod
    def _lead_hourly(hourly, lead):
        frame = pd.DataFrame({"time": hourly["time"]})
        filled = {}
        suffix = "" if lead == 0 else f"_previous_day{lead}"
        for variable in OPEN_METEO_HOURLY_VARIABLES:
            column = f"{variable}{suffix}"
            values = pd.to_numeric(hourly.get(column), errors="coerce") if column in hourly else (
                pd.Series(np.nan, index=hourly.index)
            )
            if variable == "rain":
                # rain_previous_dayN starts mid-2024; precipitation minus showers is equivalent.
                derived = (
                    pd.to_numeric(hourly[f"precipitation{suffix}"], errors="coerce")
                    - pd.to_numeric(hourly[f"showers{suffix}"], errors="coerce")
                ).clip(lower=0)
                values = values.fillna(derived)
            # Variables without lagged runs (visibility, soil moisture, freezing level)
            # fall back to the newest run: optimistic only for those features.
            filled[variable] = float(values.isna().mean())
            frame[variable] = values.fillna(pd.to_numeric(hourly[variable], errors="coerce"))
        return frame, filled

    def _days_since_rain(self, frame):
        """Training definition on the lead-0 series, seeded from the dataset."""
        rain = self.values[0][:, self.col_pos["LLUVIA"]]
        dataset = pd.to_numeric(
            frame.set_index(pd.to_datetime(frame["FECHA_DIA"]))["DIAS_DESDE_ULTIMA_LLUVIA"],
            errors="coerce",
        )
        seed = dataset[dataset.index <= self.dates[0]]
        count = float(seed.iloc[-1]) if not seed.empty else 0.0
        values = np.empty(len(rain))
        values[0] = count
        for i in range(1, len(rain)):
            count = 0.0 if rain[i - 1] > 0.1 else count + 1.0
            values[i] = count
        return values

    def covers(self, origin, target):
        return (
            target - pd.Timedelta(days=30) in self.position
            and target in self.position and origin in self.position
        )

    def _lead(self, origin, day, extra):
        if extra == "lead0" or day < origin:
            return 0
        return min((day - origin).days + extra, MAX_LEAD_DAYS)

    def features(self, origin, target, extra):
        end = self.position[target]
        days = [target - pd.Timedelta(days=offset) for offset in range(30, -1, -1)]
        leads = np.array([self._lead(origin, day, extra) for day in days])
        window = self.values[leads, np.arange(end - 30, end + 1)]
        column = lambda name: window[:, self.col_pos[name]]

        result = {name: float(column(name)[-1]) for name in self.daily_cols}
        rain = column("LLUVIA")
        previous = rain[:-1][::-1]  # previous[0] is the day before the target
        result["VIENTO_MEDIO_lag_1"] = float(column("VIENTO_MEDIO")[-2])
        result["HUM_MEDIA_lag_1"] = float(column("HUM_MEDIA")[-2])
        for lag in RAIN_LAGS:
            result[f"LLUVIA_LAG_{lag}D"] = float(previous[lag - 1])
        for window_days in RAIN_WINDOWS:
            recent = previous[:window_days]
            result[f"LLUVIA_PROMEDIO_{window_days}D_PREV"] = float(np.mean(recent))
            result[f"LLUVIA_TOTAL_{window_days}D_PREV"] = float(np.sum(recent))
            result[f"LLUVIA_DESV_{window_days}D_PREV"] = float(np.std(recent, ddof=1))
            result[f"LLUVIA_MAX_{window_days}D_PREV"] = float(np.max(recent))
            result[f"DIAS_SECOS_{window_days}D_PREV"] = float(np.sum(recent <= 0.1))

        count = self.days_since_rain_lead0[self.position[min(origin, target)]]
        for offset in range(max((target - origin).days, 0)):
            day = origin + pd.Timedelta(days=offset)
            lead = self._lead(origin, day, extra)
            day_rain = self.values[lead, self.position[day], self.col_pos["LLUVIA"]]
            count = 0.0 if day_rain > 0.1 else count + 1.0
        result["DIAS_DESDE_ULTIMA_LLUVIA"] = float(count)

        temp_max, temp_media = result["TEMP_MAX"], result["TEMP_MEDIA"]
        hum_media, hum_min = result["HUM_MEDIA"], result["HUM_MIN"]
        result["TEMP_HUM_INDEX"] = temp_media * hum_media / 100
        result["VIENTO_LLUVIA_INDEX"] = result["VIENTO_MEDIO"] * result["LLUVIA"]
        result["STORM_COMPOUND_INDEX"] = result["VIENTO_MAX"] * (1 + result["LLUVIA"])
        result["FIRE_DRY_INDEX_7D"] = (
            temp_max * result["DIAS_SECOS_7D_PREV"] / (1 + result["LLUVIA_TOTAL_7D_PREV"])
        )
        es_media = 0.6108 * np.exp((17.27 * temp_media) / (temp_media + 237.3))
        result["VPD"] = float(es_media * (1 - hum_media / 100))
        es_max = 0.6108 * np.exp((17.27 * temp_max) / (temp_max + 237.3))
        result["VPD_MAX"] = float(es_max * (1 - hum_min / 100))
        return result


def make_row_builder(rows_by_date, feature_cols, lead_weather, extra):
    """Feature row for `target` as seen by a forecast issued on `origin`."""
    if extra is None:
        return lambda origin, target: rows_by_date.loc[target, feature_cols].copy()

    def build(origin, target):
        row = rows_by_date.loc[target, feature_cols].copy()
        for column, value in lead_weather.features(origin, target, extra).items():
            if column in row.index:
                row[column] = value
        return row
    return build


def recursive_forecasts(regressor, row_for, available, counts, frame, origins, feature_cols):
    """Replay dashboard.predict_6_days: predictions feed back as event lags."""
    frame_dates = pd.to_datetime(frame["FECHA_DIA"]).to_numpy()
    cumulative = frame[["EVENTOS", *CATEGORY_SOURCES.values()]].astype(float).cumsum()
    states = []
    for origin in origins:
        window = counts.loc[origin - pd.Timedelta(days=30): origin - pd.Timedelta(days=1)]
        if len(window) < 30:
            continue
        # Shares over the last 365 observed rows before the origin, as the dashboard does.
        end = int(np.searchsorted(frame_dates, np.datetime64(origin))) - 1
        start = max(end - 365, -1)
        if end < 0:
            continue
        totals = cumulative.iloc[end] - (cumulative.iloc[start] if start >= 0 else 0.0)
        shares = {
            lag: float(totals[source] / totals["EVENTOS"]) if totals["EVENTOS"] > 0 else 0.0
            for lag, source in CATEGORY_SOURCES.items()
        }
        states.append({
            "origin": origin,
            "history": window["EVENTOS"].tolist(),
            "category_lags": {lag: float(window[source].iloc[-1]) for lag, source in CATEGORY_SOURCES.items()},
            "shares": shares,
            "alive": True,
        })

    records = []
    for step in range(len(HORIZONS)):
        batch = []
        for state in states:
            target = state["origin"] + pd.Timedelta(days=step)
            if not state["alive"] or not available(state["origin"], target):
                # Without that day's weather the chain cannot continue.
                state["alive"] = False
                continue
            row = row_for(state["origin"], target)
            for column, value in event_history_features(state["history"]).items():
                row[column] = value
            for column, value in state["category_lags"].items():
                row[column] = value
            batch.append((state, target, row))
        if not batch:
            break
        X = pd.DataFrame([row for _, _, row in batch])[feature_cols]
        predictions = np.clip(regressor.predict(X), 0, None)
        for (state, target, _), prediction in zip(batch, predictions):
            records.append({
                "origin": state["origin"], "FECHA": target,
                "horizon": step + 1, "prediction": float(prediction),
            })
            state["history"] = state["history"][1:] + [float(prediction)]
            state["category_lags"] = {
                lag: float(prediction * share) for lag, share in state["shares"].items()
            }
    return pd.DataFrame(records, columns=["origin", "FECHA", "horizon", "prediction"])


def climatology(train_rows, targets):
    """Month x weekday mean blended with weekday mean (serving_history policy)."""
    dates = pd.to_datetime(train_rows["FECHA_DIA"])
    events = train_rows["EVENTOS"].astype(float)
    by_weekday = events.groupby(dates.dt.dayofweek).mean()
    grouped = events.groupby([dates.dt.month, dates.dt.dayofweek])
    by_month_weekday, sizes = grouped.mean(), grouped.size()
    values = []
    for target in targets:
        key = (target.month, target.weekday())
        weekday = by_weekday.get(target.weekday(), events.mean())
        if sizes.get(key, 0) >= 8:
            values.append(0.75 * by_month_weekday[key] + 0.25 * weekday)
        else:
            values.append(weekday)
    return np.asarray(values, dtype=float)


def horizon_metrics(y, prediction):
    y = np.asarray(y, dtype=float)
    prediction = np.asarray(prediction, dtype=float)
    error = prediction - y
    return {
        "n": int(len(y)),
        "mae": float(np.mean(np.abs(error))),
        "rmse": float(np.sqrt(np.mean(error ** 2))),
        "r2": float(1 - np.sum(error ** 2) / np.sum((y - y.mean()) ** 2)),
        "high_auc": safe_auc(y, prediction),
        "bias": float(np.mean(error)),
        "std_ratio": float(np.std(prediction, ddof=1) / np.std(y, ddof=1)),
    }


def lead0_parity(lead_weather, frame, feature_cols, test_dates):
    """Compare rebuilt lead-0 weather features with the training dataset."""
    weather_cols = [
        column for column in feature_cols
        if column not in HISTORY_COLS and column not in CALENDAR_COLS
    ]
    rows_by_date = frame.set_index(pd.to_datetime(frame["FECHA_DIA"]))
    rebuilt = pd.DataFrame(
        [lead_weather.features(date, date, "lead0") for date in test_dates], index=test_dates
    )
    missing = [column for column in weather_cols if column not in rebuilt.columns]
    if missing:
        raise KeyError(f"Weather features not rebuilt from Previous Runs: {missing}")
    rows = []
    for column in weather_cols:
        original = rows_by_date.loc[test_dates, column].astype(float)
        rows.append({
            "feature": column,
            "corr": float(np.corrcoef(original, rebuilt[column])[0, 1])
            if original.std() > 0 and rebuilt[column].std() > 0 else np.nan,
            "mean_abs_diff": float(np.mean(np.abs(original - rebuilt[column]))),
            "dataset_std": float(original.std()),
        })
    return pd.DataFrame(rows).sort_values("corr")


def main():
    root = PROJECT_ROOT
    output = root / "05_research" / "results" / "forecast_horizons"
    output.mkdir(parents=True, exist_ok=True)

    frame, base_features, hydro_features = load_training_frame(root)
    ensemble_features = list(dict.fromkeys(base_features + hydro_features))
    missing_history = [column for column in HISTORY_COLS if column not in ensemble_features]
    if missing_history:
        raise KeyError(f"History columns not used by the active model: {missing_history}")
    y = frame["EVENTOS"].astype(float)
    frame_dates = pd.to_datetime(frame["FECHA_DIA"])
    rows_by_date = frame.set_index(frame_dates)
    dataset_dates = set(frame_dates)

    counts, observed = calendar_counts(frame)
    history_table = origin_history_table(counts)
    # Reconstruction check: recomputed history must match the training dataset.
    recomputed = history_table.reindex(frame_dates)
    comparable = recomputed.notna().all(axis=1).to_numpy()
    reconstruction_error = float(np.nanmax(np.abs(
        recomputed.to_numpy()[comparable] - frame[HISTORY_COLS].to_numpy(dtype=float)[comparable]
    )))
    print(f"History reconstruction max abs error: {reconstruction_error:.6f}", flush=True)
    observed_mean_30d = observed.shift(1).rolling(30, min_periods=10).mean()

    scenarios = dict(SCENARIOS)
    lead_weather = None
    if PREVIOUS_RUNS_PATH.exists():
        hourly = pd.read_csv(PREVIOUS_RUNS_PATH, sep=";")
        lead_weather = LeadWeather(hourly, frame, ensemble_features)
    else:
        print(f"{PREVIOUS_RUNS_PATH} not found: only historical_forecast is evaluated")
        scenarios = {"historical_forecast": None}

    def available(origin, target):
        if target not in dataset_dates:
            return False
        return lead_weather is None or lead_weather.covers(origin, target)

    builders = {
        name: make_row_builder(rows_by_date, ensemble_features, lead_weather, extra)
        for name, extra in scenarios.items()
    }

    splits = list(TimeSeriesSplit(n_splits=N_SPLITS, test_size=TEST_SIZE).split(frame))
    parity = None
    if lead_weather is not None:
        all_test = frame_dates.iloc[np.concatenate([test for _, test in splits])]
        parity = lead0_parity(lead_weather, frame, ensemble_features, all_test)
        parity.to_csv(output / "previous_runs_lead0_parity.csv", sep=";", index=False)
        print("Lead-0 parity, lowest correlations:")
        print(parity.head(8).to_string(index=False, float_format="%.3f"))

    pieces = []
    for fold, (train_idx, test_idx) in enumerate(splits, start=1):
        train_rows = frame.iloc[train_idx]
        test_dates = frame_dates.iloc[test_idx]
        print(f"Fold {fold}: {test_dates.min():%Y-%m-%d} .. {test_dates.max():%Y-%m-%d}", flush=True)

        active = fit_ensemble(train_rows, y.iloc[train_idx], base_features, ensemble_features)
        direct_models = {}
        for horizon in HORIZONS:
            # Direct model: history as known at the origin, weather/calendar of the target.
            shift = pd.Timedelta(days=horizon - 1)
            train_history = history_table.reindex(frame_dates.iloc[train_idx] - shift).to_numpy()
            train_X = train_rows[ensemble_features].copy()
            train_X[HISTORY_COLS] = train_history
            usable = ~np.isnan(train_history).any(axis=1)
            direct_models[horizon] = fit_ensemble(
                train_X[usable], y.iloc[train_idx][usable], base_features, ensemble_features
            )

        origins = pd.date_range(test_dates.min() - pd.Timedelta(days=5), test_dates.max(), freq="D")
        test_frame = pd.DataFrame({"FECHA": test_dates.to_numpy(), "EVENTOS": y.iloc[test_idx].to_numpy()})
        clim = dict(zip(test_frame["FECHA"], climatology(train_rows, test_frame["FECHA"])))
        for scenario, row_for in builders.items():
            recursive = recursive_forecasts(
                active, row_for, available, counts, frame, origins, ensemble_features
            )
            for horizon in HORIZONS:
                shift = pd.Timedelta(days=horizon - 1)
                piece = test_frame.copy()
                piece["weather"] = scenario
                piece["fold"] = fold
                piece["horizon"] = horizon
                piece["origin"] = piece["FECHA"] - shift
                usable = np.array([
                    available(origin, target)
                    for origin, target in zip(piece["origin"], piece["FECHA"])
                ])
                piece = piece[usable].reset_index(drop=True)
                test_X = pd.DataFrame([
                    row_for(origin, target)
                    for origin, target in zip(piece["origin"], piece["FECHA"])
                ])[ensemble_features]
                test_X[HISTORY_COLS] = history_table.reindex(piece["origin"]).to_numpy()
                piece["directo_por_horizonte"] = np.clip(direct_models[horizon].predict(test_X), 0, None)
                piece["promedio_30d"] = observed_mean_30d.reindex(piece["origin"]).to_numpy()
                piece["estacional_mes_dia"] = piece["FECHA"].map(clim).to_numpy()
                rec = recursive[recursive["horizon"] == horizon].set_index("FECHA")["prediction"]
                piece["recursivo_activo"] = piece["FECHA"].map(rec).to_numpy()
                pieces.append(piece)

    predictions = pd.concat(pieces, ignore_index=True).dropna(subset=METHODS)
    # Compare every method and scenario on exactly the same (date, horizon) pairs.
    pair_counts = predictions.groupby(["FECHA", "horizon"])["weather"].transform("nunique")
    common = predictions[pair_counts == len(scenarios)].copy()

    rows = []
    for (scenario, horizon), group in common.groupby(["weather", "horizon"], sort=False):
        for method in METHODS:
            rows.append({
                "weather": scenario, "horizon": horizon, "method": method,
                **horizon_metrics(group["EVENTOS"], group[method]),
            })
    leaderboard = pd.DataFrame(rows)

    fold_rows = []
    for (scenario, horizon, fold), group in common.groupby(["weather", "horizon", "fold"], sort=False):
        for method in METHODS:
            fold_rows.append({
                "weather": scenario, "horizon": horizon, "fold": fold, "method": method,
                "mae": float(np.mean(np.abs(group[method] - group["EVENTOS"]))),
            })
    fold_metrics = pd.DataFrame(fold_rows)

    bootstrap = {}
    for (scenario, horizon), group in common.groupby(["weather", "horizon"], sort=False):
        group = group.sort_values("FECHA")
        y_h = group["EVENTOS"].to_numpy()
        reference = group["promedio_30d"].to_numpy()
        bootstrap.setdefault(scenario, {})[int(horizon)] = {
            method: block_bootstrap_improvement(
                y_h, reference, group[method].to_numpy(), block_size=30, repeats=5000
            )
            for method in ["recursivo_activo", "directo_por_horizonte", "estacional_mes_dia"]
        }

    summary = {
        "protocol": (
            f"TimeSeriesSplit {N_SPLITS}x{TEST_SIZE} dias, mismos folds que el modelo activo; "
            "ensemble activo reentrenado por fold; directos reentrenados por fold y horizonte; "
            "modelos siempre entrenados con el clima del dataset"
        ),
        "horizon_definition": "h=1 es el dia de emision (lags hasta ayer); h=6 es cinco dias despues",
        "weather_scenarios": {
            "historical_forecast": "Clima del dataset (Historical Forecast API, lead corto)",
            "previous_runs_lead0": "Previous Runs API, corrida mas reciente para todos los dias",
            "previous_runs_lead": "Dia k de un forecast emitido en o usa la corrida de k-o dias antes",
            "previous_runs_lead_conservador": "Igual que previous_runs_lead con un dia extra de anticipacion",
        },
        "lagged_run_gaps": (
            "visibility, soil_moisture_0_to_1cm y freezing_level_height no tienen corridas previas: "
            "usan la corrida mas reciente (optimista solo en esas variables); rain previo a mediados "
            "de 2024 se deriva como precipitation - showers de la misma corrida"
        ),
        "history_reconstruction_max_abs_error": reconstruction_error,
        "high_activity_threshold": HIGH_ACTIVITY_THRESHOLD,
        "evaluated_pairs_per_horizon": (
            common[common["weather"] == "historical_forecast"]
            .groupby("horizon").size().astype(int).to_dict()
        ),
        "lead0_parity_min_corr": None if parity is None else float(parity["corr"].min()),
        "metrics": leaderboard.to_dict(orient="records"),
        "mae_improvement_vs_promedio_30d": bootstrap,
    }
    leaderboard.to_csv(output / "horizon_metrics.csv", sep=";", index=False)
    fold_metrics.to_csv(output / "fold_metrics.csv", sep=";", index=False)
    common.to_csv(output / "predictions.csv", sep=";", index=False)
    (output / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )

    for metric in ["mae", "high_auc"]:
        print(f"\n{metric} por horizonte")
        table = leaderboard.pivot_table(
            index="horizon", columns=["weather", "method"], values=metric
        )
        print(table.T.to_string(float_format="%.3f"))
    print("\nP(mejora MAE vs promedio_30d)")
    print(pd.DataFrame({
        (scenario, horizon): {method: values["mae_probability_improves"] for method, values in methods.items()}
        for scenario, by_horizon in bootstrap.items() for horizon, methods in by_horizon.items()
    }).to_string(float_format="%.3f"))


if __name__ == "__main__":
    main()
