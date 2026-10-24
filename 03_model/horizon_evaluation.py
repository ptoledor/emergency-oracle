"""Walk-forward evaluation of the six-day recursive forecast.

The dashboard issues six daily forecasts from one origin: day 1 uses the counts
observed up to yesterday, and each later day feeds the previous prediction back
as event history (category lags split with historical shares). This module
replays that recursion on walk-forward folds for any candidate count model.

Weather scenarios for the evaluated days (models are trained on dataset weather,
as in production):

- historical_forecast: dataset weather (Historical Forecast API, short lead).
- previous_runs_lead: day k of a forecast issued on origin o uses the Open-Meteo
  run issued k-o days before, as the dashboard would see it; days before o use
  the newest run. Needs 05_research/data/previous_runs_hourly.csv
  (05_research/fetch_previous_runs_weather.py).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from features import (
    CALENDAR_COLUMNS,
    CATEGORY_SOURCES,
    EVENT_HISTORY_COLUMNS,
    OPEN_METEO_HOURLY_VARIABLES,
    WEEKDAY_COLUMNS,
    aggregate_weather_daily,
    days_since_rain,
    event_history_features,
    event_history_table,
    weather_derived_features,
)
from forecasting import PROJECT_ROOT


HORIZONS = list(range(1, 7))
HIGH_ACTIVITY_THRESHOLD = 7.0
PREVIOUS_RUNS_PATH = PROJECT_ROOT / "05_research" / "data" / "previous_runs_hourly.csv"
MAX_LEAD_DAYS = 6
DERIVED_INPUTS = ["LLUVIA", "VIENTO_MEDIO", "VIENTO_MAX", "HUM_MEDIA", "HUM_MIN", "TEMP_MEDIA", "TEMP_MAX"]
# None: dataset weather; "lead0": newest run everywhere; int: extra lead days.
SCENARIOS = {"historical_forecast": None, "previous_runs_lead": 0}


def safe_auc(y, score, threshold=HIGH_ACTIVITY_THRESHOLD):
    target = np.asarray(y, dtype=float) > threshold
    return float(roc_auc_score(target, score)) if 0 < target.sum() < len(target) else float("nan")


def block_bootstrap_improvement(y, reference, candidate, block_size=30, repeats=5000):
    """Mean error improvement of `candidate` over `reference`, resampling time blocks."""
    y = np.asarray(y, dtype=float)
    mae_delta = np.abs(y - reference) - np.abs(y - candidate)
    rng = np.random.default_rng(42)
    starts = np.arange(len(y) - block_size + 1)
    blocks = int(np.ceil(len(y) / block_size))
    samples = np.empty(repeats)
    for repeat in range(repeats):
        chosen = rng.choice(starts, size=blocks)
        indices = (chosen[:, None] + np.arange(block_size)).ravel()[: len(y)]
        samples[repeat] = mae_delta[indices].mean()
    return {
        "mae_improvement_mean": float(samples.mean()),
        "mae_improvement_ci_low": float(np.quantile(samples, 0.025)),
        "mae_improvement_ci_high": float(np.quantile(samples, 0.975)),
        "mae_probability_improves": float(np.mean(samples > 0)),
    }


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


def calendar_counts(frame):
    """Daily counts on a full calendar; days without coverage stay missing."""
    dates = pd.to_datetime(frame["FECHA_DIA"])
    index = pd.date_range(dates.min(), dates.max(), freq="D")
    columns = ["EVENTOS", *CATEGORY_SOURCES.values()]
    return frame.set_index(dates)[columns].astype(float).reindex(index)


def origin_history_table(counts):
    """History features as known on each origin date (counts up to the day before)."""
    return event_history_table(counts)


def climatology(train_rows, targets):
    """Month x weekday mean blended with the weekday mean."""
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


class LeadWeather:
    """Daily weather features per forecast lead, rebuilt with features.py."""

    def __init__(self, hourly, frame, feature_cols):
        daily_by_lead = []
        for lead in range(MAX_LEAD_DAYS + 1):
            daily = aggregate_weather_daily(self._lead_hourly(hourly, lead))
            daily.index = pd.to_datetime(daily.index)
            daily_by_lead.append(daily)
        self.dates = daily_by_lead[0].index
        self.daily_cols = [column for column in daily_by_lead[0].columns if column in feature_cols]
        self.columns = list(dict.fromkeys(self.daily_cols + DERIVED_INPUTS))
        self.values = np.stack([daily[self.columns].to_numpy(dtype=float) for daily in daily_by_lead])
        self.position = {date: i for i, date in enumerate(self.dates)}
        self.days_since_rain_lead0 = self._days_since_rain(frame)
        self._cache = {}

    @staticmethod
    def _lead_hourly(hourly, lead):
        frame = pd.DataFrame({"time": hourly["time"]})
        suffix = "" if lead == 0 else f"_previous_day{lead}"
        for variable in OPEN_METEO_HOURLY_VARIABLES:
            column = f"{variable}{suffix}"
            values = (
                pd.to_numeric(hourly[column], errors="coerce") if column in hourly
                else pd.Series(np.nan, index=hourly.index)
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
            frame[variable] = values.fillna(pd.to_numeric(hourly[variable], errors="coerce"))
        return frame

    def _days_since_rain(self, frame):
        """Training definition on the lead-0 series, seeded from the dataset."""
        dataset = pd.to_numeric(
            frame.set_index(pd.to_datetime(frame["FECHA_DIA"]))["DIAS_DESDE_ULTIMA_LLUVIA"],
            errors="coerce",
        )
        seed = dataset[dataset.index <= self.dates[0]]
        return days_since_rain(
            self.values[0][:, self.columns.index("LLUVIA")],
            start_value=float(seed.iloc[-1]) if not seed.empty else np.nan,
        )

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
        key = (origin, target, extra)
        if key in self._cache:
            return self._cache[key]
        end = self.position[target]
        days = pd.date_range(target - pd.Timedelta(days=30), target, freq="D")
        leads = np.array([self._lead(origin, day, extra) for day in days])
        window = pd.DataFrame(
            self.values[leads, np.arange(end - 30, end + 1)], index=days, columns=self.columns
        )
        # Days before the origin use the newest run, as in the lead-0 series.
        anchor = origin - pd.Timedelta(days=1)
        seed = (days.get_loc(anchor), self.days_since_rain_lead0[self.position[anchor]])
        derived = weather_derived_features(window, days_since_rain_start=seed)
        result = {name: float(window[name].iloc[-1]) for name in self.daily_cols}
        result.update({name: float(value) for name, value in derived.iloc[-1].items()})
        self._cache[key] = result
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
    """Replay forecasting.forecast_days: predictions feed back as event history."""
    frame_dates = pd.to_datetime(frame["FECHA_DIA"]).to_numpy()
    cumulative = frame[["EVENTOS", *CATEGORY_SOURCES.values()]].astype(float).cumsum()
    states = []
    for origin in origins:
        window = counts.loc[origin - pd.Timedelta(days=30): origin - pd.Timedelta(days=1)]
        end = int(np.searchsorted(frame_dates, np.datetime64(origin))) - 1
        if len(window) < 30 or end < 0:
            continue
        # Shares over the last 365 observed rows before the origin, as in serving.
        start = max(end - 365, -1)
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
                if column in row.index:
                    row[column] = value
            for column, value in state["category_lags"].items():
                if column in row.index:
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


class WalkForward:
    """Shared folds, scenarios and baselines for comparing count models."""

    def __init__(self, frame, feature_cols, splits, scenarios=SCENARIOS):
        self.frame = frame
        self.splits = splits
        self.feature_cols = list(feature_cols)
        self.y = frame["EVENTOS"].astype(float)
        self.dates = pd.to_datetime(frame["FECHA_DIA"])
        self.rows_by_date = frame.set_index(self.dates)
        self.dataset_dates = set(self.dates)
        self.counts = calendar_counts(frame)
        observed = self.counts["EVENTOS"]
        self.observed_mean_30d = observed.shift(1).rolling(30, min_periods=10).mean()

        self.lead_weather = None
        self.scenarios = dict(scenarios)
        if any(extra is not None for extra in self.scenarios.values()):
            if PREVIOUS_RUNS_PATH.exists():
                hourly = pd.read_csv(PREVIOUS_RUNS_PATH, sep=";")
                self.lead_weather = LeadWeather(hourly, frame, self.feature_cols)
            else:
                print(f"{PREVIOUS_RUNS_PATH} no existe: solo se evalua historical_forecast")
                self.scenarios = {
                    name: extra for name, extra in self.scenarios.items() if extra is None
                }
        self.builders = {
            name: make_row_builder(self.rows_by_date, self.feature_cols, self.lead_weather, extra)
            for name, extra in self.scenarios.items()
        }

    def available(self, origin, target):
        if target not in self.dataset_dates:
            return False
        return self.lead_weather is None or self.lead_weather.covers(origin, target)

    def baselines(self):
        """30-day observed mean and month x weekday climatology per (date, horizon)."""
        pieces = []
        for fold, (train_idx, test_idx) in enumerate(self.splits, start=1):
            test_dates = self.dates.iloc[test_idx]
            clim = dict(zip(test_dates, climatology(self.frame.iloc[train_idx], test_dates)))
            for horizon in HORIZONS:
                piece = pd.DataFrame({
                    "FECHA": test_dates.to_numpy(),
                    "EVENTOS": self.y.iloc[test_idx].to_numpy(),
                    "fold": fold, "horizon": horizon,
                })
                piece["origin"] = piece["FECHA"] - pd.Timedelta(days=horizon - 1)
                piece["promedio_30d"] = self.observed_mean_30d.reindex(piece["origin"]).to_numpy()
                piece["estacional_mes_dia"] = piece["FECHA"].map(clim).to_numpy()
                pieces.append(piece)
        return pd.concat(pieces, ignore_index=True)

    def evaluate(self, fit, feature_cols):
        """Recursive predictions of `fit(X, y)` for every fold, scenario and horizon."""
        pieces = []
        for fold, (train_idx, test_idx) in enumerate(self.splits, start=1):
            model = fit(self.frame.iloc[train_idx][feature_cols], self.y.iloc[train_idx])
            test_dates = self.dates.iloc[test_idx]
            origins = pd.date_range(test_dates.min() - pd.Timedelta(days=5), test_dates.max(), freq="D")
            for scenario, builder in self.builders.items():
                recursive = recursive_forecasts(
                    model, builder, self.available, self.counts, self.frame, origins, feature_cols
                )
                recursive = recursive[recursive["FECHA"].isin(set(test_dates))]
                recursive["weather"] = scenario
                recursive["fold"] = fold
                pieces.append(recursive)
        return pd.concat(pieces, ignore_index=True)


def lead0_parity(lead_weather, frame, feature_cols, dates):
    """Compare rebuilt lead-0 weather features with the training dataset."""
    excluded = set(EVENT_HISTORY_COLUMNS) | set(CALENDAR_COLUMNS) | set(WEEKDAY_COLUMNS)
    weather_cols = [column for column in feature_cols if column not in excluded]
    rows_by_date = frame.set_index(pd.to_datetime(frame["FECHA_DIA"]))
    rebuilt = pd.DataFrame(
        [lead_weather.features(date, date, "lead0") for date in dates], index=dates
    )
    rows = []
    for column in weather_cols:
        original = rows_by_date.loc[dates, column].astype(float)
        rows.append({
            "feature": column,
            "corr": float(np.corrcoef(original, rebuilt[column])[0, 1])
            if original.std() > 0 and rebuilt[column].std() > 0 else np.nan,
            "mean_abs_diff": float(np.mean(np.abs(original - rebuilt[column]))),
            "dataset_std": float(original.std()),
        })
    return pd.DataFrame(rows).sort_values("corr")
