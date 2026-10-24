"""Model classes loaded by the forecast.

- CountModel / HorizonProbabilityClassifier: the active count model, stored as
  JSON by 03_model/train_model.py.
- SigmoidProbabilityCalibratedClassifier: referenced by the pickled category
  risk models (03_model/train_category_risk_models.py).
"""

import numpy as np


class SigmoidProbabilityCalibratedClassifier:
    """Apply a temporal Platt calibrator to a classifier's positive probability."""

    def __init__(self, base_classifier, coefficient, intercept, feature_cols):
        self.base_classifier = base_classifier
        self.coefficient = float(coefficient)
        self.intercept = float(intercept)
        self.feature_cols = list(feature_cols)
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)
        self.feature_importances_ = getattr(base_classifier, "feature_importances_", None)
        self.classes_ = np.asarray([0, 1])

    def predict_proba(self, X):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing calibrated classifier features: {missing[:10]}")
        raw_probability = np.asarray(
            self.base_classifier.predict_proba(X[self.feature_cols])[:, 1],
            dtype=float,
        )
        logit = np.clip(
            self.intercept + self.coefficient * raw_probability,
            -35,
            35,
        )
        calibrated = 1.0 / (1.0 + np.exp(-logit))
        return np.column_stack([1.0 - calibrated, calibrated])


class CountModel:
    """Daily count model stored as JSON: XGBoost boosters and/or a Poisson GLM.

    Components are blended with fixed weights and an optional spread transform
    around `center`. Saved by 03_model/train_model.py, loaded with `load`.
    """

    def __init__(self, metadata, boosters=None):
        self.metadata = metadata
        self.boosters = dict(boosters or {})
        self.feature_cols = list(metadata["feature_cols"])
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)
        importances = metadata.get("feature_importances", {})
        self.feature_importances_ = np.asarray(
            [float(importances.get(column, 0.0)) for column in self.feature_cols]
        )

    @classmethod
    def load(cls, model_dir):
        import json
        from pathlib import Path

        import xgboost as xgb

        model_dir = Path(model_dir)
        metadata = json.loads((model_dir / "metadata.json").read_text(encoding="utf-8"))
        boosters = {}
        for component in metadata["components"]:
            if component["kind"] == "xgboost":
                booster = xgb.Booster()
                booster.load_model(model_dir / component["file"])
                booster.set_param({"nthread": 1})
                boosters[component["name"]] = booster
        return cls(metadata, boosters)

    def _component_predict(self, component, X):
        columns = component["feature_cols"]
        if component["kind"] == "xgboost":
            import xgboost as xgb

            matrix = xgb.DMatrix(X[columns].astype(float), missing=np.nan)
            return self.boosters[component["name"]].predict(matrix)
        values = X[columns].to_numpy(dtype=float)
        for position, column in enumerate(columns):
            if column in component["log1p_cols"]:
                values[:, position] = np.log1p(np.clip(values[:, position], 0, None))
        scaled = (values - np.asarray(component["mean"])) / np.asarray(component["scale"])
        return np.exp(component["intercept"] + scaled @ np.asarray(component["coef"]))

    def predict(self, X):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing count model features: {missing[:10]}")
        blended = np.zeros(len(X), dtype=float)
        for component in self.metadata["components"]:
            blended += component["weight"] * np.asarray(
                self._component_predict(component, X), dtype=float
            )
        spread = self.metadata.get("spread", {"center": 0.0, "scale": 1.0, "offset": 0.0})
        calibrated = spread["center"] + spread["scale"] * (blended - spread["center"]) + spread["offset"]
        return np.clip(calibrated, 0, None)


class HorizonProbabilityClassifier:
    """P(count > threshold) per forecast day, logistic on the predicted count."""

    def __init__(self, count_model):
        self.count_model = count_model
        self.horizons = {
            int(horizon): details
            for horizon, details in count_model.metadata["horizons"].items()
        }
        self.feature_cols = count_model.feature_cols
        self.feature_names_in_ = count_model.feature_names_in_
        self.classes_ = np.asarray([0, 1])

    def predict_proba_horizon(self, X, horizon):
        details = self.horizons[min(int(horizon), max(self.horizons))]
        count = self.count_model.predict(X)
        logit = np.clip(details["prob_intercept"] + details["prob_coef"] * count, -35, 35)
        probability = 1.0 / (1.0 + np.exp(-logit))
        return np.column_stack([1.0 - probability, probability])

    def predict_proba(self, X):
        return self.predict_proba_horizon(X, 1)
