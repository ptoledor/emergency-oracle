import numpy as np


class CategoryBlendRegressor:
    def __init__(self, direct_model, category_models, weight_direct, feature_cols):
        self.direct_model = direct_model
        self.category_models = category_models
        self.weight_direct = float(weight_direct)
        self.feature_cols = list(feature_cols)
        self.feature_importances_ = self._compute_blend_importances()

    def _compute_blend_importances(self):
        direct_imp = getattr(self.direct_model, "feature_importances_", None)
        if direct_imp is None:
            return None
        direct_imp = np.asarray(direct_imp, dtype=float)
        direct_cols = list(getattr(self.direct_model, "feature_names_in_", self.feature_cols))
        weight_cat = 1.0 - self.weight_direct

        blended = {col: self.weight_direct * imp for col, imp in zip(direct_cols, direct_imp)}

        for details in self.category_models.values():
            cat_model = details["model"]
            cat_imp = getattr(cat_model, "feature_importances_", None)
            if cat_imp is None:
                continue
            cat_cols = list(getattr(cat_model, "feature_names_in_", details.get("feature_cols", self.feature_cols)))
            for col, imp in zip(cat_cols, cat_imp):
                blended[col] = blended.get(col, 0.0) + weight_cat * float(imp)

        return np.array([blended.get(col, 0.0) for col in self.feature_cols], dtype=float)

    def predict(self, X):
        missing_direct = [col for col in self.feature_cols if col not in X.columns]
        if missing_direct:
            raise KeyError(
                "Missing direct features for CategoryBlendRegressor: "
                f"{missing_direct[:10]}"
            )
        direct_prediction = np.clip(
            self.direct_model.predict(X[self.feature_cols]),
            0,
            None,
        )
        category_prediction = np.zeros(len(X), dtype=float)
        for category_name, details in self.category_models.items():
            cat_cols = list(details["feature_cols"])
            missing_category = [col for col in cat_cols if col not in X.columns]
            if missing_category:
                raise KeyError(
                    f"Missing category features for {category_name}: "
                    f"{missing_category[:10]}"
                )
            category_prediction += np.clip(
                details["model"].predict(X[cat_cols]),
                0,
                None,
            )
        return (
            self.weight_direct * direct_prediction
            + (1.0 - self.weight_direct) * category_prediction
        )


class WalkForwardRegimeClassifier:
    """Horizon-specific high-activity classifiers used by direct forecasts."""

    def __init__(self, horizon_models, feature_cols):
        self.horizon_models = dict(horizon_models)
        self.feature_cols = list(feature_cols)
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)

    def _model(self, horizon):
        horizon = int(horizon)
        if horizon not in self.horizon_models:
            raise KeyError(f"No classifier available for forecast horizon {horizon}")
        return self.horizon_models[horizon]

    def predict_proba_horizon(self, X, horizon):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing classifier features: {missing[:10]}")
        return self._model(horizon).predict_proba(X[self.feature_cols])

    def predict_proba(self, X):
        return self.predict_proba_horizon(X, 1)


class WalkForwardRegimeRegressor:
    """Mixture of normal/high count regressors selected by activity probability."""

    def __init__(self, horizon_models, feature_cols):
        self.horizon_models = dict(horizon_models)
        self.feature_cols = list(feature_cols)
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)
        self.feature_importances_ = self._aggregate_importances()

    def _models(self, horizon):
        horizon = int(horizon)
        if horizon not in self.horizon_models:
            raise KeyError(f"No regressor available for forecast horizon {horizon}")
        return self.horizon_models[horizon]

    def _aggregate_importances(self):
        importances = []
        for details in self.horizon_models.values():
            normal = getattr(details["normal"], "feature_importances_", None)
            high = getattr(details["high"], "feature_importances_", None)
            if normal is not None and high is not None:
                importances.append((np.asarray(normal) + np.asarray(high)) / 2.0)
        return np.mean(importances, axis=0) if importances else None

    def predict_horizon(self, X, horizon):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing regressor features: {missing[:10]}")
        details = self._models(horizon)
        aligned = X[self.feature_cols]
        probability = details["classifier"].predict_proba(aligned)[:, 1]
        normal = np.clip(details["normal"].predict(aligned), 0, None)
        high = np.clip(details["high"].predict(aligned), 0, None)
        return (1.0 - probability) * normal + probability * high

    def predict(self, X):
        return self.predict_horizon(X, 1)


class IntegerRoundedRegressor:
    """Operational candidate that commits expected counts to nearest integers."""

    def __init__(self, base_model):
        self.base_model = base_model
        self.feature_importances_ = getattr(base_model, "feature_importances_", None)
        self.feature_names_in_ = getattr(base_model, "feature_names_in_", None)

    def predict(self, X):
        expected = np.clip(np.asarray(self.base_model.predict(X), dtype=float), 0, None)
        return np.floor(expected + 0.5).astype(int)


class HydroObjectiveEnsembleRegressor:
    """Blend complementary XGBoost objectives and restore useful spread."""

    def __init__(
        self,
        models,
        model_feature_cols,
        weights,
        feature_cols,
        center=5.0,
        spread_scale=1.0,
        offset=0.0,
    ):
        self.models = dict(models)
        self.model_feature_cols = {
            name: list(columns) for name, columns in model_feature_cols.items()
        }
        self.weights = {name: float(value) for name, value in weights.items()}
        self.feature_cols = list(feature_cols)
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)
        self.center = float(center)
        self.spread_scale = float(spread_scale)
        self.offset = float(offset)
        self.feature_importances_ = self._aggregate_importances()

    def _aggregate_importances(self):
        combined = {column: 0.0 for column in self.feature_cols}
        for name, model in self.models.items():
            importances = getattr(model, "feature_importances_", None)
            if importances is None:
                continue
            for column, value in zip(self.model_feature_cols[name], importances):
                combined[column] = combined.get(column, 0.0) + (
                    self.weights[name] * float(value)
                )
        return np.asarray([combined.get(column, 0.0) for column in self.feature_cols])

    def predict(self, X):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing hydro ensemble features: {missing[:10]}")
        blended = np.zeros(len(X), dtype=float)
        for name, model in self.models.items():
            columns = self.model_feature_cols[name]
            blended += self.weights[name] * np.asarray(
                model.predict(X[columns]), dtype=float
            )
        calibrated = (
            self.center
            + self.spread_scale * (blended - self.center)
            + self.offset
        )
        return np.clip(calibrated, 0, None)


class RegressorProbabilityClassifier:
    """Calibrated high-activity probability derived from a count regressor."""

    def __init__(self, regressor, coefficient, intercept, feature_cols):
        self.regressor = regressor
        self.coefficient = float(coefficient)
        self.intercept = float(intercept)
        self.feature_cols = list(feature_cols)
        self.feature_names_in_ = np.asarray(self.feature_cols, dtype=object)

    def predict_proba(self, X):
        missing = [column for column in self.feature_cols if column not in X.columns]
        if missing:
            raise KeyError(f"Missing calibrated classifier features: {missing[:10]}")
        count = np.clip(
            np.asarray(self.regressor.predict(X[self.feature_cols]), dtype=float),
            0,
            None,
        )
        logit = np.clip(self.intercept + self.coefficient * count, -35, 35)
        high_probability = 1.0 / (1.0 + np.exp(-logit))
        return np.column_stack([1.0 - high_probability, high_probability])


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


def resolve_model_path(models_dir, prefix):
    import json
    from pathlib import Path
    
    models_dir = Path(models_dir)
    config_path = models_dir / "active_models.json"
    key = prefix.lstrip("_")
    
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config = json.load(f)
            if key in config:
                suffix = config[key]
                return (
                    models_dir / f"regressor_{suffix}.pkl",
                    models_dir / f"classifier_{suffix}.pkl",
                    models_dir / f"metadata_{suffix}.pkl"
                )
        except Exception:
            pass
            
    return (
        models_dir / f"regressor{prefix}.pkl",
        models_dir / f"classifier{prefix}.pkl",
        models_dir / f"metadata{prefix}.pkl"
    )

