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

