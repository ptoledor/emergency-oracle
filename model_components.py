import numpy as np


class CategoryBlendRegressor:
    def __init__(self, direct_model, category_models, weight_direct, feature_cols):
        self.direct_model = direct_model
        self.category_models = category_models
        self.weight_direct = float(weight_direct)
        self.feature_cols = list(feature_cols)
        self.feature_importances_ = direct_model.feature_importances_

    def predict(self, X):
        direct_prediction = np.clip(
            self.direct_model.predict(X[self.feature_cols]),
            0,
            None,
        )
        category_prediction = np.zeros(len(X), dtype=float)
        for details in self.category_models.values():
            category_prediction += np.clip(
                details["model"].predict(X[details["feature_cols"]]),
                0,
                None,
            )
        return (
            self.weight_direct * direct_prediction
            + (1.0 - self.weight_direct) * category_prediction
        )
