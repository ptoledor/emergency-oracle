import numpy as np


class ConstantRegressor:
    def __init__(self, value, feature_cols=None, source="constant"):
        self.value = float(value)
        self.feature_cols = list(feature_cols or [])
        self.source = source

    def predict(self, X):
        return np.full(len(X), self.value, dtype=float)
