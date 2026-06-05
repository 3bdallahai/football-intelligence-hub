import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn.impute import SimpleImputer


class SimilarityTrainer:

    def __init__(self, feature_cols: list[str], cfg: dict):
        self.feature_cols = feature_cols
        self.cfg = cfg

    # -----------------------------
    # POSITION DETECTION
    # -----------------------------
    def _get_position_group(self, pos: str) -> str:
        pos = str(pos).upper()

        if pos == "FW":
            return "FW"
        if pos == "MF":
            return "MF"
        if pos == "DF":
            return "DF"
        return "MF"   # fallback

    # -----------------------------
    # APPLY FEATURE WEIGHTS
    # -----------------------------
    def _apply_weights(self, df: pd.DataFrame) -> pd.DataFrame:

        weights_cfg = self.cfg["outfield_weights"]

        df = df.copy()
        df["position_group"] = df["primary_position"].apply(self._get_position_group)

        weighted_rows = []

        for _, row in df.iterrows():

            pos_group = row["position_group"]
            weights = weights_cfg[pos_group]

            new_row = row.copy()

            for col in self.feature_cols:
                if col in weights:
                    new_row[col] = new_row[col] * weights[col]

            weighted_rows.append(new_row)

        return pd.DataFrame(weighted_rows)

    # -----------------------------
    # TRAIN
    # -----------------------------
    def train(self, df: pd.DataFrame):

        # 1. keep only features
        df = df[self.feature_cols + ["primary_position"]].copy()

        # 2. apply position-aware weighting
        df_weighted = self._apply_weights(df)

        X = df_weighted[self.feature_cols].values

        # 3. impute
        imputer = SimpleImputer(strategy="median")
        X_imputed = imputer.fit_transform(X)

        # 4. scale
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X_imputed)

        # 5. train KNN
        knn = NearestNeighbors(
            metric=self.cfg["metric"],
            algorithm=self.cfg["algorithm"],
        )
        knn.fit(X_scaled)

        return imputer, scaler, knn

    # -----------------------------
    # SAVE ARTIFACTS
    # -----------------------------
    def save(self, imputer, scaler, knn, path_prefix: str):

        joblib.dump(imputer, f"{path_prefix}_imputer.joblib")
        joblib.dump(scaler, f"{path_prefix}_scaler.joblib")
        joblib.dump(knn, f"{path_prefix}_index.joblib")