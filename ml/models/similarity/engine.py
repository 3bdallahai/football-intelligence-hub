import numpy as np
import pandas as pd


class SimilarityEngine:

    def __init__(self, df, imputer, scaler, knn, feature_cols):
        self.df = df.reset_index(drop=True)
        self.imputer = imputer
        self.scaler = scaler
        self.knn = knn
        self.feature_cols = feature_cols

    def get_similar(self, player_name: str, top_n: int = 5) -> pd.DataFrame:

        mask = self.df["player_name"].str.lower() == player_name.lower()
        query_rows = self.df[mask]

        if query_rows.empty:
            raise ValueError(f"Player '{player_name}' not found.")

        query = query_rows.iloc[0]

        candidates = self.df[self.df["player_name"] != query["player_name"]].reset_index(drop=True)

        # -----------------------------
        # ROLE PENALTY (IMPORTANT FIX)
        # -----------------------------
        query_position = query["primary_position"]

        candidates["role_penalty"] = candidates["primary_position"].apply(
            lambda x: 1.0 if x == query_position else 0.75
        )

        # -----------------------------
        # FEATURE PIPELINE
        # -----------------------------
        X_query = query[self.feature_cols].values.reshape(1, -1)
        X_query = self.imputer.transform(X_query)
        X_query = self.scaler.transform(X_query)

        X_cand = candidates[self.feature_cols].values
        X_cand = self.imputer.transform(X_cand)
        X_cand = self.scaler.transform(X_cand)

        # -----------------------------
        # COSINE SIMILARITY (CORRECT APPROACH)
        # -----------------------------
        from sklearn.metrics.pairwise import cosine_similarity

        sims = cosine_similarity(X_query, X_cand)[0]

        # apply role penalty
        sims = sims * candidates["role_penalty"].values

        # top-k
        top_idx = np.argsort(sims)[::-1][:top_n]

        result = candidates.iloc[top_idx][
            ["player_name", "club_name", "primary_position", "market_value_eur", "age","player_key"]
        ].copy()

        result["similarity_score"] = sims[top_idx]

        return result.reset_index(drop=True)