import joblib
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

from ml.utils.db import get_engine
from ml.features.load_similarity_data import load_outfield_features, load_goalkeeper_features
from ml.models.similarity.engine import SimilarityEngine
from ml.models.similarity.router import ModelRouter

load_dotenv(Path(__file__).parent / ".env")


def load_engine(player_position: str) -> SimilarityEngine:

    router = ModelRouter()
    model_type = router.route(player_position)

    if model_type == "GK":
        prefix = "ml/data/gk_model"
        features = [
            "save_percentage", "goals_conceded_per_90", "clean_sheet_percentage",
            "saves", "shots_on_target_against", "wins", "draws", "losses",
            "yellow_cards", "red_cards",
        ]
        loader = load_goalkeeper_features
    else:
        prefix = "ml/data/outfield_model"
        features = [
            "goals_per_90", "assists_per_90", "shots_per_90", "interceptions",
            "tackles_won", "yellow_cards", "red_cards", "points_per_match",
            "goal_difference_per_90", "on_off_difference", 
        ]
        loader = load_outfield_features

    engine = get_engine()
    df = loader(engine)

    imputer = joblib.load(f"{prefix}_imputer.joblib")
    scaler  = joblib.load(f"{prefix}_scaler.joblib")
    knn     = joblib.load(f"{prefix}_index.joblib")

    return SimilarityEngine(df, imputer, scaler, knn, features)


# ── Run a prediction ──────────────────────────────────────────────
player_name     = "Éder Militão"   # ← change this
player_position = "DF"              # ← change this: GK / FW / MF / DF

engine = load_engine(player_position)
results = engine.get_similar(player_name, top_n=5)

print(results.to_string(index=False))