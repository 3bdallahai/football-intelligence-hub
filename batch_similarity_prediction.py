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


# ── Run Batch Similarities ────────────────────────────────────────────

print("Initializing engines...")
gk_engine = load_engine("GK")
outfield_engine = load_engine("DF")

# 1. Extract columns including 'player_key' and 'minutes_played'
gk_players = gk_engine.df[['player_key', 'player_name', 'primary_position', 'minutes_played']].copy()
outfield_players = outfield_engine.df[['player_key', 'player_name', 'primary_position', 'minutes_played']].copy()

# Combine them into one master list
all_players = pd.concat([gk_players, outfield_players], ignore_index=True)
print(f"Total players in database: {len(all_players)}")


all_recommendations = []

# 3. Loop only through the filtered targets
for idx, row in all_players.iterrows():
    p_key = row['player_key']
    p_name = row['player_name']
    p_pos = row['primary_position']
    p_mins = row['minutes_played']
    
    router = ModelRouter()
    current_engine = gk_engine if router.route(p_pos) == "GK" else outfield_engine
    
    try:
        # Get the similar dataframe
        similar_df = current_engine.get_similar(p_name, top_n=5)
        
        # ⚠️ IMPORTANT: If get_similar() returns columns named 'player_key' and 'player_name',
        # rename them to clarify they belong to the PREDICTED/SIMILAR player.
        rename_dict = {}
        if 'player_key' in similar_df.columns:
            rename_dict['player_key'] = 'predicted_player_key'
        if 'player_name' in similar_df.columns:
            rename_dict['player_name'] = 'predicted_player_name'
            
        if rename_dict:
            similar_df = similar_df.rename(columns=rename_dict)
        
        # Track TARGET details in the final output mapping
        similar_df['target_player_key'] = p_key
        similar_df['target_player_name'] = p_name
        similar_df['target_position'] = p_pos
        similar_df['target_minutes'] = p_mins
        
        all_recommendations.append(similar_df)
        
    except Exception as e:
        print(f"Skipping {p_name} due to an error: {e}")

# 4. Combine and Export to a single CSV file ───────────────────────────
if all_recommendations:
    final_similarity_report = pd.concat(all_recommendations, ignore_index=True)
    
    # Organize columns cleanly: Targets first, then Predictions, then metrics/scores
    cols_order = [
        'target_player_key', 
        'target_player_name', 
        'target_position', 
        'target_minutes', 
        'predicted_player_key', 
        'predicted_player_name'
    ]
    
    # Add whatever metric/score columns are left over at the end (like similarity distance/score)
    remaining_cols = [c for c in final_similarity_report.columns if c not in cols_order]
    final_similarity_report = final_similarity_report[cols_order + remaining_cols]
    
    # Save the file cleanly
    output_file = "all_player_similarities.csv"
    final_similarity_report.to_csv(output_file, index=False)
    
    print(f"\n✅ Success! All predictions saved to: {output_file}")
    print(f"Total rows exported: {len(final_similarity_report)}")

else:
    print("❌ No recommendations were generated.")