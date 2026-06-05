from sqlalchemy.engine import Engine
import pandas as pd


def load_outfield_features(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM FOOTBALL_GOLD.TRANSFORMED_SIMILARITY.MART_SIMILARITY_OUTFIELD", engine)


def load_goalkeeper_features(engine: Engine) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM FOOTBALL_GOLD.TRANSFORMED_SIMILARITY.MART_SIMILARITY_GOALKEEPER", engine)