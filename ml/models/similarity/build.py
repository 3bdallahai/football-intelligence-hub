import yaml
from ml.utils.db import get_engine
from ml.features.load_similarity_data import (
    load_outfield_features,
    load_goalkeeper_features,
)
from ml.models.similarity.trainer import SimilarityTrainer
from dotenv import load_dotenv
load_dotenv()  # loads .env before os.getenv() calls


def load_cfg():
    with open("ml/config/similarity.yaml") as f:
        return yaml.safe_load(f)


def main():

    engine = get_engine()
    cfg = load_cfg()

    # ---------------- OUTFIELD ----------------
    outfield = load_outfield_features(engine)

    trainer = SimilarityTrainer(cfg["outfield_features"], cfg)

    imputer, scaler, knn  = trainer.train(outfield)

    trainer.save(imputer, scaler, knn,  "ml/data/outfield_model")


    # ---------------- GK ----------------
    gk = load_goalkeeper_features(engine)

    trainer = SimilarityTrainer(cfg["goalkeeper_features"], cfg)

    imputer, scaler, knn  = trainer.train(gk)

    trainer.save(imputer, scaler, knn , "ml/data/gk_model")


if __name__ == "__main__":
    main()