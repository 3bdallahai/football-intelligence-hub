# ⚽ Football Intelligence Hub

> A end-to-end Football Scouting and Analytics Platform — from data ingestion to ML-powered player similarity, built on a modern data engineering stack.

---

## 📖 Overview

**Football Intelligence Hub** is a comprehensive data platform designed for football scouting and analytics. It combines real-time data streaming, automated pipeline orchestration, transformation layers, and machine learning to deliver intelligent insights on players and matches.

Whether you're a scout looking for similar player profiles, an analyst exploring match data, or a data engineer building football pipelines — this hub brings it all together.

---

## 🏗️ Architecture


![Project Architecture](diagrams/project%20architecture.png)


---

## 📁 Project Structure

```
football-intelligence-hub/
├── ingestion/                  # Data ingestion pipelines
├── Streaming/                  # Real-time streaming processing
├── airflow/                    # Airflow DAGs for workflow orchestration
├── dbt/                        # dbt models for data transformation
├── ml/                         # Machine learning models & training
├── generate_new_data/          # Data generation & simulation scripts
├── similarity_predict.py       # Player similarity prediction (single)
├── batch_similarity_prediction.py  # Batch player similarity predictions
└── README.md
```

### Module Overview

| Module | Description |
|--------|-------------|
| `ingestion/` | Handles extraction and loading of football data from external sources |
| `Streaming/` | Real-time data stream processing for live match events |
| `airflow/` | DAG definitions for scheduling and orchestrating the full pipeline |
| `dbt/` | SQL-based data transformation models for analytics-ready tables |
| `ml/` | Machine learning model training, evaluation, and inference |
| `generate_new_data/` | Scripts to simulate or generate new football data for testing |
| `similarity_predict.py` | Predict the most similar players to a given player profile |
| `batch_similarity_prediction.py` | Run similarity predictions across a full dataset in batch mode |

---

## 🚀 Features

- **Data Ingestion** — Automated collection of football match, player, and event data
- **Real-time Streaming** — Processing of live match events via streaming pipelines
- **Pipeline Orchestration** — Apache Airflow DAGs to schedule and monitor all workflows
- **Data Transformation** — Clean, modeled datasets using dbt for downstream analytics
- **Player Similarity Engine** — ML-powered system to find similar player profiles
- **Batch Predictions** — Scale similarity scoring across large player datasets

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python |
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Streaming | Streaming pipeline (see `Streaming/`) |
| ML | Scikit-learn / custom models |
| Scripting | Shell |

---

## ⚙️ Getting Started

### Prerequisites

- Python 3.8+
- Apache Airflow
- dbt
- Required Python packages (see individual module requirements)

### Installation

```bash
# Clone the repository
git clone https://github.com/3bdallahai/football-intelligence-hub.git
cd football-intelligence-hub

# Install dependencies
pip install -r requirements.txt
```

### Running Player Similarity

```bash
# Single player prediction
python similarity_predict.py

# Batch predictions across all players
python batch_similarity_prediction.py
```

### Running Airflow DAGs

```bash
# Initialize Airflow
airflow db init

# Start the scheduler and webserver
airflow scheduler &
airflow webserver --port 8080
```

### Running dbt Models

```bash
cd dbt
dbt run
dbt test
```

---

## 🤖 ML — Player Similarity

The similarity engine uses player statistics and attributes to compute similarity scores between players. This powers scouting use cases such as:

- **Finding replacement players** for a squad position
- **Identifying talent** that mirrors a target player's profile
- **Benchmarking players** across leagues and seasons

**Scripts:**
- `similarity_predict.py` — Single player similarity lookup
- `batch_similarity_prediction.py` — Bulk scoring for scouting reports

---

## 🗺️ Roadmap

- [ ] Add a REST API layer for querying player similarity
- [ ] Build a dashboard for visualizing scouting reports
- [ ] Expand ML models (transfer value prediction, performance forecasting)
- [ ] Add support for additional data sources and leagues
- [ ] Containerize with Docker for easier deployment

---

## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

---

## 📄 License

This project is open source. See [LICENSE](LICENSE) for details.

---

