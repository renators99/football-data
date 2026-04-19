"""Script to train a simple ML model for predicting match results."""

import logging
import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

from ..utils.config import load_config_from_env

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

BQ_CLIENT = bigquery.Client()

def train_model():
    """Train a model to predict match outcomes."""
    config = load_config_from_env()
    dataset = config.get("bq_dataset", "football_data")

    query = f"""
    SELECT
        home_team, away_team, full_time_home_goals, full_time_away_goals,
        half_time_home_goals, half_time_away_goals, home_shots, away_shots,
        CASE WHEN full_time_result = 'H' THEN 1 WHEN full_time_result = 'A' THEN 2 ELSE 0 END as result
    FROM `{config['gcp_project']}.{dataset}.silver_matches`
    WHERE season >= '2020'
    """

    df = BQ_CLIENT.query(query).to_dataframe()
    df = df.dropna()

    features = ['full_time_home_goals', 'full_time_away_goals', 'half_time_home_goals', 'half_time_away_goals', 'home_shots', 'away_shots']
    X = df[features]
    y = df['result']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    LOGGER.info(f"Model accuracy: {accuracy:.2f}")

    # Save model to GCS
    from google.cloud import storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(config['gcs_bucket'])
    blob = bucket.blob('models/football_match_predictor.pkl')
    blob.upload_from_string(joblib.dumps(model), content_type='application/octet-stream')

    LOGGER.info("Model saved to GCS")

if __name__ == "__main__":
    train_model()