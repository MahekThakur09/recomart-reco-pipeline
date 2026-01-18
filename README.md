# RecoMart Recommendation Pipeline

A scalable and modular data pipeline for an e-commerce recommendation engine.
This project simulates real-world ingestion, validation, feature engineering,
and model training workflows.

## Project Structure

recomart-reco-pipeline/
│
├── data/
│   ├── raw/            # Raw ingested data
│   ├── validated/      # Quality-checked data
│   ├── processed/      # Cleaned and transformed data
│
├── ingestion/          # Data ingestion logic
├── validation/         # Data quality & schema checks
├── preparation/       # Data cleaning & joins
├── features/           # Feature engineering
├── feature_store/      # Centralized feature storage
├── model/              # Model training & evaluation
├── orchestration/      # Pipeline orchestration
├── reports/            # Metrics & reports
├── logs/               # Execution logs
