import pandas as pd
import requests
import logging
import time
from pathlib import Path

# -----------------------
# Setup paths
# -----------------------
RAW_DATA_PATH = Path("data/raw")
LOG_PATH = Path("logs")

RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)

# -----------------------
# Logging config
# -----------------------
logging.basicConfig(
    filename=LOG_PATH / "ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------
# Load interaction data
# -----------------------
def load_events():
    try:
        logging.info("Loading Events_1.csv")
        df = pd.read_csv(
    RAW_DATA_PATH / "Events_1.csv",
    encoding="latin1",
    engine="python",
    on_bad_lines="skip"
)
        logging.info(f"Loaded events with shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error loading events.csv: {e}")
        raise

# -----------------------
# Load product metadata
# -----------------------
def load_item_properties():
    try:
        logging.info("Loading itemproperties.csv")
        df = pd.read_csv(
    RAW_DATA_PATH / "ItemPropertiesPart1.csv",
    encoding="latin1",
    engine="python",
    on_bad_lines="skip"
)
        logging.info(f"Loaded item properties with shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error loading itemproperties.csv: {e}")
        raise


# -----------------------
# Run ingestion
# -----------------------
def run_ingestion():
    logging.info("Starting ingestion pipeline")

    events_df = load_events()
    props_df = load_item_properties()
   

    logging.info("Ingestion completed successfully")

if __name__ == "__main__":
    run_ingestion()
