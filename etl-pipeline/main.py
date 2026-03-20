from extract import extract_data
from transform import transform_data
from load import load_data
import logging
import json
logging.basicConfig(level=logging.INFO)

def run_pipeline():
    try:
        logging.info("Starting pipeline")
        data = extract_data()
        df = transform_data(data)
        load_data(df)
        logging.info("Pipeline successful")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
def run_pipeline():
    print("Extracting data...")
    data = extract_data()

    print("Transforming data...")
    df = transform_data(data)

    print("Loading data...")
    load_data(df)

    print("Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()

def save_to_file(data):
    with open("crypto.json", "w") as f:
        json.dump(data, f)

data = extract_data()
save_to_file(data)


