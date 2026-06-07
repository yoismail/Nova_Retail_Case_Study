import logging
import os
from etl.logger import setup_logging, timed
from etl.spark_session import create_spark_session
from etl.scale_data import scale_data


# Initialize logging
setup_logging()

# Raw Path Directory
RAW_PATH = "data/raw"


def ensure_raw_data_exists():
    """Check if the raw data directory exists before extraction."""
    if not os.path.exists(RAW_PATH):
        logging.error(f"Raw data directory '{RAW_PATH}' does not exist.")
        raise FileNotFoundError(
            f"Raw data directory '{RAW_PATH}' does not exist.")


def extract(spark):
    """Extract data from raw CSV files into Spark DataFrames."""
    logging.info("Extracting data...")

    dataset = {}

    files = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "customers": "olist_customers_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
    }

    for name, file in files.items():
        try:
            path = os.path.join(RAW_PATH, file)
            # Step 1: Read raw file - temporary variable for clarity
            # Added timestampFormat for consistent parsing of date columns
            df = spark.read.csv(path, header=True, inferSchema=True,
                                timestampFormat="yyyy-MM-dd HH:mm:ss")

            # Step 2: SCALE DATA ONLY WHEN TESTING - comment out for production
            if os.getenv("SCALE_DATA", "false").lower() == "true":
                logging.info(f"Scaling {name} by 20x for testing...")
                # Only works if called like this: $SCALE_DATA=true python etl/extract.py
                df = scale_data(df, multiplier=20)

            # Step 3: SAVE SCALED VERSION into dataset - Permanent variable for downstream layers
            dataset[name] = df

            logging.info(f"{name}: {df.count()} rows")
            logging.info(f"{name} schema:")
            df.printSchema()

        except Exception as e:
            logging.error(f"Failed to load {file}: {e}")

    return dataset


@timed
def main():
    """Main function to execute the extraction process."""
    ensure_raw_data_exists()

    spark = create_spark_session()
    try:
        extract(spark)

    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("===========Spark session stopped.==============")


if __name__ == "__main__":
    main()
