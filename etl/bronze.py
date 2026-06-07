from etl.logger import setup_logging
from etl.extract import extract
from etl.spark_session import create_spark_session
import os
import logging


# Initialise logging — runs once when script starts
setup_logging()


def layer_path(layer, name):
    """
    Construct a consistent file path for a given layer and dataset name.

    Args:
        layer: Name of the data zone/layer (e.g. 'bronze', 'silver', 'gold')
        name: Name of the dataset/table

    Returns:
        Full relative path string
    """
    return os.path.join("data", layer, name)


def bronze_layer(datasets: dict) -> dict:
    """
    Bronze Layer processing:
    - Stores raw extracted data exactly as it is, without changes
    - Saves each dataset in Parquet format for efficient storage/reading
    - Preserves original structure, schema and all records (raw data archive)

    Args:
        datasets: Dictionary of DataFrames loaded from source files by extract()

    Returns:
        Unchanged datasets dictionary, ready to pass to next layer
    """
    logging.info("Starting Bronze Layer processing...")

    # Process each dataset from the extraction step
    for name, df in datasets.items():
        # Build output path: e.g. data/bronze/orders
        output_path = layer_path("bronze", name)

        # Write raw data to Bronze:
        # - format: Parquet (compressed, columnar, fast)
        # - mode: overwrite (replace full dataset each run)
        df.write \
          .mode("overwrite") \
          .parquet(output_path)

        logging.info(f"Successfully written to Bronze: {name}")

    logging.info(
        "=========== Bronze Layer processing completed successfully ===========")
    return datasets


def main():
    """Main entry point: orchestrates extraction and Bronze storage."""
    spark = None
    try:
        # Create Spark session using shared configuration
        spark = create_spark_session()

        # Step 1: Extract all raw CSV files into DataFrames
        datasets = extract(spark)

        # Step 2: Write raw data to Bronze layer
        bronze_layer(datasets)

    except Exception as e:
        # Log error details and propagate failure
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

    finally:
        # Always release Spark resources, even if something failed
        if spark:
            spark.stop()
            logging.info("=========== Spark session stopped ===========")


if __name__ == "__main__":
    main()
