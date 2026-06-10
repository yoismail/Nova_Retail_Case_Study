import os
import logging
from etl.logger import setup_logging
from etl.spark_session import create_spark_session
from etl.extract import extract
from pyspark.sql.functions import year, month, col


# ============================================================
# Datasets that get partitioned by year/month at Bronze write.
# Maps dataset name to the source timestamp column used to
# derive partition columns. Other datasets are written
# unpartitioned (small dimensions: customers, sellers, products
# and not-yet-date-bearing facts: order_payments, reviews).
# ============================================================
PARTITIONED_DATASETS = {
    "orders":      "order_purchase_timestamp",
    "order_items": "shipping_limit_date",
}


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


def _add_partition_columns(df, ts_col):
    """
    Derive order_year and order_month from a timestamp column for partitioning.

    These columns are storage-layout metadata only — they enable partition
    pruning on read without changing the source data semantics.
    """
    return (
        df
        .withColumn("order_year", year(col(ts_col)))
        .withColumn("order_month", month(col(ts_col)))
    )


def bronze_layer(datasets: dict) -> dict:
    """
    Bronze Layer processing:
    - Stores raw extracted data as Parquet for efficient storage/reading
    - Partitions order-level fact tables by year/month for downstream pruning
    - Small dimensions written unpartitioned
    - Preserves source column fidelity in all cases

    Args:
        datasets: Dictionary of DataFrames loaded from source files by extract()

    Returns:
        Unchanged datasets dictionary, ready to pass to next layer
    """
    logging.info("Starting Bronze Layer processing...")

    for name, df in datasets.items():
        output_path = layer_path("bronze", name)

        if name in PARTITIONED_DATASETS:
            # Fact-shaped table: derive partition columns from its timestamp
            ts_col = PARTITIONED_DATASETS[name]
            df_partitioned = _add_partition_columns(df, ts_col)

            df_partitioned.write \
                .mode("overwrite") \
                .partitionBy("order_year", "order_month") \
                .parquet(output_path)

            logging.info(
                f"Successfully written to Bronze: {name} "
                f"(partitioned by year/month on {ts_col})"
            )
        else:
            # Dimension or untimed table: write unpartitioned
            df.write \
              .mode("overwrite") \
              .parquet(output_path)

            logging.info(f"Successfully written to Bronze: {name}")

    logging.info(
        "=========== Bronze Layer processing completed successfully ===========")
    return datasets


def main():
    """Main entry point: orchestrates extraction and Bronze storage."""
    setup_logging()
    spark = None
    try:
        spark = create_spark_session()
        datasets = extract(spark)
        bronze_layer(datasets)

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

    finally:
        if spark:
            spark.stop()
            logging.info("=========== Spark session stopped ===========")


if __name__ == "__main__":
    main()
