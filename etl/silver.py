import os
import logging
from etl.logger import setup_logging
from etl.spark_session import create_spark_session
from etl.extract import extract
from pyspark.sql.functions import (
    col, date_format, to_timestamp, year, month, dayofmonth, dayofweek, to_date,
    broadcast,
)


def layer_path(layer, name):
    """
    Construct a consistent file path for a given layer and dataset name.

    Args:
        layer: Name of the data layer (e.g. 'bronze', 'silver', 'gold')
        name: Name of the dataset/table

    Returns:
        Full path string
    """
    return os.path.join("data", layer, name)


def silver_layer(datasets):
    """
    Process data into Silver layer:
    - Join all relevant datasets into one wide table
    - Broadcast small dimension tables to avoid wide shuffles
    - Apply basic data quality rules
    - Cache the result so downstream layers reuse the joined frame
    - Save cleaned, structured data as Parquet

    Args:
        datasets: Dictionary of DataFrames loaded from raw/bronze
    """
    logging.info("Starting Silver Layer processing...")

    # Step 1: Validate inputs
    required_datasets = ["orders", "customers", "products",
                         "order_payments", "order_items", "sellers"]
    for ds_name in required_datasets:
        if ds_name not in datasets:
            raise ValueError(f"Missing required dataset: '{ds_name}'")
        if datasets[ds_name].isEmpty():
            logging.warning(
                f"Dataset '{ds_name}' is empty - joins may produce unexpected results")

    # Extract datasets from dictionary
    orders = datasets["orders"]
    customers = datasets["customers"]
    products = datasets["products"]
    payments = datasets["order_payments"]
    items = datasets["order_items"]
    sellers = datasets["sellers"]

    # Step 2: Join all tables
    # LEFT joins: keep ALL orders, even if some related information is missing.
    # broadcast() hints on small dimensions avoid expensive shuffle joins.
    # Sellers, products, customers all comfortably fit in executor memory.
    silver_df = (
        orders
        .join(broadcast(customers), "customer_id", "left")
        .join(payments, "order_id", "left")
        .join(items, "order_id", "left")
        .join(broadcast(products), "product_id", "left")
        .join(broadcast(sellers), "seller_id", "left")

        # CONVERT TO PROPER DATE/TIME TYPES
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
        # Date only
        .withColumn("order_purchase_date", to_date(col("order_purchase_timestamp")))

        # CAST NUMERIC COLUMNS to DECIMAL for precise aggregation
        .withColumn("payment_value", col("payment_value").cast("DECIMAL(10,2)"))
        .withColumn("freight_value", col("freight_value").cast("DECIMAL(10,2)"))

        # EXTRACT DATE PARTS — very useful for grouping later
        .withColumn("order_year", year(col("order_purchase_date")))
        .withColumn("order_month", month(col("order_purchase_date")))
        .withColumn("order_day", dayofmonth(col("order_purchase_date")))
        # 1=Sun, 7=Sat
        .withColumn("order_weekday", dayofweek(col("order_purchase_date")))
        # "January"
        .withColumn("order_month_name", date_format(col("order_purchase_date"), "MMMM"))
        # "2018-01"
        .withColumn("order_year_month", date_format(col("order_purchase_date"), "yyyy-MM"))
    )

    # Step 3: Data quality filtering
    # Remove rows where payment value is NULL — these are invalid sales records
    silver_df = silver_df.filter(col("payment_value").isNotNull())

    # Step 4: Cache before reuse — count + write + downstream Gold all hit this frame.
    # Without cache, the join chain above would be recomputed on every action.
    # Defaults to MEMORY_AND_DISK so we spill safely if memory is tight at scale.
    silver_df = silver_df.cache()

    # Log row count after cleaning (this materialises the cache)
    row_count = silver_df.count()
    logging.info(f"Valid records after cleaning: {row_count:,}")

    # Step 5: Save Silver dataset
    output_path = layer_path("silver", "sales_data")
    silver_df.write \
             .mode("overwrite") \
             .parquet(output_path)

    logging.info(
        "=========== Silver Layer processing completed successfully ===========")

    return silver_df


def main():
    """Main entry point: orchestrates extraction and Silver transformation."""
    setup_logging()
    spark = None
    try:
        spark = create_spark_session()
        datasets = extract(spark)
        silver_layer(datasets)

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

    finally:
        if spark:
            spark.stop()
            logging.info("=========== Spark session stopped ===========")


if __name__ == "__main__":
    main()
