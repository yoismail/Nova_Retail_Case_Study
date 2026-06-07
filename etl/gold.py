from pyspark.sql.functions import sum, count_distinct, avg, round
from etl.logger import setup_logging
import os
import logging
from etl.spark_session import create_spark_session


# Initialise logging
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


def gold_layer(silver_df):
    """
    Gold Layer processing:
    - Create business ready datasets from clean Silver data
    - 1. Fact table: detailed sales records with key attributes only
    - 2. Summary table: aggregated metrics by region and product category

    Args:
        silver_df: Clean, joined DataFrame from Silver layer
    """
    logging.info("Starting Gold Layer processing...")

    # Step 1: Validate input
    required_columns = [
        "order_id", "customer_state", "product_category_name", "payment_value",
        "freight_value", "payment_type", "order_purchase_date", "order_year", "order_month", "order_year_month", "order_weekday"
    ]
    for col_name in required_columns:
        if col_name not in silver_df.columns:
            raise ValueError(
                f"Missing required column in Silver data: '{col_name}'")

    # Step 2: Create Fact Sales table
    # Keep only relevant columns: lightweight, analysis-ready detailed table
    fact_sales = silver_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "seller_id",
        "customer_state",
        "payment_value",
        "payment_type",
        "product_category_name",
        "freight_value",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_year",
        "order_month",
        "order_day",
        "order_year_month",
        "order_weekday"
    )

    logging.info(f"Fact Sales created: {fact_sales.count()} records")

    # Step 3: Create Sales Summary report
    # Aggregate metrics grouped by State and Product Category
    sales_summary = (
        fact_sales
        .groupBy("customer_state", "product_category_name")
        .agg(
            count_distinct("order_id").alias("total_orders"),
            round(sum("payment_value"), 2).alias("total_revenue"),
            round(avg("payment_value"), 2).alias("avg_order_value")
        )
        # sort nicely
        .orderBy("customer_state", "total_revenue", ascending=[True, False])

    )

    logging.info(f"Sales Summary created: {sales_summary.count()} groups")

    # Step 4: Sales Summary BY MONTH + STATE
    sales_by_month_state = (
        fact_sales
        .groupBy("order_year_month", "customer_state")
        .agg(
            count_distinct("order_id").alias("total_orders"),
            round(sum("payment_value"), 2).alias("total_revenue"),
            round(avg("payment_value"), 2).alias("avg_order_value")
        )
        .orderBy("order_year_month", "customer_state")
    )

    # Step 5: Save Gold datasets
    # Detailed fact table
    fact_sales.write \
        .mode("overwrite") \
        .parquet(layer_path("gold", "fact_sales"))

    # Aggregated summary table
    sales_summary.write \
                 .mode("overwrite") \
                 .parquet(layer_path("gold", "sales_summary"))

    # Sales by month + state
    sales_by_month_state.write \
        .mode("overwrite") \
        .parquet(layer_path("gold", "sales_by_month_state"))

    logging.info(
        "=========== Gold Layer processing completed successfully ===========")

    # Return all three tables
    return fact_sales, sales_summary, sales_by_month_state


def main():
    """Main entry point: load Silver data and build Gold datasets."""
    spark = None
    try:
        # Create Spark session
        spark = create_spark_session()

        # Read clean, validated Silver data
        silver_path = layer_path("silver", "sales_data")
        logging.info(f"Reading Silver data from: {silver_path}")
        silver_df = spark.read.parquet(silver_path)

        # Run Gold transformations
        gold_layer(silver_df)

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise

    finally:
        # Always release resources
        if spark:
            spark.stop()
            logging.info("=========== Spark session stopped ===========")


if __name__ == "__main__":
    main()
