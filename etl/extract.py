import logging
import os
from etl.logger import setup_logging, timed
from etl.spark_session import create_spark_session
from etl.scale_data import scale_data
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)


# Raw Path Directory
RAW_PATH = "data/raw"


# ============================================================
# Explicit schemas for each source CSV.
# Defining these explicitly avoids a redundant CSV pass for
# inferSchema, and pins types so silent string-fallback doesn't
# happen (e.g. review_score was previously read as string).
# ============================================================

ORDERS_SCHEMA = StructType([
    StructField("order_id",                       StringType(),    True),
    StructField("customer_id",                    StringType(),    True),
    StructField("order_status",                   StringType(),    True),
    StructField("order_purchase_timestamp",       TimestampType(), True),
    StructField("order_approved_at",              TimestampType(), True),
    StructField("order_delivered_carrier_date",   TimestampType(), True),
    StructField("order_delivered_customer_date",  TimestampType(), True),
    StructField("order_estimated_delivery_date",  TimestampType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id",            StringType(),    True),
    StructField("order_item_id",       IntegerType(),   True),
    StructField("product_id",          StringType(),    True),
    StructField("seller_id",           StringType(),    True),
    StructField("shipping_limit_date", TimestampType(), True),
    StructField("price",               DoubleType(),    True),
    StructField("freight_value",       DoubleType(),    True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",                 StringType(),  True),
    StructField("product_category_name",      StringType(),  True),
    StructField("product_name_lenght",        IntegerType(), True),
    StructField("product_description_lenght", IntegerType(), True),
    StructField("product_photos_qty",         IntegerType(), True),
    StructField("product_weight_g",           IntegerType(), True),
    StructField("product_length_cm",          IntegerType(), True),
    StructField("product_height_cm",          IntegerType(), True),
    StructField("product_width_cm",           IntegerType(), True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",              StringType(),  True),
    StructField("customer_unique_id",       StringType(),  True),
    StructField("customer_zip_code_prefix", IntegerType(), True),
    StructField("customer_city",            StringType(),  True),
    StructField("customer_state",           StringType(),  True),
])

SELLERS_SCHEMA = StructType([
    StructField("seller_id",              StringType(),  True),
    StructField("seller_zip_code_prefix", IntegerType(), True),
    StructField("seller_city",            StringType(),  True),
    StructField("seller_state",           StringType(),  True),
])

ORDER_PAYMENTS_SCHEMA = StructType([
    StructField("order_id",             StringType(),  True),
    StructField("payment_sequential",   IntegerType(), True),
    StructField("payment_type",         StringType(),  True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value",        DoubleType(),  True),
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id",               StringType(),    True),
    StructField("order_id",                StringType(),    True),
    StructField("review_score",            IntegerType(),
                True),  # was string before
    StructField("review_comment_title",    StringType(),    True),
    StructField("review_comment_message",  StringType(),    True),
    StructField("review_creation_date",    TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True),
])


# Map dataset name to (filename, schema) so the extract loop stays simple
DATASET_REGISTRY = {
    "orders":         ("olist_orders_dataset.csv",         ORDERS_SCHEMA),
    "order_items":    ("olist_order_items_dataset.csv",    ORDER_ITEMS_SCHEMA),
    "products":       ("olist_products_dataset.csv",       PRODUCTS_SCHEMA),
    "customers":      ("olist_customers_dataset.csv",      CUSTOMERS_SCHEMA),
    "sellers":        ("olist_sellers_dataset.csv",        SELLERS_SCHEMA),
    "order_payments": ("olist_order_payments_dataset.csv", ORDER_PAYMENTS_SCHEMA),
    "reviews":        ("olist_order_reviews_dataset.csv",  REVIEWS_SCHEMA),
}


def ensure_raw_data_exists():
    """Check if the raw data directory exists before extraction."""
    if not os.path.exists(RAW_PATH):
        logging.error(f"Raw data directory '{RAW_PATH}' does not exist.")
        raise FileNotFoundError(
            f"Raw data directory '{RAW_PATH}' does not exist.")


def extract(spark):
    """
    Extract data from raw CSV files into Spark DataFrames using explicit schemas.

    Failures are collected across all datasets and raised as a single
    summary exception at the end, so the pipeline fails loudly with full
    visibility into which files failed and why.
    """
    logging.info("Extracting data...")

    dataset = {}
    failures = []

    for name, (file, schema) in DATASET_REGISTRY.items():
        try:
            path = os.path.join(RAW_PATH, file)

            # Step 1: Read with explicit schema — no inference pass
            df = spark.read.csv(
                path,
                header=True,
                schema=schema,
                timestampFormat="yyyy-MM-dd HH:mm:ss"
            )

            # Step 2: Scale only when testing (opt-in via env var)
            if os.getenv("SCALE_DATA", "false").lower() == "true":
                logging.info(f"Scaling {name} by 2x for testing...")
                df = scale_data(df, multiplier=2)

            # Step 3: Save into dataset dict for downstream layers
            dataset[name] = df

            logging.info(f"{name}: {df.count()} rows")
            logging.info(f"{name} schema:")
            df.printSchema()

        except Exception as e:
            logging.error(f"Failed to load {file}: {e}")
            failures.append((name, file, str(e)))

    # Fail loudly if any dataset didn't load — surface ALL failures, not just the first
    if failures:
        summary = "\n".join(
            f"  - {name} ({file}): {err}" for name, file, err in failures
        )
        raise RuntimeError(
            f"Extract failed for {len(failures)} dataset(s):\n{summary}"
        )

    return dataset


@timed
def main():
    """Main function to execute the extraction process."""
    setup_logging()
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
