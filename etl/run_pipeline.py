from etl.logger import setup_logging, timed, section
from etl.spark_session import create_spark_session
from etl.extract import extract
from etl.bronze import bronze_layer
from etl.silver import silver_layer
from etl.gold import gold_layer
from etl.load import load_df_to_postgres
import logging

# Initialize logging
setup_logging()


@timed
def main():
    spark = None
    try:
        # ONE Spark Session for EVERYTHING
        spark = create_spark_session()  # Full config + POSTGRES JAR

        section("STEP 1: EXTRACT")
        datasets = extract(spark)

        section("STEP 2: BRONZE")
        datasets = bronze_layer(datasets)

        section("STEP 3: SILVER")
        silver_df = silver_layer(datasets)

        section("STEP 4: GOLD")
        fact_sales, sales_summary, sales_by_month_state = gold_layer(
            silver_df)  # Returns all three tables

        # STEP 5: LOAD DIRECTLY TO POSTGRESQL
        section("STEP 5: LOAD TO POSTGRESQL")

        # Load fact_sales
        load_df_to_postgres(
            df=fact_sales,
            schema_name="retail_gold",
            table_name="fact_sales",
            mode="overwrite"
        )

        # Load sales_summary
        load_df_to_postgres(
            df=sales_summary,
            schema_name="retail_gold",
            table_name="sales_summary",
            mode="overwrite"
        )

        # Load sales_by_month_state
        load_df_to_postgres(
            df=sales_by_month_state,
            schema_name="retail_gold",
            table_name="sales_by_month_state",
            mode="overwrite"
        )

        logging.info(
            "\033[92m =============== PIPELINE COMPLETED ===============\033[0m")

    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
            logging.info("🔌Spark session stopped")


if __name__ == "__main__":
    main()
