from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from etl.db_config import DB_CONFIG
import logging


def get_jdbc_url() -> str:
    """Build PostgreSQL JDBC URL from config"""
    return (
        f"jdbc:postgresql://{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


def load_df_to_postgres(
    df: DataFrame,
    schema_name: str,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Load an existing Spark DataFrame directly into PostgreSQL.

    Args:
        df: Spark DataFrame to write (from Gold layer)
        schema_name: Target schema
        table_name: Target table
        mode: 'overwrite' or 'append'
    """
    logging.info(f"Starting load → {schema_name}.{table_name}")

    try:
        # Step 1: Basic validation
        row_count = df.count()
        col_count = len(df.columns)
        logging.info(f"Data ready: {row_count:,} rows | {col_count} columns")

        if row_count == 0:
            logging.warning("No data — skipping load.")
            return

        # Step 2: Write directly
        jdbc_url = get_jdbc_url()

        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", f"{schema_name}.{table_name}") \
          .option("user", DB_CONFIG["user"]) \
          .option("password", DB_CONFIG["password"]) \
          .option("driver", "org.postgresql.Driver") \
          .option("batchsize", 10000) \
          .mode(mode) \
          .save()

        logging.info(
            f"Load successful → {schema_name}.{table_name} — {row_count:,} rows")

        logging.info("===========Starting validation check...===========")

        # Step 3: Quick validation
        check_df = df.sparkSession.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema_name}.{table_name}") \
            .option("user", DB_CONFIG["user"]) \
            .option("password", DB_CONFIG["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        loaded = check_df.count()
        if loaded == row_count:
            logging.info("Validation OK — row count matches")
        else:
            logging.warning(
                f"Mismatch! Source: {row_count:,} | DB: {loaded:,}")

    except AnalysisException as e:
        logging.error(f"Data error: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Load failed: {str(e)}", exc_info=True)
        raise
