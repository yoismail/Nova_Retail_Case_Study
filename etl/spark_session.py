import os
import sys
import logging
from etl.logger import setup_logging, section
from pyspark.sql import SparkSession


# Configuration Constants
HADOOP_HOME = "C:/hadoop"
POSTGRES_JAR_PATH = "file:///C:/spark/jars/postgresql-42.7.4.jar"
APP_NAME = "Nova Retail ETL Pipeline"
DRIVER_MEMORY = "14g"
EXECUTOR_MEMORY = "14g"
SHUFFLE_PARTITIONS = "64"
NETWORK_TIMEOUT = "600s"


# Environment Setup
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(
    HADOOP_HOME, "bin") + os.pathsep + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def create_spark_session() -> SparkSession:
    """Create and configure a SparkSession with standard settings."""
    section("Creating Spark Session")

    try:
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.jars", POSTGRES_JAR_PATH) \
            .config("spark.driver.memory", DRIVER_MEMORY) \
            .config("spark.executor.memory", EXECUTOR_MEMORY) \
            .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryoserializer.buffer.max", "1024m") \
            .config("spark.driver.host", "localhost") \
            .config("spark.network.timeout", NETWORK_TIMEOUT) \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info(
            "============== Spark Session created successfully ==============")
        return spark

    except Exception as e:
        logging.error(f"Error creating Spark Session: {str(e)}")
        raise


def main() -> None:
    """Entry point for standalone execution."""
    setup_logging()
    create_spark_session()


if __name__ == "__main__":
    main()
