import os

from pyspark.sql import SparkSession

from data_ingestion.config import job_parameters, download_ingestion_datasets
from data_ingestion.ingestion import Ingestion

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

# If running locally, first download the necessary datasets to twdu-germany/data-ingestion/tmp/
if ENVIRONMENT == "local":
    bucket = "twdu-germany-data-source"
    download_ingestion_datasets(bucket=bucket, parameters=job_parameters)

# ---------- Part III: Run Da Ting (for Part II, see data_ingestion/ingestion.py) ---------- #

spark = SparkSession \
    .builder \
    .appName("TWDU Germany Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Ingestion(spark, job_parameters).run()