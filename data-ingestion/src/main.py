import os

from pyspark.sql import SparkSession

from data_ingestion.config import job_parameters
from data_ingestion.ingestion import Ingester

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

# ---------- Part III: Run Da Ting (for Part II, see data_ingestion/ingestion.py) ---------- #

print("TWDU: Starting Spark Job")

spark = SparkSession \
    .builder \
    .appName("TWDU Europe Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Ingester(spark, job_parameters).run()

print("TWDU: Spark Job Complete")