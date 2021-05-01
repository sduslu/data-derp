import os

from pyspark.sql import SparkSession

from data_ingestion.config import job_parameters
from data_ingestion.ingestion import Ingester

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the data-derp container, don't forget to set the environment variable: ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="ENVIRONMENT", default="aws")

# ---------- Part III: Run Da Ting (for Part II, see data_ingestion/ingestion.py) ---------- #

print("Starting Spark Job")
print()

spark = SparkSession \
    .builder \
    .appName("Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Ingester(spark, job_parameters).run()

print()
print("Spark Job Complete")
