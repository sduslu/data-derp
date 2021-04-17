from pyspark.sql import SparkSession

from twdu_ingestion.config import job_parameters
from twdu_ingestion.ingestion import Ingestion

# ---------- Part III: Run Da Ting (for Part II, see twdu_ingestion/ingestion.py) ---------- #

spark = SparkSession \
    .builder \
    .appName("TWDU Germany Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Ingestion(spark, job_parameters).run()