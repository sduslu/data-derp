import os

from pyspark.sql import SparkSession

from data_ingestion.config import job_parameters, download
from data_ingestion.ingestion import Ingestion

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

if ENVIRONMENT == "local":
    # Generate tmp folders and download data
    input_dir = "/workspaces/twdu-germany/data-ingestion/tmp/input-data/"
    output_dir = "/workspaces/twdu-germany/data-ingestion/tmp/output-data/"
    for dir in [input_dir, output_dir]:
        if not os.path.exists(dir): os.makedirs(dir)

    download(path=job_parameters["temperatures_country_input_path"])
    download(path=job_parameters["temperatures_global_input_path"])
    download(path=job_parameters["co2_input_path"])

# ---------- Part III: Run Da Ting (for Part II, see data_ingestion/ingestion.py) ---------- #

spark = SparkSession \
    .builder \
    .appName("TWDU Germany Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Ingestion(spark, job_parameters).run()