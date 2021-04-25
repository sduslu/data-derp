import os

from pyspark.sql import SparkSession

from data_transformation.config import job_parameters, download_twdu_dataset
from data_transformation.transformation import Transformer

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

# If running locally, first download the necessary datasets to local storage
if ENVIRONMENT == "local":
    # Download the necessary datasets
    download_twdu_dataset(
        s3_uri="s3://twdu-germany-pl-km/data-ingestion/EmissionsByCountry.parquet/", 
        destination=job_parameters["co2_input_path"],
        format="parquet")
    download_twdu_dataset(
        s3_uri="s3://twdu-germany-pl-km/data-ingestion/GlobalTemperatures.parquet/", 
        destination=job_parameters["temperatures_global_input_path"],
        format="parquet")
    download_twdu_dataset(
        s3_uri="s3://twdu-germany-pl-km/data-ingestion/TemperaturesByCountry.parquet/", 
        destination=job_parameters["temperatures_country_input_path"],
        format="parquet")

# ---------- Part III: Run Da Ting (for Part II, see data_transformation/transformation.py) ---------- #

spark = SparkSession \
    .builder \
    .appName("TWDU Germany Glue Data Transformation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

Transformer(spark, job_parameters).run()