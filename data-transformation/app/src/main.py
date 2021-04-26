import os

from pyspark.sql import SparkSession

from data_transformation.config import job_parameters, download_twdu_dataset
from data_transformation.transformation import Transformer

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

# If running locally, first download the necessary datasets to local storage
if ENVIRONMENT == "local":

    # Change this to your team name (e.g. ab-cd-ef).
    team_name = "foo-bar"

    # Download the necessary datasets locally from the ingestion sets
    download_twdu_dataset(
        s3_uri=f"s3://twdu-europe-{team_name}/data-ingestion/EmissionsByCountry.parquet/",
        destination=job_parameters["co2_input_path"])
    download_twdu_dataset(
        s3_uri=f"s3://twdu-europe-{team_name}/data-ingestion/GlobalTemperatures.parquet/",
        destination=job_parameters["temperatures_global_input_path"])
    download_twdu_dataset(
        s3_uri=f"s3://twdu-europe-{team_name}/data-ingestion/TemperaturesByCountry.parquet/",
        destination=job_parameters["temperatures_country_input_path"])

# ---------- Part III: Run Da Ting (for Part II, see data_transformation/transformation.py) ---------- #

print("TWDU: Starting Spark Job")

spark = SparkSession \
    .builder \
    .appName("TWDU Glue Data Transformation") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Enable Arrow-based columnar data transfers
# In Spark 2.4.3 (the version used in AWS Glue), Apache Arrow must be enabled to use Pandas UDFs
# https://spark.apache.org/docs/2.4.3/sql-pyspark-pandas-with-arrow.html
# For newer versions of Spark, Arrow is enabled by default.
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

Transformer(spark, job_parameters).run()

print("TWDU: Spark Job Complete")