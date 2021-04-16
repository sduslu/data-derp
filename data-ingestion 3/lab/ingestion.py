import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

import pandas as pd

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue-specific locally
ENVIRONMENT = "local" # DO NOT MODIFY: CD pipeline will replace "local" with "aws"

if ENVIRONMENT == "local":
    job_parameters = {
        "temperature_uri": "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Berkley%20Land-Ocean%20Temperature%20Anomaly/Berkley%20Land-Ocean%20Temperature%20Anomaly.csv",
        "temperature_output_dir": "s3://twdu-germany/temperature-output-parquet/",
        "co2_uri": "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2021))/CO2%20emissions%20(Aggregate%20dataset%20(2021)).csv",
        "co2_output_dir": "s3://twdu-germany-pl-km-test/co2-output-parquet/",
    }
elif ENVIRONMENT == "aws":
    from awsglue.utils import getResolvedOptions
    # Provide these parameters in your AWS Glue Job/JobRun definition
    job_parameters = getResolvedOptions(
        sys.argv, 
        [
            "temperatures_uri",
            "temperature_output_dir",
            "co2_uri",
            "co2_output_dir"
        ]
    )
else:
    raise ValueError("""ENVIRONMENT must be "local" or "aws" only""")

spark = SparkSession \
    .builder \
    .appName("TWDU Germany Glue Data Ingestion") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# ---------- Part II: Business Logic ---------- #

co2_df = pd.read_csv(job_parameters["co2_uri"])
co2_df.columns = [x.lower().replace(" ", "_") for x in co2_df.columns]
# print(co2_df)

co2_spark = spark.createDataFrame(co2_df).select(*co2_df.columns[:3]).limit(1000)
co2_spark.write.format("parquet").save(path=job_parameters["co2_output_dir"], mode="overwrite")

# NOTE: to read s3 straight outta pandas, install s3fs first
# temp_countries_df = pd.read_csv("s3://twdu-germany-source/GlobalLandTemperaturesByCountry.csv")
# print(temp_countries_df)