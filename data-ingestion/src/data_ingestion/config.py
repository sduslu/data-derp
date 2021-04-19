import sys
import os

import pandas as pd
from pyspark.sql import SparkSession

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

if ENVIRONMENT == "aws":
    try:
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
    except ModuleNotFoundError:
        raise ModuleNotFoundError("""
        No module named 'awsglue' 
        ********
        Hello ThoughtWorker! Are you developing outside of the TWDU Dev Container? 
        If so, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
        ********
        """)

# EDIT HERE - set the output paths for your local Spark jobs as desired
elif  ENVIRONMENT == "local":

    job_parameters = {
        "temperature_uri": "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Berkley%20Land-Ocean%20Temperature%20Anomaly/Berkley%20Land-Ocean%20Temperature%20Anomaly.csv",
        "temperature_output_dir": "./data-ingestion/tmp/output-data/temperature.parquet",
        "co2_uri": "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2021))/CO2%20emissions%20(Aggregate%20dataset%20(2021)).csv",
        "co2_output_dir": "./data-ingestion/tmp/output-data/co2.parquet",
    }

    def replace_invalid_chars(column_name):
        INVALID_CHARS = " ,;{}()\\n\\t=" # characters not allowed by Spark SQL
        UNDERSCORE_CANDIDATES = [" ", ",", ";", "="]
        for char in INVALID_CHARS:
            replacement = "_" if char in UNDERSCORE_CANDIDATES else ""
            column_name = column_name.replace(char, replacement)
        return column_name.lower() # return as lowercase

    def download_and_convert(uri, local_path):
        pandas_df = pd.read_csv(uri)
        pandas_df.columns = [replace_invalid_chars(x) for x in pandas_df.columns]

        csv_path, parquet_path = [f"{local_path}.{extension}" for extension in ["csv", "parquet"]]
        pandas_df.to_csv(csv_path, index=False)

        spark \
            .read.format("csv").load(csv_path) \
            .write.format("parquet").mode("overwrite").save(parquet_path)
        os.remove(csv_path)

    spark = SparkSession.builder.getOrCreate()

    input_dir = "./data-ingestion/tmp/input-data/"
    output_dir = "./data-ingestion/tmp/output-data/"
    temperature_path = "./data-ingestion/tmp/input-data/temperature"
    co2_path = "./data-ingestion/tmp/input-data/co2"

    for dir in [input_dir, output_dir]:
        if not os.path.exists(dir): os.makedirs(dir)

    download_and_convert(job_parameters["temperature_uri"], temperature_path)
    download_and_convert(job_parameters["co2_uri"], co2_path)

else:
    raise ValueError("""ENVIRONMENT must be "local" or "aws" only""")
