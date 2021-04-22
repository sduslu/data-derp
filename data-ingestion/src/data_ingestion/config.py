import sys
import os
import pandas as pd

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")


if ENVIRONMENT not in ["local", "aws"]:
    raise ValueError("""ENVIRONMENT must be "local" or "aws" only""")

elif ENVIRONMENT == "aws":
    try:
        from awsglue.utils import getResolvedOptions
        # Provide these parameters in your AWS Glue Job/JobRun definition
        job_parameters = getResolvedOptions(
            sys.argv, 
            [
                "temperatures_country_input_path",
                "temperatures_country_output_path",
                "temperatures_global_input_path",
                "temperatures_global_output_path",
                "co2_input_path",
                "co2_output_path"
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
elif ENVIRONMENT == "local":

    job_parameters = {
        "temperatures_country_input_path":  "/workspaces/twdu-germany/data-ingestion/tmp/input-data/TemperaturesByCountry.csv",
        "temperatures_country_output_path": "/workspaces/twdu-germany/data-ingestion/tmp/output-data/TemperaturesByCountry.parquet",
        "temperatures_global_input_path":   "/workspaces/twdu-germany/data-ingestion/tmp/input-data/GlobalTemperatures.csv",
        "temperatures_global_output_path":  "/workspaces/twdu-germany/data-ingestion/tmp/output-data/GlobalTemperatures.parquet",
        "co2_input_path":                   "/workspaces/twdu-germany/data-ingestion/tmp/input-data/EmissionsByCountry.csv",
        "co2_output_path":                  "/workspaces/twdu-germany/data-ingestion/tmp/output-data/EmissionsByCountry.parquet",
    }

    def download(path):
        """Anonymously downloads csv from S3 to a custom (potentially non-existent) destination"""
        if ("/" not in path) or (".csv" not in path):
            raise ValueError("path should be of format ./.../<filename>.csv")

        filename = path.split("/")[-1]
        folder = path.replace(filename, "")

        s3_path = "s3://twdu-germany-data-source/" + filename
        pandas_df = pd.read_csv(s3_path) # NOTE: to read from S3 straight from pd.read_csv, make sure you've already pip installed s3fs"

        # create destination folder(s) if doesn't exist so that Pandas can successfully write the csv into the folder
        if not os.path.exists(folder): 
            os.makedirs(folder)

        pandas_df.to_csv(path, index=False) # download as csv to local storage (for local development only)
        return