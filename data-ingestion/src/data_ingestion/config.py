import sys
import os
import pandas as pd
from s3fs import S3FileSystem

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the TWDU Dev Container, don't forget to set the environment variable: TWDU_ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="TWDU_ENVIRONMENT", default="aws")

if ENVIRONMENT not in ["local", "aws"]:
    raise ValueError("""ENVIRONMENT must be "local" or "aws" only""")

elif ENVIRONMENT == "aws":
    try:
        from awsglue.utils import getResolvedOptions
        # Provide these arguments in your AWS Glue Job/JobRun definition
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

def download_twdu_dataset(s3_uri: str, destination: str):
    """Anonymously downloads a dataset from S3 to a custom destination.
       If any parent directories do not exist, this function will create them.
    """
    filename = destination.strip("/").split("/")[-1]
    folder = destination.replace(filename, "")

    if not os.path.exists(folder):
        os.makedirs(folder) # create destination folder(s) if they don't exist

    s3 = S3FileSystem(anon=True)
    print("Downloading from:", s3_uri)
    print("Downloading to:", destination)
    s3.get(s3_uri.replace("s3://", ""), destination, recursive=True)
    return