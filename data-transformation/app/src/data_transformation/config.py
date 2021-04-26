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
                "co2_input_path",                  # Input Table 1/3
                "temperatures_global_input_path",  # Input Table 2/3
                "temperatures_country_input_path", # Input Table 3/3

                "co2_temperatures_global_output_path",  # Output Table 1/4
                "co2_temperatures_country_output_path", # Output Table 2/4
                "europe_big_3_co2_output_path",         # Output Table 3/4
                "co2_oceania_output_path",              # Output Table 4/4
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
        "co2_input_path":                  "/workspaces/twdu-europe/data-transformation/app/tmp/input-data/EmissionsByCountry.parquet/",
        "temperatures_global_input_path":  "/workspaces/twdu-europe/data-transformation/app/tmp/input-data/GlobalTemperatures.parquet/",
        "temperatures_country_input_path": "/workspaces/twdu-europe/data-transformation/app/tmp/input-data/TemperaturesByCountry.parquet/",

        "co2_temperatures_global_output_path":  "/workspaces/twdu-europe/data-transformation/app/tmp/output-data/GlobalEmissionsVsTemperatures.parquet/",
        "co2_temperatures_country_output_path": "/workspaces/twdu-europe/data-transformation/app/tmp/output-data/CountryEmissionsVsTemperatures.parquet/",
        "europe_big_3_co2_output_path":         "/workspaces/twdu-europe/data-transformation/app/tmp/output-data/EuropeBigThreeEmissions.parquet/",
        "co2_oceania_output_path":              "/workspaces/twdu-europe/data-transformation/app/tmp/output-data/OceaniaEmissionsEdited.parquet/",
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