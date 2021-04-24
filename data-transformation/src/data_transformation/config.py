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
        # Provide these arguments in your AWS Glue Job/JobRun definition
        job_parameters = getResolvedOptions(
            sys.argv, 
            [
                "co2_input_path",                  # Input Table 1/3
                "temperatures_global_input_path",  # Input Table 2/3
                "temperatures_country_input_path", # Input Table 3/3

                "temperatures_co2_global_output_path",  # Output Table 1/4
                "temperatures_co2_country_output_path", # Output Table 2/4
                "europe_big_3_co2_output_path",         # Output Table 3/4
                "co2_interpolated_output_path",         # Output Table 4/4
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
        "co2_input_path":                  "/workspaces/twdu-germany/data-transformation/tmp/input-data/EmissionsByCountry.parquet",
        "temperatures_global_input_path":  "/workspaces/twdu-germany/data-transformation/tmp/input-data/GlobalTemperatures.parquet",
        "temperatures_country_input_path": "/workspaces/twdu-germany/data-transformation/tmp/input-data/TemperaturesByCountry.parquet",

        "temperatures_co2_global_output_path":  "/workspaces/twdu-germany/data-transformation/tmp/output-data/GlobalTemperaturesVsEmissions.parquet",
        "temperatures_co2_country_output_path": "/workspaces/twdu-germany/data-transformation/tmp/output-data/CountryTemperaturesVsEmissions.parquet",
        "europe_big_3_co2_output_path":         "/workspaces/twdu-germany/data-transformation/tmp/output-data/EuropeBigThreeEmissions.parquet",
        "co2_interpolated_output_path":         "/workspaces/twdu-germany/data-transformation/tmp/output-data/CountryEmissionsInterpolated.parquet",
    }

def download_twdu_dataset(s3_uri: str, destination: str, format: str):
    """Anonymously downloads a dataset from S3 to a custom destination.
       If any parent directories do not exist, this function will create them.
    """
    if format not in ["csv", "parquet"]:
        raise ValueError('file_format must be either "csv" or "parquet"')
   
    filename = destination.split("/")[-1]
    folder = destination.replace(filename, "")

    if not os.path.exists(folder):
        os.makedirs(folder) # create destination folder(s) if they don't exist, since Pandas does not allow non-existent parent folders

    if format == "csv":
        pandas_df = pd.read_csv(s3_uri, storage_options={"anon": True}) # to read directly from S3 using pd.read_csv, make sure you've pip installed s3fs
        pandas_df.to_csv(destination, index=False) # download as csv to local storage (for local development only)
    else:
        pandas_df = pd.read_parquet(s3_uri, storage_options={"anon": True}) # to read directly from S3 using pd.read_csv, make sure you've pip installed s3fs
        pandas_df.to_parquet(destination, index=False) # download as csv to local storage (for local development only)
    return