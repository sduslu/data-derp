import sys
import os

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue dependencies locally
# If developing outside of the Dev Container, don't forget to set the environment variable: ENVIRONMENT=local
ENVIRONMENT = os.getenv(key="ENVIRONMENT", default="aws")

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
        Are you developing outside of the Dev Container? 
        If so, don't forget to set the environment variable: ENVIRONMENT=local
        ********
        """)

# EDIT HERE - set the output paths for your local Spark jobs as desired
elif ENVIRONMENT == "local":

    root_dir = os.path.dirname(os.path.realpath(__file__)).split("/data-ingestion/")[0]
    job_parameters = {
        "co2_input_path":                   f"{root_dir}/datasets/ingestion/inputs/EmissionsByCountry.csv",
        "temperatures_global_input_path":   f"{root_dir}/datasets/ingestion/inputs/GlobalTemperatures.csv",
        "temperatures_country_input_path":  f"{root_dir}/datasets/ingestion/inputs/TemperaturesByCountry.csv",

        "co2_output_path":                  f"{root_dir}/datasets/data-ingestion/tmp/outputs/EmissionsByCountry.parquet/",
        "temperatures_global_output_path":  f"{root_dir}/datasets/data-ingestion/tmp/outputs/GlobalTemperatures.parquet/",
        "temperatures_country_output_path": f"{root_dir}/datasets/data-ingestion/tmp/outputs/TemperaturesByCountry.parquet/",
    }