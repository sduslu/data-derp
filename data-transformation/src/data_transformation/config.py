import sys
import os
import pandas as pd
from s3fs import S3FileSystem

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
        Are you developing outside of the Dev Container? 
        If so, don't forget to set the environment variable: ENVIRONMENT=local
        ********
        """)

# EDIT HERE - set the output paths for your local Spark jobs as desired
elif ENVIRONMENT == "local":

    root_dir = os.path.dirname(os.path.realpath(__file__)).split("/data-transformation/")[0]
    job_parameters = {
        "co2_input_path":                  f"{root_dir}/datasets/transformation/inputs/EmissionsByCountry.parquet/",
        "temperatures_global_input_path":  f"{root_dir}/datasets/transformation/inputs/GlobalTemperatures.parquet/",
        "temperatures_country_input_path": f"{root_dir}/datasets/transformation/inputs/TemperaturesByCountry.parquet/",

        "co2_temperatures_global_output_path":  f"{root_dir}/data-transformation/tmp/outputs/GlobalEmissionsVsTemperatures.parquet/",
        "co2_temperatures_country_output_path": f"{root_dir}/data-transformation/tmp/outputs/CountryEmissionsVsTemperatures.parquet/",
        "europe_big_3_co2_output_path":         f"{root_dir}/data-transformation/tmp/outputs/EuropeBigThreeEmissions.parquet/",
        "co2_oceania_output_path":              f"{root_dir}/data-transformation/tmp/outputs/OceaniaEmissionsEdited.parquet/",
    }