import sys
import os

# ---------- Part I: Job Setup ---------- #

# By sticking with standard Spark, we can avoid having to deal with Glue-specific locally
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
        "temperature_output_dir": "s3://twdu-germany-pl-km/temperature-output-parquet/",
        "co2_uri": "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/CO2%20emissions%20(Aggregate%20dataset%20(2021))/CO2%20emissions%20(Aggregate%20dataset%20(2021)).csv",
        "co2_output_dir": "s3://twdu-germany-pl-km/co2-output-parquet/",
    }

else:
    raise ValueError("""ENVIRONMENT must be "local" or "aws" only""")