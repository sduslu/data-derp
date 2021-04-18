import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import pandas as pd

# ---------- Part II: Business Logic (for Part I, see data_ingestion/config.py) ---------- #

class Ingestion:
    """TWDU Ingestion Python Class"""

    def __init__(self, spark, parameters):
        self.spark = spark
        self.parameters = parameters
        self.replace_spaces_with_underscores = lambda x: x.lower().replace(" ", "_")

    def run(self):
        self.write_co2(self.read_co2())

    def read_co2(self):
        """NOTE: to read from S3 straight from pd.read_csv, make sure you've already pip installed s3fs"""
        co2_df = pd.read_csv(self.parameters["co2_uri"])
        co2_df.columns = [self.replace_spaces_with_underscores(x) for x in co2_df.columns]
        return co2_df

    def write_co2(self, df):
        co2_spark = self.spark.createDataFrame(df).select(*df.columns[:3]).limit(1000)
        co2_spark.write.format("parquet").save(path=self.parameters["co2_output_dir"], mode="overwrite")