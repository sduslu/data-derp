import unittest
from unittest.mock import Mock, patch
import os
from shutil import rmtree

from pyspark.sql import SparkSession
from test_spark_helper import PySparkTest

import pyspark.sql.functions as F
from pyspark.sql.types import *

import pandas as pd
import numpy as np

from app.data_transformation.config import download_twdu_dataset
from app.data_transformation.transformation import Transformer
from app.data_transformation.twdu_bonus import twdu_debug


class TestTransformation(PySparkTest):

    def setUp(self): # runs before each and every test
        self.parameters = {
        "co2_input_path":                  "/workspaces/twdu-europe/data-transformation/tmp/input-data/EmissionsByCountry.parquet/",
        "temperatures_global_input_path":  "/workspaces/twdu-europe/data-transformation/tmp/input-data/GlobalTemperatures.parquet/",
        "temperatures_country_input_path": "/workspaces/twdu-europe/data-transformation/tmp/input-data/TemperaturesByCountry.parquet/",

        "co2_temperatures_global_output_path":  "/workspaces/twdu-europe/data-transformation/tmp/test/output-data/GlobalEmissionsVsTemperatures.parquet/",
        "co2_temperatures_country_output_path": "/workspaces/twdu-europe/data-transformation/tmp/test/output-data/CountryEmissionsVsTemperatures.parquet/",
        "europe_big_3_co2_output_path":         "/workspaces/twdu-europe/data-transformation/tmp/test/output-data/EuropeBigThreeEmissions.parquet/",
        "co2_oceania_output_path":              "/workspaces/twdu-europe/data-transformation/tmp/test/output-data/OceaniaEmissionsEdited.parquet/",
        }
        self.transformer = Transformer(self.spark, self.parameters)
        return

    def tearDown(self): # runs after each and every test
        output_paths = [self.parameters[x] for x in [
            "co2_temperatures_global_output_path", 
            "co2_temperatures_country_output_path", 
            "europe_big_3_co2_output_path",
            "co2_oceania_output_path"
            ]
        ]
        for path in output_paths:
            if os.path.exists(path):
                rmtree(path)

    def test_fix_country(self):
        original = pd.DataFrame({"Country": ["  gErMaNy ", "   uNiTeD sTaTeS    "]})
        spark_df = self.spark.createDataFrame(original)
        spark_df = spark_df.withColumn("Country", self.transformer.fix_country(F.col("Country")))
        fixed = spark_df.toPandas()
        try:
            result = sorted(fixed["Country"])
            self.assertEqual(result, ["Germany", "United States"])
        except Exception as e:
            raise type(e)(''.join(twdu_debug(original))) from e

    def test_run(self):
        # Download the necessary datasets
        download_twdu_dataset(
            s3_uri="s3://twdu-europe-team-pl-km/data-ingestion/EmissionsByCountry.parquet/",
            destination=self.parameters["co2_input_path"])
        download_twdu_dataset(
            s3_uri="s3://twdu-europe-team-pl-km/data-ingestion/GlobalTemperatures.parquet/",
            destination=self.parameters["temperatures_global_input_path"])
        download_twdu_dataset(
            s3_uri="s3://twdu-europe-team-pl-km/data-ingestion/TemperaturesByCountry.parquet/",
            destination=self.parameters["temperatures_country_input_path"])

        # Run the job and check for _SUCCESS files for each partition
        self.transformer.run()
        output_paths = [self.parameters[x] for x in [
            "co2_temperatures_global_output_path", 
            "co2_temperatures_country_output_path", 
            "europe_big_3_co2_output_path",
            "co2_oceania_output_path"
            ]
        ]

        for path in output_paths:
            files = os.listdir(path)
            snappy_parquet_files = [x for x in files if x.endswith(".snappy.parquet")]
            # For this exercise, we require you to control each table's partitioning to 1 parquet partition
            self.assertTrue(True if len(snappy_parquet_files) == 1 else False)
            self.assertTrue(True if "_SUCCESS" in files else False)

if __name__ == '__main__':
    unittest.main()