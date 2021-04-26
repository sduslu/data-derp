import pytest
import sys
import os

from shutil import rmtree
from twdu_test_utils.pyspark import TestPySpark

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *

import pandas as pd
import numpy as np

from data_transformation.transformation import Transformer
from twdu_bonus import twdu_debug

class TestTransformation(TestPySpark):

    def setup_method(self): # runs before each and every test
        self.parameters = {
        "co2_input_path":                  "/workspaces/twdu-europe/twdu-datasets/transformation/inputs/EmissionsByCountry.parquet/",
        "temperatures_global_input_path":  "/workspaces/twdu-europe/twdu-datasets/transformation/inputs/GlobalTemperatures.parquet/",
        "temperatures_country_input_path": "/workspaces/twdu-europe/twdu-datasets/transformation/inputs/TemperaturesByCountry.parquet/",

        "co2_temperatures_global_output_path":  "/workspaces/twdu-europe/data-transformation/tmp/test/outputs/GlobalEmissionsVsTemperatures.parquet/",
        "co2_temperatures_country_output_path": "/workspaces/twdu-europe/data-transformation/tmp/test/outputs/CountryEmissionsVsTemperatures.parquet/",
        "europe_big_3_co2_output_path":         "/workspaces/twdu-europe/data-transformation/tmp/test/outputs/EuropeBigThreeEmissions.parquet/",
        "co2_oceania_output_path":              "/workspaces/twdu-europe/data-transformation/tmp/test/outputs/OceaniaEmissionsEdited.parquet/",
        }
        self.transformer = Transformer(self.spark, self.parameters)
        return


    def teardown_method(self):
        output_paths = self.parameters.values()
        for path in output_paths:
            if ("/tmp/" in path) and os.path.exists(path):
                rmtree(path.rsplit("/", 1)[0])

    def test_fix_country(self):
        original = pd.DataFrame({"Country": ["  gErMaNy ", "   uNiTeD sTaTeS    "]})
        spark_df = self.spark.createDataFrame(original)
        spark_df = spark_df.withColumn("Country", self.transformer.fix_country(F.col("Country")))
        fixed = spark_df.toPandas()
        try:
            result = sorted(fixed["Country"])
            assert result == ["Germany", "United States"]
        except Exception as e:
            raise type(e)(''.join(twdu_debug(original))) from e

    def test_run(self):
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
            assert (True if len(snappy_parquet_files) == 1 else False)
            assert (True if "_SUCCESS" in files else False)

if __name__ == '__main__':
    pytest.main(sys.argv)