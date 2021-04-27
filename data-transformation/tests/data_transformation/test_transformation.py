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
from twdu_transformation_expected import get_expected_metadata

class TestTransformation(TestPySpark):

    @classmethod
    def setup_class(cls): # runs before each and every test
        cls.spark = cls.start_spark()
        root_dir = os.path.dirname(os.path.realpath(__file__)).split("/data-transformation/")[0]
        cls.parameters = {
        "co2_input_path":                  f"{root_dir}/twdu-datasets/transformation/inputs/EmissionsByCountry.parquet/",
        "temperatures_global_input_path":  f"{root_dir}/twdu-datasets/transformation/inputs/GlobalTemperatures.parquet/",
        "temperatures_country_input_path": f"{root_dir}/twdu-datasets/transformation/inputs/TemperaturesByCountry.parquet/",

        "co2_temperatures_global_output_path":  f"{root_dir}/data-transformation/tmp/test/outputs/GlobalEmissionsVsTemperatures.parquet/",
        "co2_temperatures_country_output_path": f"{root_dir}/data-transformation/tmp/test/outputs/CountryEmissionsVsTemperatures.parquet/",
        "europe_big_3_co2_output_path":         f"{root_dir}/data-transformation/tmp/test/outputs/EuropeBigThreeEmissions.parquet/",
        "co2_oceania_output_path":              f"{root_dir}/data-transformation/tmp/test/outputs/OceaniaEmissionsEdited.parquet/",
        }
        cls.transformer = Transformer(cls.spark, cls.parameters)
        return

    @classmethod
    def teardown_class(cls):
        cls.stop_spark()
        output_paths = cls.parameters.values()
        for path in output_paths:
            if ("/tmp/" in path) and os.path.exists(path):
                rmtree(path.rsplit("/", 1)[0])
        

    def test_fix_country(self):
        original = pd.Series(["  gErMaNy ", "   uNiTeD sTaTeS    "])
        spark_df = self.spark.createDataFrame(pd.DataFrame({"Country": original}))
        spark_df = spark_df.withColumn("Country", self.transformer.fix_country(F.col("Country")))
        fixed = spark_df.toPandas()
        try:
            result = sorted(fixed["Country"])
            assert result == ["Germany", "United States"]
        except Exception as e:
            raise type(e)(''.join(twdu_debug(original))) from e

    def test_remove_lenny_face(self):
        """
        Objective: Convert an incoming string into a format that can be casted to a FloatType by Spark.
        Spark is smart enough to convert "69.420" to 69.420 but <69.420> will be casted to a null.
        To keep the exercise simple (no regex required), you'll only need to handle the Lenny face.
        """
        original = pd.Series(["( ͡° ͜ʖ ͡°)4.384( ͡° ͜ʖ ͡°)", "#", "?", "-", "( ͡° ͜ʖ ͡°)1.53( ͡° ͜ʖ ͡°)"])
        result = original.map(self.transformer.remove_lenny_face)
        try:
            assert result.to_list() == ["4.384", "#", "?", "-", "1.53"]
        except Exception as e:
            raise type(e)(''.join(twdu_debug(original))) from e

    def test_run(self):
        """High level job test: count + schema checks but nothing more granular"""
        # Run the job and check for _SUCCESS files for each partition
        self.transformer.run()

        output_path_keys = [
            "co2_temperatures_global_output_path", 
            "co2_temperatures_country_output_path", 
            "europe_big_3_co2_output_path",
            "co2_oceania_output_path"
            ]
        output_path_values = [self.parameters[k] for k in output_path_keys]
        expected_metadata_dict = get_expected_metadata()
        expected_metadata = [expected_metadata_dict[k.replace("_path", "")] for k in output_path_keys]

        for (path, expected) in list(zip(output_path_values, expected_metadata)):
            files = os.listdir(path)
            snappy_parquet_files = [x for x in files if x.endswith(".snappy.parquet")]
            # For this exercise, we require you to control each table's partitioning to 1 parquet partition
            assert (True if len(snappy_parquet_files) == 1 else False)
            assert (True if "_SUCCESS" in files else False)

            # Check count and schema - this covers most of pyspark-test's (https://pypi.org/project/pyspark-test/) functionality already
            # No need for a full equality check (it collects everything into the driver's memory - too time/memory consuming)
            df = self.spark.read.parquet(path)
            assert df.count() == expected["count"]
            assert df.schema == expected["schema"]

if __name__ == '__main__':
    pytest.main(sys.argv)