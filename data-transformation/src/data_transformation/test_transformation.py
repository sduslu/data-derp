import unittest
from unittest.mock import Mock, patch
import os
from shutil import rmtree
from test_spark_helper import PySparkTest

import pandas as pd
import numpy as np

from config import download_twdu_dataset
from transformation import Transformer
from twdu_bonus import twdu_debug


class TestTransformation(PySparkTest):

    def setUp(self): # runs before each and every test
        self.parameters = {
        "co2_input_path":                  "/workspaces/twdu-germany/data-transformation/tmp/input-data/EmissionsByCountry.parquet",
        "temperatures_global_input_path":  "/workspaces/twdu-germany/data-transformation/tmp/input-data/GlobalTemperatures.parquet",
        "temperatures_country_input_path": "/workspaces/twdu-germany/data-transformation/tmp/input-data/TemperaturesByCountry.parquet",

        "temperatures_co2_global_output_path":  "/workspaces/twdu-germany/data-transformation/tmp/test/output-data/GlobalTemperaturesVsEmissions.parquet",
        "temperatures_co2_country_output_path": "/workspaces/twdu-germany/data-transformation/tmp/test/output-data/CountryTemperaturesVsEmissions.parquet",
        "europe_big_3_co2_output_path":         "/workspaces/twdu-germany/data-transformation/tmp/test/output-data/EuropeBigThreeEmissions.parquet",
        "co2_interpolated_output_path":         "/workspaces/twdu-germany/data-transformation/tmp/test/output-data/CountryEmissionsInterpolated.parquet",
        }
        self.transformer = Transformer(self.spark, self.parameters)
        return

    def tearDown(self): # runs after each and every test
        output_paths = [self.parameters[x] for x in [
            "temperatures_co2_global_output_path", 
            "temperatures_co2_country_output_path", 
            "europe_big_3_co2_output_path",
            "co2_interpolated_output_path"
            ]
        ]
        for path in output_paths:
            if os.path.exists(path):
                rmtree(path)

    def test_casing(self):
        original = pd.Series(["gErMaNy", "uNiTeD sTaTeS"])
        fix_casing = lambda x: x # TODO: import from Transformation module instead
        fixed = original.map(fix_casing)
        try:
            result = sorted(fixed.to_list())
            self.assertEqual(result, ["Germany", "United States"])
        except Exception as e:
            raise type(e)(''.join(twdu_debug(original))) from e

    def test_run(self):
        # Download the necessary datasets
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-pl-km/data-ingestion/EmissionsByCountry.parquet/", 
            destination=self.parameters["co2_input_path"],
            format="parquet")
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-pl-km/data-ingestion/GlobalTemperatures.parquet/", 
            destination=self.parameters["temperatures_global_input_path"],
            format="parquet")
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-pl-km/data-ingestion/TemperaturesByCountry.parquet/", 
            destination=self.parameters["temperatures_country_input_path"],
            format="parquet")

        # Run the job and check for _SUCCESS files for each partition
        self.transformer.run()
        output_paths = [self.parameters[x] for x in [
            "temperatures_co2_global_output_path", 
            "temperatures_co2_country_output_path", 
            "europe_big_3_co2_output_path",
            "co2_interpolated_output_path"
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