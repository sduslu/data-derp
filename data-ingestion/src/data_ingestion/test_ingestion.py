import unittest
from unittest.mock import Mock, patch
import os
from shutil import rmtree
from test_spark_helper import PySparkTest

import pandas as pd
import numpy as np

from config import download_twdu_dataset
from ingestion import Ingester


class TestIngestion(PySparkTest):

    def setUp(self): # runs before each and every test
        self.parameters = {
            "temperatures_country_input_path":  "/workspaces/twdu-germany/data-ingestion/tmp/input-data/TemperaturesByCountry.csv",
            "temperatures_country_output_path": "/workspaces/twdu-germany/data-ingestion/tmp/test/output-data/TemperaturesByCountry.parquet",
            "temperatures_global_input_path":   "/workspaces/twdu-germany/data-ingestion/tmp/input-data/GlobalTemperatures.csv",
            "temperatures_global_output_path":  "/workspaces/twdu-germany/data-ingestion/tmp/test/output-data/GlobalTemperatures.parquet",
            "co2_input_path":                   "/workspaces/twdu-germany/data-ingestion/tmp/input-data/EmissionsByCountry.csv",
            "co2_output_path":                  "/workspaces/twdu-germany/data-ingestion/tmp/test/output-data/EmissionsByCountry.parquet",
        }
        self.ingester = Ingester(self.spark, self.parameters)
        return

    def tearDown(self): # runs after each and every test
        output_paths = [self.parameters[x] for x in ["temperatures_country_output_path", "temperatures_global_output_path", "co2_output_path"]]
        for path in output_paths:
            if os.path.exists(path):
                rmtree(path)

    def test_replace_invalid_chars(self):
        # BEWARE: dictionaries in Python do not necessarily enforce order. 
        # To check column names, always use sorted()
        INVALID_CHARS = [" ", ",", ";", "\n", "\t", "=", "-", "{", "}", "(", ")"] 
        df = pd.DataFrame(
            {
                'My Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Another Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
            }
        )
        df.columns = [self.ingester.replace_invalid_chars(x) for x in df.columns]

        all_columns_valid = True
        for column in df.columns:
            if not all_columns_valid:
                break
            for char in INVALID_CHARS:
                if char in column:
                    all_columns_valid = False
                    break
        self.assertTrue(True if all_columns_valid else False)
        self.assertEqual(sorted(df.columns), sorted(["My_Awesome_Column", "Another_Awesome_Column"]))

    def test_run(self):
        # Download the necessary datasets
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-data-source/TemperaturesByCountry.csv", 
            destination=self.parameters["temperatures_country_input_path"])
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-data-source/GlobalTemperatures.csv", 
            destination=self.parameters["temperatures_global_input_path"])
        download_twdu_dataset(
            s3_uri="s3://twdu-germany-data-source/EmissionsByCountry.csv", 
            destination=self.parameters["co2_input_path"])

        # Run the job and check for _SUCCESS files for each partition
        self.ingester.run()
        output_paths = [self.parameters[x] for x in ["temperatures_country_output_path", "temperatures_global_output_path", "co2_output_path"]]
        for path in output_paths:
            files = os.listdir(path)
            snappy_parquet_files = [x for x in files if x.endswith(".snappy.parquet")]
            # For this exercise, we require you to control each table's partitioning to 1 parquet partition
            self.assertTrue(True if len(snappy_parquet_files) == 1 else False)
            self.assertTrue(True if "_SUCCESS" in files else False)

if __name__ == '__main__':
    unittest.main()