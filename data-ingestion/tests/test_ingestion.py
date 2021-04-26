import pytest
import sys
import os

from shutil import rmtree
from twdu_test_utils.pyspark import TestPySpark

import pandas as pd
import numpy as np

from data_ingestion.ingestion import Ingester

class TestIngestion(TestPySpark):

    def setup_method(self):
        self.parameters = {
            "co2_input_path":                   "/workspaces/twdu-europe/twdu-datasets/ingestion/inputs/EmissionsByCountry.csv",
            "temperatures_global_input_path":   "/workspaces/twdu-europe/twdu-datasets/ingestion/inputs/GlobalTemperatures.csv",
            "temperatures_country_input_path":  "/workspaces/twdu-europe/twdu-datasets/ingestion/inputs/TemperaturesByCountry.csv",

            "co2_output_path":                  "/workspaces/twdu-europe/data-ingestion/tmp/test/outputs/EmissionsByCountry.parquet/",
            "temperatures_global_output_path":  "/workspaces/twdu-europe/data-ingestion/tmp/test/outputs/GlobalTemperatures.parquet/",
            "temperatures_country_output_path": "/workspaces/twdu-europe/data-ingestion/tmp/test/outputs/TemperaturesByCountry.parquet/",
        }
        self.ingester = Ingester(self.spark, self.parameters)
        return

    def teardown_method(self):
        output_paths = self.parameters.values()
        for path in output_paths:
            if ("/tmp/" in path) and os.path.exists(path):
                rmtree(path.rsplit("/", 1)[0])

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
        assert all_columns_valid
        assert sorted(df.columns) == sorted(["My_Awesome_Column", "Another_Awesome_Column"])

    def test_run(self):
        # Run the job and check for _SUCCESS files for each partition
        self.ingester.run()
        output_paths = [self.parameters[x] for x in ["temperatures_country_output_path", "temperatures_global_output_path", "co2_output_path"]]
        for path in output_paths:
            files = os.listdir(path)
            snappy_parquet_files = [x for x in files if x.endswith(".snappy.parquet")]
            # For this exercise, we require you to control each table's partitioning to 1 parquet partition
            assert (True if len(snappy_parquet_files) == 1 else False)
            assert (True if "_SUCCESS" in files else False)

if __name__ == '__main__':
    pytest.main(sys.argv)