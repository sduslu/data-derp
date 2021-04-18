import unittest
from unittest.mock import Mock, patch
import os
from test_spark_helper import PySparkTest

import pandas as pd
import numpy as np

from ingestion import Ingestion


class TestIngestion(PySparkTest):

    def setUp(self):
        self.parameters = {
            'co2_uri': 'https://some-co2-uri.com',
            'co2_output_dir': './data-ingestion/tmp'
        }
        self.ingestion = Ingestion(self.spark, self.parameters)

    def tearDown(self):
        output_dir = self.parameters["co2_output_dir"]
        if os.path.exists(output_dir):
            for file in os.listdir(output_dir):
                os.remove(output_dir + "/" + file)
            os.rmdir(output_dir)

    def test_replace_spaces_with_underscores(self):
        df = pd.DataFrame(
            {
                'My Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Another Awesome Column': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
            }
        )
        df.columns = [self.ingestion.replace_spaces_with_underscores(x) for x in df.columns]
        self.assertEqual(sorted(df.columns), sorted(["my_awesome_column", "another_awesome_column"]))


    @patch('pandas.read_csv')
    def test_read(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                'Some Country': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Some Year': pd.Series(["1900", "1901", "1902", "1903"])
             }
        )

        result = self.ingestion.read_co2()
        self.assertEqual(sorted(result.columns.tolist()), sorted(["some_country", "some_year"]))
        self.assertEqual(result["some_country"].tolist(), ["Germany", "New Zealand", "Australia", "UK"])
        self.assertEqual(result["some_year"].tolist(), ["1900", "1901", "1902", "1903"])

    def test_write(self):
        df = pd.DataFrame(
            {
                'some_country': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                'some_year': pd.Series(["1900", "1901", "1902", "1903"])
            }
        )
        result = self.ingestion.write_co2(df)

        files = os.listdir(self.parameters["co2_output_dir"])

        self.assertTrue(True if "_SUCCESS" in files else False)
        # self.assertEqual(len(files), 4) # by default, 4 parquet partitions are written. but it's not a real requirement 

    @patch('pandas.read_csv')
    def test_run(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                'Some Country': pd.Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Some Year': pd.Series(["1900", "1901", "1902", "1903"])
             }
        )

        result = self.ingestion.run()
        files = os.listdir(self.parameters["co2_output_dir"])

        self.assertTrue(True if "_SUCCESS" in files else False)
        # self.assertEqual(len(files), 4) # by default, 4 parquet partitions are written. but it's not a real requirement 

if __name__ == '__main__':
    unittest.main()