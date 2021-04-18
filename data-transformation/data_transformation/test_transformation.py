import unittest
from unittest.mock import Mock, patch
import pandas as pd
from pandas import Series
import numpy as np
from test_spark_helper import PySparkTest
import os

from transformation import Transformation


class TestTransformation(PySparkTest):

    def setUp(self):
        self.parameters = {
            'co2_uri': 'https://some-co2-uri.com',
            'co2_output_dir': './data-transformation/tmp'
        }
        self.transformation = Transformation(self.spark, self.parameters)

    def tearDown(self):
        output_dir = self.parameters["co2_output_dir"]
        if os.path.exists(output_dir):
            for file in os.listdir(output_dir):
                os.remove(output_dir + "/" + file)
            os.rmdir(output_dir)

    @patch('pandas.read_csv')
    def test_read_co2_global(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                'Some Country': Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Some Year': Series(["1900", "1901", "1902", "1903"])
             }
        )

        result = self.transformation.read_co2_global()
        self.assertEqual(result.columns.tolist(), ["Some Country", "Some Year"] )
        self.assertEqual(result["some_country"].tolist(), ["Germany", "New Zealand", "Australia", "UK"])
        self.assertEqual(result["some_year"].tolist(), ["1900", "1901", "1902", "1903"])

    @patch('pandas.read_csv')
    def test_transform(self, mock_read_csv):
        mock_read_csv.return_value = pd.DataFrame(
            {
                'Some Country': Series(["Germany", "New Zealand", "Australia", "UK"]),
                'Some Year': Series(["1900", "1901", "1902", "1903"])
             }
        )

        result = self.transformation.transform()
        files = os.listdir(self.parameters["co2_output_dir"])

        self.assertTrue(True if "_SUCCESS" in files else False)
        self.assertEqual(len(files), 4)
