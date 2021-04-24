import unittest
import logging
from pyspark.sql import SparkSession

import sys
from time import sleep

class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
            .master('local')
            .appName('testing')
            .enableHiveSupport()
            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()