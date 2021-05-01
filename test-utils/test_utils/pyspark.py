import pytest
import logging
import sys

from pyspark.sql import SparkSession


class TestPySpark:
    """Base Class for PySpark testing and SparkSession management"""

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
    def start_spark(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        if not sys.warnoptions:
            import warnings
            warnings.simplefilter("ignore")
        return cls.spark

    @classmethod
    def stop_spark(cls):
        cls.spark.stop()

if __name__ == '__main__':
    pytest.main(sys.argv)