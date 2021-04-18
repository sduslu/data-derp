from pyspark.sql.types import *

import pandas as pd

class Transformation:
    def __init__(self, spark, parameters):
        self.spark = spark
        self.parameters = parameters

    def transform(self):
        return self.read_co2_global()

    def read_co2_global(self):
        return pd.read_csv(self.parameters["co2_uri"])