import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import pandas as pd

# ---------- Part II: Business Logic (for Part I, see twdu_ingestion/config.py) ---------- #

class Transformation:
    def __init__(self, spark, parameters):
        self.spark = spark
        self.parameters = parameters

    def run(self):
        return