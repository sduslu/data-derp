import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import pandas as pd

# ---------- Part II: Business Logic (for Part I, see data_transformation/config.py) ---------- #

class Transformer:
    """TWDU Data Transformer Python Class"""

    def __init__(self, spark, parameters):
        self.spark = spark
        self.parameters = parameters
        return

    def run(self):
        return