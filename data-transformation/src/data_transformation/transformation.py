from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *

import pandas as pd

# ---------- Part II: Business Logic (for Part I, see data_transformation/config.py) ---------- #

class Transformer:
    """TWDU Data Transformer Python Class"""

    def __init__(self, spark: SparkSession, parameters: "dict[str, str]", boss_level: bool = True):
        self.spark = spark
        self.parameters = parameters
        self.boss_level = boss_level
        return

    def read_emissions(self) -> DataFrame:
        """
        Topics: cast, select, alias

        Read EmissionsByCountry.parquet into a Spark DataFrame.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - TotalEmissions: float
            - PerCapitaEmissions: float
            - ShareofGlobalEmissions: float
        """
        co2_df = self.spark.read.format("parquet").load(self.parameters["co2_input_path"])
        emissions_df = co2_df.select(
            F.col("Year"),
            F.col("Entity").alias("Country"),
            F.col("Annual_CO2_emissions").cast(FloatType()).alias("TotalEmissions"),
            F.col("Per_capita_CO2_emissions").cast(FloatType()).alias("PerCapitaEmissions"),
            F.col("Share_of_global_CO2_emissions").cast(FloatType()).alias("ShareofGlobalEmissions"),
        )
        return emissions_df

    def aggregate_global_emissions(emissions_df: DataFrame) -> DataFrame:
        """
        Topics: aggregate functions, alias

        Aggregate the total CO2 emissions globally on an ANNUAL basis.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - TotalEmissions: float
        """
        global_emissions = emissions_df.groupBy("Year").agg(
            F.sum(F.col("TotalEmissions")).cast(FloatType()).alias("TotalEmissions")
        )
        return global_emissions

    def aggregate_global_temperatures(self) -> DataFrame:
        """
        Topics: aggregate functions, alias

        Aggregate temperature measurements globally on an ANNUAL basis.
        Think carefully about the appropriate aggregation functions to use.
        For this project, you can just ignore any 'Uncertainty' columns.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - LandAverageTemperature: float
            - LandMaxTemperature: float
            - LandMinTemperature: float
            - LandAndOceanAverageTemperature: float
        """
        temps_global_df = self.spark.read.format("parquet").load(self.parameters["temperatures_global_input_path"])
        temps_global_df = temps_global_df.withColumn("Year", F.year(F.col("Date")))

        global_temperatures = temps_global_df.groupBy("Year").agg(
            F.avg("LandAverageTemperature").cast(FloatType()).alias("LandAverageTemperature"), # TODO: Exercise
            F.max("LandMaxTemperature").cast(FloatType()).alias("LandMaxTemperature"), # TODO: Exercise
            F.min("LandMinTemperature").cast(FloatType()).alias("LandMinTemperature"), # TODO: Exercise
            F.avg("LandAndOceanAverageTemperature").cast(FloatType()).alias("LandAndOceanAverageTemperature") # TODO: Exercise
        )
        return global_temperatures

    def join_global_emissions_temperatures(
        global_emissions: DataFrame, 
        global_temperatures: DataFrame) -> DataFrame:
        """
        Topics: joins

        Perform an INNER JOIN between the results of
            1. aggregate_global_emissions
            2. aggregate_global_temperatures
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - TotalEmissions: float
            - LandAverageTemperature: float
            - LandMaxTemperature: float
            - LandMinTemperature: float
            - LandAndOceanAverageTemperature: floats
        """
        global_emissions_temperatures = global_emissions.join(global_temperatures, on="Year", how="inner") # TODO: Exercise
        return global_emissions_temperatures

    def aggregate_country_temperatures(self) -> DataFrame:
        """
        Topics: casting, udf/pandas_udf, aggregation functions

        Aggregate temperature measurements per country on an ANNUAL basis.
        INVESTIGATE the data to look for any data quality issues
        Think carefully about:
            - any necessary cleaning (WARNING: don't assume Spark can intelligently read/cast everything)
            - the appropriate aggregation functions to use
        For this project, you can just ignore any 'Uncertainty' columns.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - AverageTemperature: float
        """
        temps_country_df = self.spark.read.format("parquet").load(self.parameters["temperatures_country_input_path"])
        
        year_expr = F.to_timestamp(F.col("Date"), format="MM-dd-yyyy") # TODO: Exercise
        country_expr = F.initcap(F.lower(F.trim(F.col("Country")))) # TODO: Exercise

        # HINT: anything wrapped with Lenny's face is a valid measurement, otherwise it's invalid.
        # There are multiple ways to tackle this: regexp_extract, regexp_replace, udf, pandas_udf, etc.
        # We recommend a pandas_udf as it's a nice transferrable skill with good performance.
        # For this, check out the pandas.Series.str family of methods.
        # FINAL HINT, regex=False will work just fine ;)

        # Declare the function and create the UDF
        def fix_temperature(temperatures: pd.Series) -> pd.Series:
            cleaned = temperatures.str.replace("( ͡° ͜ʖ ͡°)", "", regex=False) # TODO: exercise
            casted = pd.to_numeric(cleaned, errors="coerce") # let "coerce" handle the non-Lenny cases
            return casted

        fix_temperature_udf = F.pandas_udf(fix_temperature, returnType=FloatType())
        temperature_expr = fix_temperature_udf(F.col("AverageTemperature"))

        # TODO: exercise
        cleaned_df = temps_country_df.select(
            year_expr.alias("Year"),
            country_expr.alias("Country"),
            temperature_expr.alias("AverageTemperature")
        )
        country_temperatures = cleaned_df.groupBy("Year", "Country").agg( 
            F.avg(F.col("AverageTemperature")).cast(FloatType()).alias("AverageTemperature")
        )
        return country_temperatures

    def join_country_emissions_temperatures(
        country_emissions: DataFrame,
        country_temperatures: DataFrame) -> DataFrame:
        """
        Topics: joins

        Perform an INNER JOIN between the results of:
            1. read_emissions
            2. aggregate_country_temperatures
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - TotalEmissions: float
            - PerCapitaEmissions: float
            - ShareofGlobalEmissions: float
            - AverageTemperature: float
        """
        # HINT: don't forget a slight modification compared to the join_global_emissions_temperatures function
        # In the real world, you should always make sure that the country names have been standardized.
        # However, for our exercise, just assume that a no-match is truly no-match.
        country_emissions_temperatures = country_emissions.join(country_temperatures, on=["Year", "Country"], how="inner") # TODO: Exercise
        return country_emissions_temperatures


    def reshape_europe_big_three_emissions(emissions_df: DataFrame) -> DataFrame:
        """
        Topics: filter, pivot (with distinct values hinting) 

        Using the result of read_emissions(), filter for 1900 onwards only.
        Next, reshape the data to satisfy the requirements below.

        Your output Spark DataFrame's schema should be:
            - Year: integer
            - France_TotalEmissions: float
            - France_PerCapitaEmissions: float
            - Germany_TotalEmissions: float
            - Germany_PerCapitaEmissions: float
            - UnitedKingdom_TotalEmissions: float
            - UnitedKingdom_PerCapitaEmissions: float
        """
        # TODO: exercise
        modern_era_df = emissions_df.filter(F.col("Year") >= F.lit(1900)) 
        europe_big_three_df = modern_era_df \
            .groupBy("Year") \
            .pivot("Country", values=["France", "Germany", "United Kingdom"]) \
            .agg(
                F.first("TotalEmissions").alias("TotalEmissions"),
                F.first("PerCapitaEmissions").alias("PerCapitaEmissions")
                )
        return europe_big_three_df


    def boss_battle(emissions_df: DataFrame) -> DataFrame:
        """
        Topics: when (switch statements), udf/pandas_udf, Window functions, coalesce (filling nulls with a priority order)

        Your CO2 data provider informs you they suspect a massive bug in their calculations for every LEAP YEAR. 
        To be safe, you've been asked to prepare an alternative dataset.
        Using the result of read_emissions(), edit the estimates for leap years using the following priority:
            1. nearest non-null value before (i.e. 'forward fill')
            2. nearest non-null value after (i.e. 'backward fill')
            3. nullify the value for that year
        Then, reshape the data according to the schema requirements below.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - TotalEmissions: float
        """
        return

    def run(self):
        """
        BEFORE writing out any Spark DataFrame to S3:
            - coalesce to 1 partition
            - orderBy("year") 
        This is just for convenience in future analytics - not a massive deal :)
        """
        return