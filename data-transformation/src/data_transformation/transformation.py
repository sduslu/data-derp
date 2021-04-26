from pyspark.sql import DataFrame, Column
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

        # You'll notice that there's an Entity called "World".
        # Since we're analyzing emissions of countries, let's discard "World"
        country_emissions = co2_df \
            .filter(F.col("Entity") != F.lit("World")) \
            .select(
                F.col("Year"),
                F.col("Entity").alias("Country"),
                F.col("Annual_CO2_emissions").cast(FloatType()).alias("TotalEmissions"),
                F.col("Per_capita_CO2_emissions").cast(FloatType()).alias("PerCapitaEmissions"),
                F.col("Share_of_global_CO2_emissions").cast(FloatType()).alias("ShareofGlobalEmissions"),
            )
        return country_emissions

    @staticmethod # doesn't rely on self.spark nor self.parameters
    def aggregate_global_emissions(country_emissions: DataFrame) -> DataFrame:
        """
        Topics: aggregate functions, alias

        Aggregate the total CO2 emissions globally on an ANNUAL basis.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - TotalEmissions: float
        """
        global_emissions = country_emissions.groupBy("Year").agg(
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

    @staticmethod # doesn't rely on self.spark nor self.parameters
    def join_global_emissions_temperatures(
        global_emissions: DataFrame, 
        global_temperatures: DataFrame
        ) -> DataFrame:
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

    @staticmethod # doesn't rely on self.spark nor self.parameters
    def fix_country(col: Column) -> Column:
        """
        Use built-in Spark functions to clean-up the "Country" column
        e.g. "   cAnAdA " -> "Canada"
        """
        return F.initcap(F.lower(F.trim(col)))

    @staticmethod # doesn't rely on self.spark nor self.parameters    
    def fix_temperature(temperature: str) -> str:
        """
        HINT: only temperature entries with Lenny's face are valid measurements
        There are multiple ways to tackle this: udf, pandas_udf, regexp_extract, regexp_replace, etc.
        Normally, we'd recommend a pandas_udf as it's a nice transferrable skill with good performance.
        However, to keep this job simple, let's use a normal udf.

        The point is to demonstrate that you can write arbitrary Python logic as a UDF 
        if Spark doesn't have the built-in function you need.
        """
        return temperature.replace("( ͡° ͜ʖ ͡°)", "") # TODO: exercise

    def aggregate_country_temperatures(self) -> DataFrame:
        """
        Topics: casting, udf/pandas_udf, aggregation functions

        Aggregate temperature measurements per country on an ANNUAL basis.
        INVESTIGATE the data to look for any data quality issues
        Think carefully about:
            - any necessary cleaning (WARNING: don't assume Spark can intelligently read/cast everything)
            - the appropriate aggregation function to use
        For this project, you can just ignore any 'Uncertainty' columns.
        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - AverageTemperature: float
        """
        temps_country_df = self.spark.read.format("parquet").load(self.parameters["temperatures_country_input_path"])
        
        # Register your function as a UDF
        fix_temperature_udf = F.udf(self.fix_temperature, returnType=StringType())
        temperature_expr = fix_temperature_udf(F.col("AverageTemperature")).cast(FloatType())

        year_expr = F.year(F.to_timestamp(F.col("Date"), format="MM-dd-yyyy")) # TODO: Exercise
        country_expr = self.fix_country(F.col("Country")) # TODO: Exercise

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

    @staticmethod # doesn't rely on self.spark nor self.parameters
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

    @staticmethod # doesn't rely on self.spark nor self.parameters
    def reshape_europe_big_three_emissions(country_emissions: DataFrame) -> DataFrame:
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
        modern_era_df = country_emissions.filter(F.col("Year") >= F.lit(1900)) 
        europe_big_three_emissions = modern_era_df \
            .groupBy("Year") \
            .pivot("Country", values=["France", "Germany", "United Kingdom"]) \
            .agg(
                F.first("TotalEmissions").alias("TotalEmissions"),
                F.first("PerCapitaEmissions").alias("PerCapitaEmissions")
                )
        # You might've noticed that "United Kingdom" has a space. 
        # If you recall, spaces are not permitted in Apache Parquet column names. Let's address that:
        friendly_columns = [F.col(x).alias(x.replace(" ", "")) for x in europe_big_three_emissions.columns]
        europe_big_three_emissions = europe_big_three_emissions.select(friendly_columns)
        return europe_big_three_emissions

    @staticmethod # doesn't rely on self.spark nor self.parameters
    def boss_battle(country_emissions: DataFrame) -> DataFrame:
        """
        Topics: when (switch statements), udf/pandas_udf, Window functions, coalesce (filling nulls with a priority order)

        The CO2 data provider for Australia and New Zealand informs you that there's a massive bug in their estimations for every LEAP YEAR. 
        As a result, your team will have to produce an edited dataset for Australia and New Zealand only.
        Using the result of read_emissions(), edit the TotalEmissions estimates for leap years using the following priority:
            1. nearest non-null value before (i.e. 'forward fill')
            2. nearest non-null value after (i.e. 'backward fill')
            3. nullify the value
        Recap:
            - DISCARD all rows for countries other than Australia or New Zealand
            - KEEP rows from all years (including non-leap years) for Australia or New Zealand
            - KEEP only the following columns: Year, Country, and TotalEmissions

        Your output Spark DataFrame's schema should be:
            - Year: integer
            - Country: string
            - TotalEmissions: float
        """
        oceania_emissions = country_emissions.filter(F.col("Country").isin(["Australia", "New Zealand"]))

        # TODO: Exercise
        from calendar import isleap
        def check_leap(year: int) -> bool:
            return isleap(year)

        leap_year_udf = F.udf(check_leap, returnType=BooleanType())
        is_leap_year = leap_year_udf(F.col("Year"))

        # TODO: Exercise
        w_before = Window().partitionBy("Country").orderBy(F.col("Year").desc()).rowsBetween(Window.unboundedPreceding, -1)
        w_after = Window().partitionBy("Country").orderBy(F.col("Year")).rowsBetween(1, Window.unboundedFollowing)

        # TODO: Exercise
        nearest_before = F.first(F.col("TotalEmissions"), ignorenulls=True).over(w_before)
        nearest_after = F.first(F.col("TotalEmissions"), ignorenulls=True).over(w_after)

        emissions_prioritized = F.coalesce(
            nearest_before, # TODO: Exercise
            nearest_after, # TODO: Exercise
            F.lit(None)
        )
        emissions_case = F.when(is_leap_year, emissions_prioritized).otherwise(F.col("TotalEmissions"))
        emissions_expr = emissions_case.cast(FloatType())

        oceania_emissions_edited = oceania_emissions.select(
            "Year",
            "Country",
            emissions_expr.alias("TotalEmissions")
        )
        return oceania_emissions_edited

    def run(self):
        """
        BEFORE writing out any Spark DataFrame to S3:
            - coalesce to 1 partition
            - orderBy("Year") 
        This is just for convenience in our testing functions :)
        """

        # Task 1:
        country_emissions: DataFrame = self.read_emissions()
        global_emissions: DataFrame = self.aggregate_global_emissions(country_emissions)
        global_temperatures: DataFrame = self.aggregate_global_temperatures()
        global_emissions_temperatures: DataFrame = self.join_global_emissions_temperatures(
            global_emissions, 
            global_temperatures
            )
        global_emissions_temperatures.coalesce(1).orderBy("Year") \
            .write.format("parquet").mode("overwrite") \
            .save(self.parameters["co2_temperatures_global_output_path"])

        # Task 2:
        country_temperatures: DataFrame = self.aggregate_country_temperatures()
        country_emissions_temperatures: DataFrame = self.join_country_emissions_temperatures(
            country_emissions, 
            country_temperatures
        )
        country_emissions_temperatures.coalesce(1).orderBy("Year") \
            .write.format("parquet").mode("overwrite") \
            .save(self.parameters["co2_temperatures_country_output_path"])

        # Task 3:
        europe_big_three_emissions: DataFrame = self.reshape_europe_big_three_emissions(country_emissions)
        europe_big_three_emissions.coalesce(1).orderBy("Year") \
            .write.format("parquet").mode("overwrite") \
            .save(self.parameters["europe_big_3_co2_output_path"])

        # Task 4: 
        oceania_emissions_edited = self.boss_battle(country_emissions)
        oceania_emissions_edited.coalesce(1).orderBy("Year") \
            .write.format("parquet").mode("overwrite") \
            .save(self.parameters["co2_oceania_output_path"])

        # REVIEW: Knowing that all Spark transformations are lazy and always get recomputed,
        # do you see any opportunities for improvement in performance?
        return