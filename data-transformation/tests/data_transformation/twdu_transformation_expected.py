from pyspark.sql.types import *

def get_expected_metadata():

    co2_temperatures_global_expected_count = 266
    co2_temperatures_global_expected_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("TotalEmissions", FloatType(), True),
        StructField("LandAverageTemperature", FloatType(), True),
        StructField("LandMaxTemperature", FloatType(), True),
        StructField("LandMinTemperature", FloatType(), True),
        StructField("LandAndOceanAverageTemperature", FloatType(), True)
        ]
    )

    co2_temperatures_country_expected_count = 18529
    co2_temperatures_country_expected_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Country", StringType(), True),
        StructField("TotalEmissions", FloatType(), True),
        StructField("PerCapitaEmissions", FloatType(), True),
        StructField("ShareOfGlobalEmissions", FloatType(), True),
        StructField("AverageTemperature", FloatType(), True)
        ]
    )

    europe_big_3_co2_expected_count = 120
    europe_big_3_co2_expected_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("France_TotalEmissions", FloatType(), True),
        StructField("France_PerCapitaEmissions", FloatType(), True),
        StructField("Germany_TotalEmissions", FloatType(), True),
        StructField("Germany_PerCapitaEmissions", FloatType(), True),
        StructField("UnitedKingdom_TotalEmissions", FloatType(), True),
        StructField("UnitedKingdom_PerCapitaEmissions", FloatType(), True)
        ]
    )

    co2_oceania_expected_count = 302
    co2_oceania_expected_schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Country", StringType(), True),
        StructField("TotalEmissions", FloatType(), True)
        ]
    )

    expected_output_metadata = {
        "co2_temperatures_global_output": {"count": co2_temperatures_global_expected_count, "schema": co2_temperatures_global_expected_schema},
        "co2_temperatures_country_output": {"count": co2_temperatures_country_expected_count, "schema": co2_temperatures_country_expected_schema},
        "europe_big_3_co2_output": {"count": europe_big_3_co2_expected_count, "schema": europe_big_3_co2_expected_schema},
        "co2_oceania_output": {"count": co2_oceania_expected_count, "schema": co2_oceania_expected_schema}
    }
    
    return expected_output_metadata
