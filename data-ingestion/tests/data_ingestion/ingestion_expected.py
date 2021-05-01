from pyspark.sql.types import *

def get_expected_metadata():

    country_temps_expected_count = 577462
    country_temps_expected_schema = StructType([
        StructField("Date", StringType(), True),
        StructField("AverageTemperature", StringType(), True),
        StructField("AverageTemperatureUncertainty", StringType(), True),
        StructField("Country", StringType(), True)
        ]
    )

    global_temps_expected_count = 3192
    global_temps_expected_schema = StructType([
        StructField("Date", TimestampType(), True),
        StructField("LandAverageTemperature", DoubleType(), True),
        StructField("LandAverageTemperatureUncertainty", DoubleType(), True),
        StructField("LandMaxTemperature", DoubleType(), True),
        StructField("LandMaxTemperatureUncertainty", DoubleType(), True),
        StructField("LandMinTemperature", DoubleType(), True),
        StructField("LandMinTemperatureUncertainty", DoubleType(), True),
        StructField("LandAndOceanAverageTemperature", DoubleType(), True),
        StructField("LandAndOceanAverageTemperatureUncertainty", DoubleType(), True)
        ]
    )

    co2_expected_count = 23645
    co2_expected_schema = StructType([
        StructField("Entity", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Annual_CO2_emissions", DoubleType(), True),
        StructField("CO2_emissions_from_coal", DoubleType(), True),
        StructField("CO2_emissions_from_oil", DoubleType(), True),
        StructField("CO2_emissions_from_gas", DoubleType(), True),
        StructField("CO2_emissions_from_cement", DoubleType(), True),
        StructField("CO2_emissions_from_flaring", DoubleType(), True),
        StructField("CO2_emissions_from_other_industry", DoubleType(), True),
        StructField("CO2_emissions_from_bunkers", DoubleType(), True),
        StructField("Annual_CO2_growth_%", DoubleType(), True),
        StructField("Annual_CO2_growth_abs", DoubleType(), True),
        StructField("Annual_consumption_based_CO2_emissions", DoubleType(), True),
        StructField("CO2_emissions_embedded_in_trade", DoubleType(), True),
        StructField("Share_of_CO2_emissions_embedded_in_trade", DoubleType(), True),
        StructField("Per_capita_CO2_emissions", DoubleType(), True),
        StructField("Coal_emissions_per_capita", DoubleType(), True),
        StructField("Oil_emissions_per_capita", DoubleType(), True),
        StructField("Gas_emissions_per_capita", DoubleType(), True),
        StructField("Flaring_emissions_per_capita", DoubleType(), True),
        StructField("Cement_emissions_per_capita", DoubleType(), True),
        StructField("Other_emissions_per_capita", DoubleType(), True),
        StructField("Per_capita_consumption_based_CO2_emissions", DoubleType(), True),
        StructField("Emissions_embedded_in_trade_per_capita", DoubleType(), True),
        StructField("Share_of_global_CO2_emissions", DoubleType(), True),
        StructField("Share_of_global_coal_emissions", DoubleType(), True),
        StructField("Share_of_global_oil_emissions", DoubleType(), True),
        StructField("Share_of_global_gas_emissions", DoubleType(), True),
        StructField("Share_of_global_flaring_emissions", DoubleType(), True),
        StructField("Share_of_global_cement_emissions", DoubleType(), True),
        StructField("Cumulative_CO2_emissions", DoubleType(), True),
        StructField("Cumulative_coal_emissions", DoubleType(), True),
        StructField("Cumulative_oil_emissions", DoubleType(), True),
        StructField("Cumulative_gas_emissions", DoubleType(), True),
        StructField("Cumulative_flaring_emissions", DoubleType(), True),
        StructField("Cumulative_cement_emissions", DoubleType(), True),
        StructField("Cumulative_other_industry_emissions", DoubleType(), True),
        StructField("Share_of_global_cumulative_CO2_emissions", DoubleType(), True),
        StructField("CO2_per_GDP_kg_per_$PPP", DoubleType(), True),
        StructField("Consumption_based_CO2_per_GDP_kg_per_$PPP", DoubleType(), True),
        StructField("CO2_per_unit_energy_kgCO2_per_kilowatt_hour", DoubleType(), True),
        StructField("Share_of_global_cumulative_coal_emissions", DoubleType(), True),
        StructField("Share_of_global_cumulative_oil_emissions", DoubleType(), True),
        StructField("Share_of_global_cumulative_gas_emissions", DoubleType(), True),
        StructField("Share_of_global_cumulative_flaring_emissions", DoubleType(), True),
        StructField("Share_of_global_cumulative_cement_emissions", DoubleType(), True)
        ]
    )

    expected_output_metadata = {
        "co2_output": {"count": co2_expected_count, "schema": co2_expected_schema},
        "temperatures_global_output": {"count": global_temps_expected_count, "schema": global_temps_expected_schema},
        "temperatures_country_output": {"count": country_temps_expected_count, "schema": country_temps_expected_schema}
    }
    return expected_output_metadata