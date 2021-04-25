module "glue-job" {
  source = "../terraform-modules/glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-transformation"
  script-path = "data-transformation/main.py"
  additional-params = {
    "--extra-py-files":                       "s3://${var.project-name}-${var.module-name}/data-transformation/transformation-0.1-py3.egg", # https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
    
    "--co2_input_path":                       "s3://${var.project-name}-${var.module-name}/data-ingestion/EmissionsByCountry.parquet",
    "--temperatures_global_input_path":       "s3://${var.project-name}-${var.module-name}/data-ingestion/GlobalTemperatures.parquet",
    "--temperatures_country_input_path":      "s3://${var.project-name}-${var.module-name}/data-ingestion/TemperaturesByCountry.parquet",
    
    "--temperatures_co2_global_output_path":  "s3://${var.project-name}-${var.module-name}/data-transformation/GlobalTemperaturesVsEmissions.parquet",
    "--temperatures_co2_country_output_path": "s3://${var.project-name}-${var.module-name}/data-transformation/CountryTemperaturesVsEmissions.parquet",
    "--europe_big_3_co2_output_path":         "s3://${var.project-name}-${var.module-name}/data-transformation/EuropeBigThreeEmissions.parquet",
    "co2_interpolated_output_path":           "s3://${var.project-name}-${var.module-name}/data-transformation/CountryEmissionsInterpolated.parquet",
  }
}