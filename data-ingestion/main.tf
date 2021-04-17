module "glue-job" {
  source = "../terraform-modules/glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-ingestion"
  script-path = "main.py"
  additional-params = {
    "--additional-python-modules": "s3://${var.project-name}-${var.module-name}/data_ingestion-0.1-py3.egg", # https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
    "--extra-py-files": "s3://${var.project-name}-${var.module-name}/data_ingestion-0.1-py3.egg", # https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html
  }
}