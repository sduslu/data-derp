module "glue-job" {
  source = "../terraform-modules/glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-ingestion"
  script-path = "main.py"
  additional-params = {
    "--extra-py-files": "s3://${var.project-name}-${var.module-name}/ingestion_config.zip,s3://${var.project-name}-${var.module-name}/data_ingestion-0.1-py3-none-any.whl"
    }
}