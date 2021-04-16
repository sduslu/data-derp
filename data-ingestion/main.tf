module "glue-job" {
  source = "../terraform-modules/glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-ingestion"
  script-path = "ingestion.py"
  additional-params = {
    "--extra-py-files": "s3://${var.project-name}-${var.module-name}/ingestion_config.zip"
    }
}