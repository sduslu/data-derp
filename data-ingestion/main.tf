module "glue-job" {
  source = "../terraform-modules/glue-job"

  project-name = var.project-name
  module-name = var.module-name
  submodule-name = "data-ingestion"
  script-path = "ingestion.py"
}