module "data-workflow" {
  source = "../terraform-modules/glue-workflow"

  project-name = var.project-name
  module-name = var.module-name
}