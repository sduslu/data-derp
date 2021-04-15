module "s3-bucket" {
  source = "../terraform-modules/s3-bucket"

  bucket-name = "${var.project-name}-${var.module-name}"
  force-destroy = true

  // Remove this later!
  bucket-acl = "public-read"
  private = false
}