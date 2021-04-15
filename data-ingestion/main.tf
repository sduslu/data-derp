resource "aws_glue_job" "this" {
  name     = "${var.project-name}-${var.module-name}"
  role_arn = aws_iam_role.this.arn

  command {
    script_location = "s3://${module.s3-bucket.bucket-name}/main.py"
  }

  default_arguments = {
    "--job-language" = "python"
  }
}

resource "aws_iam_role" "this" {
  name = "${var.project-name}-${var.module-name}-lambda-execution"
  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/twdu-germany/twdu-germany-delegated-boundary"
  force_detach_policies = true
  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "glue.amazonaws.com"
        },
        "Effect": "Allow",
        "Sid": ""
      }
    ]
}
EOF
}

module "s3-bucket" {
  source = "../terraform-modules/s3-bucket"

  bucket-name = "${var.project-name}-${var.module-name}-data-ingestion"
  force-destroy = true
}