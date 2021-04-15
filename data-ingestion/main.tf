resource "aws_glue_job" "this" {
  name     = "${var.project-name}-${var.module-name}-data-ingestion"
  role_arn = aws_iam_role.this.arn

  command {
    script_location = "s3://${data.aws_s3_bucket.this.bucket}/data-ingestion-etl/main.py"
  }

  default_arguments = {
    "--job-language" = "python"
  }
}

data "aws_s3_bucket" "this" {
  bucket = "${var.project-name}-${var.module-name}"
}

resource "aws_iam_role" "this" {
  name = "${var.project-name}-${var.module-name}-data-ingestion"
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