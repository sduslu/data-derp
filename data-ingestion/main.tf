resource "aws_glue_job" "this" {
  name     = "${var.project-name}-${var.module-name}-data-ingestion"
  role_arn = aws_iam_role.this.arn

  number_of_workers = 2
  worker_type = "Standard"
  glue_version = "2.0"

  command {
    script_location = "s3://${data.aws_s3_bucket.this.bucket}/ingestion.py"
  }

  default_arguments = {
    "--job-language" = "python"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.this.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = ""
    "--TempDir"                          = "s3://${data.aws_s3_bucket.this.bucket}/data-ingestion/temp/"
    '--enable-spark-ui'                  =  'true',
    '--spark-event-logs-path'             = 's3://${data.aws_s3_bucket.this.bucket}/data-ingestion/spark-event-logs-path/'

  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "${var.project-name}-${var.module-name}-data-ingestion/glue"
  retention_in_days = 14
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

resource "aws_iam_role_policy_attachment" "glue-service-role" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3-full-access" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
