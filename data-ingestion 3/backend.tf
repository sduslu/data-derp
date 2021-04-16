terraform {
  backend "s3" {
    bucket = "twdu-germany-infra-state"
    key    = "data-ingestion"
    region = "eu-central-1"
    dynamodb_table = "terraform-lock"
  }
}

provider "aws" {
  region = "eu-central-1"
}

data "aws_caller_identity" "current" {}
