terraform {
  backend "s3" {
    key    = "data-ingestion"
    region = "eu-central-1"
  }
}

provider "aws" {
  region = "eu-central-1"
}

data "aws_caller_identity" "current" {}
