terraform {
  backend "s3" {
    key    = "base"
    region = "eu-central-1"
    dynamodb_table = "terraform-lock"
  }
}

provider "aws" {
  region = "eu-central-1"
}

data "aws_caller_identity" "current" {}
