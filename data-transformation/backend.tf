terraform {
  backend "s3" {
    key    = "data-transformation"
    region = "eu-central-1"
    dynamodb_table = "terraform-lock"
  }
}

provider "aws" {
  region = "eu-central-1"
}

data "aws_caller_identity" "current" {}
