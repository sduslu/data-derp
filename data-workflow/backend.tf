terraform {
  backend "s3" {
    key    = "data-workflow"
    region = "eu-central-1"
    dynamodb_table = "terraform-lock"
  }
}

provider "aws" {
  region = "eu-central-1"
}