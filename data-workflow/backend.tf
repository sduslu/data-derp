terraform {
  backend "s3" {
    bucket = "twdu-germany-terraform-state"
    key    = "data-workflow"
    region = "eu-central-1"
    dynamodb_table = "terraform-lock"
  }
}

provider "aws" {
  region = "eu-central-1"
}