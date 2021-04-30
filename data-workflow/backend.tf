terraform {
  backend "s3" {
    key    = "data-workflow"
    region = "eu-central-1"
  }
}

provider "aws" {
  region = "eu-central-1"
}