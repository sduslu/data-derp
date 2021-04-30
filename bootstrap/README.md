# Bootstrap
This directory holds Cloudformation templates to set up the following
* [Self-hosted Github Runner](#githubrunner)
* [Terraform Remote State S3 Bucket and DynamoDB](#terraform-remote-state)

## Gitlab Runner
We are running a Gitlab Runner in AWS as an EC2 instance backed by an Autoscaling group in a private subnet of a VPC. The number of GithubActions credits is insufficient for the amount of development that will proceed during the training.

### Setup
1. Follow the [AWS Setup Instructions](../aws.md) to run AWS CLI commands against Okta apps
2. `./go githubrunner`

### Resources
* [Adding Self-Hosted Runners](https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners)
* [Create a Registration Token for Gitlab Runner](https://docs.github.com/en/rest/reference/actions#create-a-registration-token-for-a-repository)

### Notes
* Forks of this repo can also use the runner. Can we disable forks?

## Terraform Remote State
In order to use terraform, a remote state must already exist. This gets around the chicken-egg problem of configuring a bucket before running terraform applys.

### Setup
1. Follow the [AWS Setup Instructions](../aws.md) to run AWS CLI commands against Okta apps
2. `./go terraform-state`

## Data Source
Set up a base Data Source bucket that contains desanitised data for downstream processing

### Setup
1. Follow the [AWS Setup Instructions](../aws.md) to run AWS CLI commands against Okta apps
2. `./go data-source

