# Bootstrap
This directory holds Cloudformation templates to set up the following
* [Self-hosted Github Runner](#githubrunner)
* [Terraform Remote State S3 Bucket and DynamoDB](#terraform-remote-state)


## Gitlab Runner
We are running a Gitlab Runner in AWS as an EC2 instance backed by an Autoscaling group in a private subnet of a VPC. The number of GithubActions credits is insufficient for the amount of development that will proceed during the training.

### Setup
1. Generate a Gitlab Runner Token. This token is valid for 60 minutes.
2. Add Token to SSM Parameter store `/twdu-germany/github-reg-token`   
3. In the AWS Console, create a Cloudformation Stack using the [gitlabrunner.yaml](./gitlabrunner.yaml)

**TODO**: create bootstrap script to do this via CLI

### Resources
* [Adding Self-Hosted Runners](https://docs.github.com/en/actions/hosting-your-own-runners/adding-self-hosted-runners)
* [Create a Registration Token for Gitlab Runner](https://docs.github.com/en/rest/reference/actions#create-a-registration-token-for-an-organization)
    * TODO: but where to store the registration token?

### Notes
* Forks of this repo can also use the runner. Can we disable forks?

## Terraform Remote State
In order to use terraform, a remote state must already exist. This gets around the chicken-egg problem of configuring a bucket before running terraform applys.

### Setup
1. In the AWS Console, create a Cloudformation Stack using [terraform-state.yaml](./terraform-state.yaml)
2. Reference this bucket in the Terraform backend remote s3 state
