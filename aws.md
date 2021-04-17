# AWS
* [AWS Account: TWDU Germany](https://thoughtworks.okta.com/home/amazon_aws/0oa1kzdqca8OEU6ju0h8/272)
* [Get AWS credentials for CLI](#aws-creds-for-cli)

## AWS Creds for CLI
### Prerequisites
* Python 3 (recommendation: [install pyenv](https://github.com/pyenv/pyenv#installation). Version 3.7.8 works just fine.)
* AWS CLI: `pip install awscli`
* [Crowbar](https://github.com/moritzheiber/crowbar/#history): `brew install moritzheiber/tap/crowbar`

### Quickstart
To install AWS CLI/Crowbar: `./go setup`. This assumes you have Python already.

### Set Up Crowbar
Set up a profile called `twdu-germany` to use with the AWS TWDU Germany account:
```bash
crowbar profiles add twdu-germany -u <YOUR-USERNAME> -p okta --url "https://thoughtworks.okta.com/home/amazon_aws/0oa1kzdqca8OEU6ju0h8/272"
```

Login and verify access:
```bash
AWS_PROFILE=twdu-germany aws s3 ls --region eu-central-1
```

Switching a role:
