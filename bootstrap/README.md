# Bootstrap
This directory holds a single Cloudformation template to set up the following
* A Single VPC, NAT Gateway, IG, Private/Public Subnet, VPC Endpoints to ensure private networking
* GithubRunner attached to your specified Github repository 
* Terraform Remote State S3 Bucket and DynamoDB
* S3 bucket containing the exercise's data to be ingested and transformed

## Setup
1. [Create a Github Personal Access Token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) with the Repo Scope. This will be used to generate a token to register a GithubRunner.
![github-repo-scope](./assets/github-repo-scope.png)
   
2. To reduce clashing with other AWS credentials, the bootstrap script uses an AWS_PROFILE set to `data-derp`. Manually create an AWS profile named `data-derp` with relevant credentials to your AWS account. For those expected to assume a role (within the same account), there is a helper function:
```bash
./switch-role.sh <starting-role> <target-role>
```
   
3. Create the Stack
```bash
./bootstrap.sh -p your-project-name -m your-team-name -u your-github-username
```

4. When prompted, enter your Personal Access Token (created in step 1)
```bash
Enter host password for user 'your-github-username': <the-personal-access-token>
```

4. View your [Cloudformation Stacks in the AWS Console](https://eu-central-1.console.aws.amazon.com/cloudformation/home?region=eu-central-1#/stacks)
