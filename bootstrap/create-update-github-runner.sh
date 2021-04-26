#!/usr/bin/env bash

set -e

script_dir=$(cd "$(dirname "$0")" ; pwd -P)
PROJECT=twdu-europe

create-update-github-runner() {
  pushd "${script_dir}" > /dev/null
    username="${1}"

    if [ -z "${username}" ]; then
      echo "GITHUB_USERNAME not set. Usage <func> GITHUB_USERNAME"
      exit 1
    fi

    token=$(fetch-github-registration-token ${username})
    switch-to-admin
    create-update-ssm-parameter "${token}"
    create-update-github-runner-stack
  popd > /dev/null
}

fetch-github-registration-token() {
    username="${1}"

    if [ -z "${username}" ]; then
      echo "USERNAME not set. Usage <func> USERNAME"
      exit 1
    fi

    response=$(curl \
      -u $username \
      -X POST \
      -H "Accept: application/vnd.github.v3+json" \
      https://api.github.com/repos/kelseymok/twdu-europe/actions/runners/registration-token)

    echo $response | jq -r .token
}

create-update-ssm-parameter() {
    token="${1}"
    if [ -z "${token}" ]; then
      echo "TOKEN not set. Usage: <func> TOKEN"
      exit 1
    fi

    parameter="/${PROJECT}/github-runner-reg-token"
    if [[ ! $(AWS_PROFILE=default aws ssm get-parameter --name "${parameter}" --region eu-central-1) ]]; then
      echo "Parameter (${parameter}) does not exist. Creating..."
      AWS_PROFILE=default aws ssm put-parameter \
        --name "${parameter}" \
        --value "${token}" \
        --type SecureString \
        --region eu-central-1
    else
      echo "Parameter (${parameter}) exists. Updating..."
      AWS_PROFILE=default aws ssm put-parameter \
        --name "${parameter}" \
        --value "${token}" \
        --overwrite \
        --region eu-central-1
    fi
}

create-update-github-runner-stack() {
  stack_name="${PROJECT}-githubrunner"
    if [[ ! $(aws cloudformation describe-stacks --stack-name "${stack_name}" --region eu-central-1) ]]; then
      echo "Stack (${stack_name}) does not exist. Creating..."
      AWS_PROFILE=default aws cloudformation create-stack --stack-name "${stack_name}" \
        --template-body file://./githubrunner.yaml \
        --capabilities CAPABILITY_NAMED_IAM \
        --region eu-central-1 \
        --parameters ParameterKey=ModuleName,ParameterValue=all ParameterKey=InstanceType,ParameterValue=t3.small
    else
      echo "Stack (${stack_name}) exists. Creating ChangeSet..."
      now=$(date +%s)
      AWS_PROFILE=default aws cloudformation create-change-set \
        --stack-name "${stack_name}" \
        --change-set-name "update-${now}" \
        --template-body file://./githubrunner.yaml \
        --capabilities CAPABILITY_NAMED_IAM \
        --region eu-central-1 \
        --parameters ParameterKey=ModuleName,ParameterValue=all ParameterKey=InstanceType,ParameterValue=t3.medium

      sleep 10
      AWS_PROFILE=default aws cloudformation execute-change-set \
        --change-set-name "update-${now}" \
        --stack-name "${stack_name}"
    fi
}

switch-to-admin() {
  identity=$(AWS_PROFILE=${PROJECT} aws sts get-caller-identity)
  account=$(echo $identity | jq -r '.Account')
  arn=$(echo $identity | jq -r '.Arn')

  response=$(AWS_PROFILE=${PROJECT} aws sts assume-role \
  --role-arn "arn:aws:iam::${account}:role/federated-admin" \
  --role-session-name "bootstrap-$(echo $arn | cut -d '/' -f 3)")

  aws configure set aws_access_key_id $(echo $response | jq -r '.Credentials.AccessKeyId') --profile default
  aws configure set aws_secret_access_key $(echo $response | jq -r '.Credentials.SecretAccessKey') --profile default
  aws configure set aws_session_token $(echo $response | jq -r '.Credentials.SessionToken') --profile default
}

username="${1}"

if [ -z "${username}" ]; then
  echo "GITHUB_USERNAME not set. Usage <func> GITHUB_USERNAME"
  exit 1
fi

create-update-github-runner $username