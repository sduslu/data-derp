#!/usr/bin/env bash

set -e

script_dir=$(cd "$(dirname "$0")" ; pwd -P)
PROJECT=twdu-germany

create-update() {
  pushd "${script_dir}" > /dev/null
    switch-to-admin
    create-update-stack
    sleep 10
    upload_data
  popd > /dev/null
}

create-update-stack() {
  stack_name="${PROJECT}-data-source"
    if [[ ! $(aws cloudformation describe-stacks --stack-name "${stack_name}" --region eu-central-1) ]]; then
      echo "Stack (${stack_name}) does not exist. Creating..."
      AWS_PROFILE=default aws cloudformation create-stack --stack-name "${stack_name}" \
        --template-body file://./data-source.yaml \
        --capabilities CAPABILITY_NAMED_IAM \
        --region eu-central-1
    else
      echo "Stack (${stack_name}) exists. Creating ChangeSet..."
      now=$(date +%s)
      AWS_PROFILE=default aws cloudformation create-change-set \
        --stack-name "${stack_name}" \
        --change-set-name "update-${now}" \
        --template-body file://./data-source.yaml \
        --capabilities CAPABILITY_NAMED_IAM \
        --region eu-central-1 \

      sleep 10
      AWS_PROFILE=default aws cloudformation execute-change-set \
        --change-set-name "update-${now}" \
        --stack-name "${stack_name}"
    fi
}

upload_data() {
  pushd "${script_dir}" > /dev/null

    AWS_PROFILE=default aws s3 cp "GlobalLandTemperaturesByCountry.csv" "s3://twdu-germany-data-source/GlobalLandTemperaturesByCountry.csv"
    AWS_PROFILE=default aws s3 cp "TemperaturesByCountryDesanitized.csv" "s3://twdu-germany-data-source/TemperaturesByCountryDesanitized.csv"
    AWS_PROFILE=default aws s3 cp "GlobalTemperatures.csv" "s3://twdu-germany-data-source/GlobalTemperatures.csv"
    AWS_PROFILE=default aws s3 cp "CO2 emissions (Aggregate dataset (2021)).csv" "s3://twdu-germany-data-source/CO2 emissions (Aggregate dataset (2021)).csv"

  popd > /dev/null
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

create-update