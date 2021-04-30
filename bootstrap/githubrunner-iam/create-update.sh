#!/usr/bin/env bash

set -e

script_dir=$(cd "$(dirname "$0")" ; pwd -P)
aws_profile=data-derp

pushd "${script_dir}" > /dev/null
  project_name="${1}"
  module_name="${2}"

  for i in project_name module_name; do
    if [ -z "!{i}" ]; then
      echo "${i} not set. Usage <func> PROJECT_NAME MODULE_NAME"
      exit 1
    fi
  done

  stack_name="${project_name}-${module_name}-githubrunner-iam"
  if [[ ! $(AWS_PROFILE=${aws_profile} aws cloudformation describe-stacks --stack-name "${stack_name}" --region eu-central-1) ]]; then
    echo "Stack (${stack_name}) does not exist. Creating..."
    AWS_PROFILE=${aws_profile} aws cloudformation create-stack --stack-name "${stack_name}" \
      --template-body file://./template.yaml \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameters ParameterKey=ProjectName,ParameterValue=${project_name} ParameterKey=ModuleName,ParameterValue=${module_name} \
      --region eu-central-1
  else
    echo "Stack (${stack_name}) exists. Creating ChangeSet..."
    now=$(date +%s)
    AWS_PROFILE=${aws_profile} aws cloudformation create-change-set \
      --stack-name "${stack_name}" \
      --change-set-name "update-${now}" \
      --template-body file://./template.yaml \
      --capabilities CAPABILITY_NAMED_IAM \
      --parameters ParameterKey=ProjectName,ParameterValue=${project_name} ParameterKey=ModuleName,ParameterValue=${module_name} \
      --region eu-central-1 \

    sleep 10
    AWS_PROFILE=${aws_profile} aws cloudformation execute-change-set \
      --change-set-name "update-${now}" \
      --stack-name "${stack_name}"
  fi
popd > /dev/null
