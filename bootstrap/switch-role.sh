#!/usr/bin/env bash

set -e

script_dir=$(cd "$(dirname "$0")" ; pwd -P)
base_profile="${1}"
role_name="${2}"

target_profile=data-derp

for i in base_profile role_name; do
  if [ -z "!{i}" ]; then
    echo "${i} not set. Usage <func> BASE_AWS_PROFILE ROLE_NAME"
    exit 1
  fi
done

identity=$(AWS_PROFILE=${base_profile} aws sts get-caller-identity)
account=$(echo $identity | jq -r '.Account')
arn=$(echo $identity | jq -r '.Arn')

response=$(AWS_PROFILE=${base_profile} aws sts assume-role \
--role-arn "arn:aws:iam::${account}:role/${role_name}" \
--role-session-name "bootstrap-$(echo $arn | cut -d '/' -f 3)")

aws configure set aws_access_key_id $(echo $response | jq -r '.Credentials.AccessKeyId') --profile "${target_profile}"
aws configure set aws_secret_access_key $(echo $response | jq -r '.Credentials.SecretAccessKey') --profile "${target_profile}"
aws configure set aws_session_token $(echo $response | jq -r '.Credentials.SessionToken') --profile "${target_profile}"