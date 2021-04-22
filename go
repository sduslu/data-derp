#!/usr/bin/env bash

set -ex
script_dir=$(cd "$(dirname "$0")" ; pwd -P)
PROJECT="twdu-germany"

goal_pull-dev-container() {
  pushd "${script_dir}" > /dev/null
    username=${1}

    if [ -z "${username}" ]; then
      echo "USERNAME not set. Usage USERNAME"
      exit 1
    fi

    read  -p "Enter Github Token: " -s token

    echo ${token} | docker login https://docker.pkg.github.com -u ${username} --password-stdin

    docker pull docker.pkg.github.com/kelseymok/twdu-germany/dev-container:latest
    docker tag docker.pkg.github.com/kelseymok/twdu-germany/dev-container:latest twdu-dev-container:latest
  popd > /dev/null
}

goal_build-dev-container() {
  pushd "${script_dir}" > /dev/null
    docker build -t twdu-dev-container .
  popd > /dev/null
}

goal_switch-to-admin-role() {
  pushd "${script_dir}" > /dev/null
    identity=$(AWS_PROFILE=${PROJECT} aws sts get-caller-identity)
    account=$(echo $identity | jq -r '.Account')
    arn=$(echo $identity | jq -r '.Arn')

    response=$(AWS_PROFILE=${PROJECT} aws sts assume-role \
    --role-arn "arn:aws:iam::${account}:role/federated-admin" \
    --role-session-name "local-$(echo $arn | cut -d '/' -f 3)")

    aws configure set aws_access_key_id $(echo $response | jq -r '.Credentials.AccessKeyId') --profile default
    aws configure set aws_secret_access_key $(echo $response | jq -r '.Credentials.SecretAccessKey') --profile default
    aws configure set aws_session_token $(echo $response | jq -r '.Credentials.SessionToken') --profile default
  popd > /dev/null
}

goal_setup-workflow() {
  pushd "${script_dir}" > /dev/null
    workflow_name=$1
    if [ -z "${workflow_name}" ]; then
      echo "WORKFLOW_NAME not set. Usage <workflow-name: ab-cd-ef>"
      exit 1
    fi
    git checkout master
    echo "Creating branch: ${workflow_name}"
    git checkout -B "${workflow_name}"
    echo "Branch (${workflow_name}) created."

    cp .github/workflows/example.yml.tpl ".github/workflows/${workflow_name}.yml"
    sed -i '' -e s/example1\-example2/foo\-bar/g ./.github/workflows/foo-bar.yml
    git add .github/workflows/${workflow_name}.yml
    git commit -m "auto: creating branch (${workflow_name}) and github actions workflow"
  popd > /dev/null
}

goal_setup() {
  if [[ ! $(crowbar) ]]; then
    brew install moritzheiber/tap/crowbar
  else
    echo "Crowbar installed. Nothing to do here!"
  fi

  if [[ !$(pyenv) ]]; then
    brew install pyenv
  else
    echo "Pyenv installed. Nothing to do here!"
  fi

  if [[ ! $(aws) ]]; then
    pip install awscli
  else
    echo "AWS cli installed. Nothing to do here!"
  fi

  echo "Setting up Crowbar profile (${PROJECT})"
  read  -p "Enter OKTA username (before @): " -s okta_username

  crowbar profiles add "${PROJECT}" -u $okta_username -p okta --url "https://thoughtworks.okta.com/home/amazon_aws/0oa1kzdqca8OEU6ju0h8/272"
  if [[ $(AWS_PROFILE=twdu-germany aws s3 ls --region eu-central-1) ]]; then
    echo "Crowbar profile successfully created and connected to AWS"
  else
    echo "Crowbar profile could not connect to AWS resources. Did you enter the right username/password?"
  fi
}

TARGET=${1:-}
if type -t "goal_${TARGET}" &>/dev/null; then
  "goal_${TARGET}" ${@:2}
else
  echo "Usage: $0 <goal>

goal:
    pull-dev-container            - Pulls dev container (Usage: GithubUsername)
    build-dev-container           - Builds dev container
    setup                         - Installs Crowbar and AWS CLI if not exists
"
  exit 1
fi
