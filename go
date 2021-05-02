#!/usr/bin/env bash

set -ex
script_dir=$(cd "$(dirname "$0")" ; pwd -P)
PROJECT="data-derp"

goal_pull-dev-container() {
  pushd "${script_dir}" > /dev/null
    username=${1}

    if [ -z "${username}" ]; then
      echo "USERNAME not set. Usage USERNAME"
      exit 1
    fi

    read  -p "Enter Github Token: " -s token

    echo ${token} | docker login https://docker.pkg.github.com -u ${username} --password-stdin

    docker pull docker.pkg.github.com/kelseymok/data-derp/dev-container:latest
    docker tag docker.pkg.github.com/kelseymok/data-derp/dev-container:latest data-derp:latest
  popd > /dev/null
}

goal_build-dev-container() {
  pushd "${script_dir}" > /dev/null
    docker build -t data-derp .
  popd > /dev/null
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
    setup-workflow                - Sets up workflow and branch given team name
"
  exit 1
fi
