#!/usr/bin/env bash

set -e
set -o nounset
set -o pipefail

script_dir=$(cd "$(dirname "$0")" ; pwd -P)

goal_pull-dev-container() {
  pushd "${script_dir}" > /dev/null
    username=${1}

    if [ -z "${username}" ]; then
      echo "USERNAME not set. Usage TOKEN USERNAME"
      exit 1
    fi

    read  -p "Enter Github Token: " -s token

    echo ${token} | docker login https://docker.pkg.github.com -u ${username} --password-stdin

    docker pull docker.pkg.github.com/kelseymok/twdu-germany/dev-container:latest
  popd > /dev/null
}

TARGET=${1:-}
if type -t "goal_${TARGET}" &>/dev/null; then
  "goal_${TARGET}" ${@:2}
else
  echo "Usage: $0 <goal>

goal:
    pull-dev-container            - Pulls dev container (Usage: Username GithubToken)
"
  exit 1
fi
