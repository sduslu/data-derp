#!/usr/bin/env bash

set -e
script_dir=$(cd "$(dirname "$0")" ; pwd -P)

usage() { echo "Usage: $0 [-p <project name: string>] [-m <module name: string>]" 1>&2; exit 1; }

while getopts ":p:m:" o; do
    case "${o}" in
        p)
            project=${OPTARG}
            ;;
        m)
            module=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${project}" ] || [ -z "${module}" ]; then
    usage
fi

pushd "${script_dir}" > /dev/null
  INSTANCE_ID=$(AWS_PROFILE=data-derp aws ec2 describe-instances \
    --filter "Name=tag:Name,Values=${project}-${module}-spark-ui" \
    --query "Reservations[].Instances[?State.Name == 'running'].InstanceId[]" \
    --region "eu-central-1" \
    --output text)

  if [ -z "${INSTANCE_ID}" ]; then
    echo "No instances found. Exiting."
  fi

  # create the port forwarding tunnel
  AWS_PROFILE=data-derp aws ssm start-session --target $INSTANCE_ID \
                         --document-name AWS-StartPortForwardingSession \
                         --region eu-central-1 \
                         --parameters '{"portNumber":["18080"],"localPortNumber":["18080"]}'
popd > /dev/null


