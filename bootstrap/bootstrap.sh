#!/bin/bash

set -e

usage() { echo "Usage: $0 [-p <project name: string>] [-m <module name: string>] [-u <github username: string>]" 1>&2; exit 1; }

while getopts ":p:m:u:" o; do
    case "${o}" in
        p)
            project=${OPTARG}
            ;;
        m)
            module=${OPTARG}
            ;;
        u)
            githubuser=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${project}" ] || [ -z "${module}" ] || [ -z "${githubuser}" ]; then
    usage
fi

./all-in-one/create-stack.sh "${project}" "${module}" "${githubuser}"