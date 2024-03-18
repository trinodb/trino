#!/usr/bin/env bash

VARADA_SCRIPTS_DIR=$1/scripts
source ${VARADA_SCRIPTS_DIR}/common.sh

${VARADA_SCRIPTS_DIR}/generate_java.sh $1
safe_check

project_basedir=$(get_project_basedir $1)
echo "generate.sh -> project_basedir=${project_basedir} PRESTO_HOME=$1"
