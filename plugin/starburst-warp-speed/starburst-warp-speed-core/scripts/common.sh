#!/bin/sh

safe_check()
{
    exit_code=${?}
    if [[ ${exit_code} != 0 ]]; then
        echo "exit wasn't 0 (got ${exit_code}), check the logs"
        exit ${exit_code}
    fi
}

is_in_docker_or_linux()
{
  if [[ $(uname -s) = Linux* ]] || [ -f /.dockerenv ]; then
    return 0
  else
    return 1
  fi
}

get_project_basedir()
{
  if is_in_docker_or_linux; then
    project_basedir=${1}
  else
    # shellcheck disable=SC2016
    project_basedir='${PRESTO_HOME}/trino-varada'
  fi

  echo ${project_basedir}
}

copy_file_if_changed()
{
    current_name=${1}
    destination_name=${2}
    diff  ${current_name} ${destination_name} &> /dev/null
    if [ $? -ne 0 ]; then
        echo "Changes detected in: ${destination_name}"
        if [ -f ${destination_name} ]; then
            rm ${destination_name}
        fi
        mv ${current_name} ${destination_name}
    else
        rm ${current_name}
    fi
}

assert_presto_home()
{
    if [ -z ${PRESTO_HOME} ]; then
        echo "PRESTO_HOME is not defined"
        exit 1
    elif [ ! -d ${PRESTO_HOME} ]; then
        echo "PRESTO_HOME is not a valid path"
        exit 1
    fi
}

get_git_rev()
{
    if [ ! -z ${GIT_COMMIT_OVERRIDE} ]; then
      echo ${GIT_COMMIT_OVERRIDE}
      return
    fi
    echo $(git rev-parse HEAD | cut -c 1-8)
}

read_presto_version()
{
    if [ ! -z ${GIT_COMMIT_OVERRIDE} ]; then
      echo ${GIT_COMMIT_OVERRIDE}
      return
    fi
    # read the version from the root pom.xml - the third occurrence of version should be our one
    pom=${1}/pom.xml

    if [ ! -r ${pom} ]; then
      echo "Unable to read from pom.xml at ${pom}"
      exit 1
    fi

    grep -A 1 presto-root ${pom} | grep version | sed 's+[><]+;+g' | awk -F \; '{print $3}'
}

get_arch()
{
	arch=$(uname -m)
	curarch="amd64"
	if [ ${arch} = "arm64" ] || [ ${arch} = "aarch64" ]; then
        curarch="aarch64"
	fi

	echo "${curarch}"
}

MVN_CMD="${PRESTO_HOME}/mvnw --errors --no-transfer-progress"

if [ -z ${JENKINS_HOME} ]; then
    # retain transfer progress for developer environment
    MVN_CMD="${PRESTO_HOME}/mvnw --errors"
fi
