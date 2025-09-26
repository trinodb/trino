#!/usr/bin/env bash

set -xeuo pipefail
trap "exit" INT # allows to terminate script on ctrl+c instead of terminating single mvnw execution

maven="${BASH_SOURCE%/*}/../../../mvnw"
maven_run_tests="${maven} clean test ${MAVEN_TEST:--B} -pl :trino-test-jdbc-compatibility-old-driver"

"${maven}" -version

current_version=$(${maven} help:evaluate -Dexpression=project.version -q -DforceStdout)
previous_released_version=$((${current_version%-SNAPSHOT}-1))
first_tested_version=352
# test n-th version only
version_step=$(( (previous_released_version - first_tested_version) / 7 ))

echo "Current version: ${current_version}"
echo "Testing every ${version_step}. version between ${first_tested_version} and ${previous_released_version}"

# 404 was skipped
# 422-424 depend on the incompatible version of the open-telemetry semantic conventions used while invoking tests
tested_versions=$(seq "${first_tested_version}" ${version_step} "${previous_released_version}" | grep -vx '404\|42[234]')

if (( (previous_released_version - first_tested_version) % version_step != 0 )); then
    tested_versions="${tested_versions} ${previous_released_version}"
fi

exit_code=0
failed_versions=()

for version in ${tested_versions[*]}; do
    if [[ "${version}" == "477" ]]
    then
        echo "TODO: 477 was skipped because trino-jdbc-477 wasn't release to sonatype central"
        continue
    fi
    if ! time env TRINO_JDBC_VERSION_UNDER_TEST="${version}" ${maven_run_tests} -Ddep.presto-jdbc-under-test="${version}"; then
        exit_code=1
        failed_versions+=("${version}")
    fi
done

echo "$0: exiting with ${exit_code}, failed versions: ${failed_versions[@]+\"${failed_versions[@]}\"}"
exit "${exit_code}"
