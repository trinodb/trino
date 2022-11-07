#!/usr/bin/env bash

function retry() {
    local END
    local EXIT_CODE

    END=$(($(date +%s) + 600))

    while (( $(date +%s) < $END )); do
        set +e
        "$@"
        EXIT_CODE=$?
        set -e

        if [[ ${EXIT_CODE} == 0 ]]; then
            break
        fi
        sleep 5
    done

    return ${EXIT_CODE}
}

function hadoop_master_container() {
    docker-compose -f "${DOCKER_COMPOSE_LOCATION}" ps -q hadoop-master | grep .
}

function hadoop_master_ip() {
    HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $HADOOP_MASTER_CONTAINER
}

function check_hadoop() {
    HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
    docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-server2 | grep -i running &> /dev/null &&
        docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status hive-metastore | grep -i running &> /dev/null &&
        docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -i 0.0.0.0:10000 &> /dev/null &&
        docker exec ${HADOOP_MASTER_CONTAINER} netstat -lpn | grep -i 0.0.0.0:9083 &> /dev/null
}

function exec_in_hadoop_master_container() {
    HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
    docker exec ${HADOOP_MASTER_CONTAINER} "$@"
}

function stop_unnecessary_hadoop_services() {
    HADOOP_MASTER_CONTAINER=$(hadoop_master_container)
    docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl status
    docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-resourcemanager
    docker exec ${HADOOP_MASTER_CONTAINER} supervisorctl stop yarn-nodemanager
}

# Expands docker compose file paths files into the format "-f $1 -f $2 ...."
# Arguments:
#   $1, $2, ...: A list of docker-compose files used to start/stop containers
function expand_compose_args() {
    local files=( "${@}" )
    local compose_args=""
    for file in ${files[@]}; do
        compose_args+=" -f ${file}"
    done
    echo "${compose_args}"
}

function cleanup_docker_containers() {
    local compose_args="$(expand_compose_args "$@")"
    # stop containers started with "up"
    docker-compose ${compose_args} down --remove-orphans

    # docker logs processes are being terminated as soon as docker container are stopped
    # wait for docker logs termination
    wait
}

function cleanup_hadoop_docker_containers() {
    cleanup_docker_containers "${DOCKER_COMPOSE_LOCATION}"
}

function termination_handler() {
    set +e
    cleanup_docker_containers "$@"
    exit 130
}

# Check that all arguments are the names of non-empty variables.
function check_vars() {
    ( # Subshell to preserve xtrace
        set +x # Disable xtrace to make the messages printed clear
        local failing=0
        for arg; do
            if [[ ! -v "${arg}" ]]; then
                echo "error: Variable not set: ${arg}" >&2
                failing=1
            elif [[ -z "${!arg}" ]]; then
                echo "error: Variable is empty: ${arg}" >&2
                failing=1
            fi
        done
        return "$failing"
    )
}

SCRIPT_DIR="${BASH_SOURCE%/*}"
INTEGRATION_TESTS_ROOT="${SCRIPT_DIR}/.."
PROJECT_ROOT="${INTEGRATION_TESTS_ROOT}/../.."
DOCKER_COMPOSE_LOCATION="${INTEGRATION_TESTS_ROOT}/conf/docker-compose.yml"
source "${INTEGRATION_TESTS_ROOT}/conf/hive-tests-defaults.sh"

# check docker and docker compose installation
docker-compose version
docker version

# extract proxy IP
if [ -n "${DOCKER_MACHINE_NAME:-}" ]
then
    PROXY=`docker-machine ip`
else
    PROXY=127.0.0.1
fi

# Starts containers based on multiple docker compose locations
# Arguments:
#   $1, $2, ...: A list of docker-compose files used to start containers
function start_docker_containers() {
    local compose_args="$(expand_compose_args $@)"
    # Purposefully don't surround ${compose_args} with quotes so that docker-compose infers multiple arguments
    # stop already running containers
    docker-compose ${compose_args} down || true

    # catch terminate signals
    # trap arguments are not expanded until the trap is called, so they must be in a global variable
    TRAP_ARGS="$@"
    trap 'termination_handler $TRAP_ARGS' INT TERM

    # pull docker images
    if [[ "${CONTINUOUS_INTEGRATION:-false}" == 'true' ]]; then
        retry docker-compose ${compose_args} pull --quiet
    fi

    # start containers
    docker-compose ${compose_args} up -d
}

function start_hadoop_docker_containers() {
    start_docker_containers "${DOCKER_COMPOSE_LOCATION}"

    # start docker logs for hadoop container
    docker-compose -f "${DOCKER_COMPOSE_LOCATION}" logs --no-color hadoop-master &

    # wait until hadoop processes is started
    retry check_hadoop
}

# $1 = base URI for table names
function create_test_tables() {
    local table_name table_path
    local base_path="${1:?create_test_tables requires an argument}"
    base_path="${base_path%/}" # remove trailing slash

    table_name="trino_test_external_fs"
    table_path="$base_path/$table_name/"
    exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
    exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /docker/files/test_table.csv{,.gz,.bz2,.lz4} "${table_path}"
    exec_in_hadoop_master_container /usr/bin/hive -e "CREATE EXTERNAL TABLE $table_name(t_bigint bigint) LOCATION '${table_path}'"

    table_name="trino_test_external_fs_with_header"
    table_path="$base_path/$table_name/"
    exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
    exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /docker/files/test_table_with_header.csv{,.gz,.bz2,.lz4} "${table_path}"
    exec_in_hadoop_master_container /usr/bin/hive -e "
        CREATE EXTERNAL TABLE $table_name(t_bigint bigint)
        STORED AS TEXTFILE
        LOCATION '${table_path}'
        TBLPROPERTIES ('skip.header.line.count'='1')"

    table_name="trino_test_external_fs_with_header_and_footer"
    table_path="$base_path/$table_name/"
    exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
    exec_in_hadoop_master_container hadoop fs -copyFromLocal -f /docker/files/test_table_with_header_and_footer.csv{,.gz,.bz2,.lz4} "${table_path}"
    exec_in_hadoop_master_container /usr/bin/hive -e "
        CREATE EXTERNAL TABLE $table_name(t_bigint bigint)
        STORED AS TEXTFILE
        LOCATION '${table_path}'
        TBLPROPERTIES ('skip.header.line.count'='2', 'skip.footer.line.count'='2')"
}

# $1 = basename of core-site.xml template
# other arguments are names of variables to substitute in the file
function deploy_core_site_xml() {
    local template="${1:?deploy_core_site_xml expects at least one argument}"
    shift
    local args=()
    local name value
    for name; do
        shift
        value="${!name//\\/\\\\}" # escape \ as \\
        value="${value//|/\\|}" # escape | as \|
        args+=(-e "s|%$name%|$value|g")
    done
    exec_in_hadoop_master_container bash -c \
        'sed "${@:2}" "/docker/files/$1" > /etc/hadoop/conf/core-site.xml' \
        bash "$template" "${args[@]}"
}

# Checks if Gitflow Incremental Builder (GIB) is enabled and the trino-hive-hadoop2 module should be build and/or tested
function abort_if_not_gib_impacted() {
    local module=plugin/trino-hive-hadoop2
    local impacted_log=gib-impacted.log
    if [ -f "$impacted_log" ] && ! grep -q "^${module}$" "$impacted_log"; then
        echo >&2 "Module $module not present in $impacted_log, exiting"
        exit 0
    fi
    return 0
}
