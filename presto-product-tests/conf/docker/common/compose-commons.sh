# docker-compose has a limited understanding of relative paths and interprets them relative to
# compose-file location. We can't guarantee the shape of the paths coming from env variables,
# so we canonicalize them.
function export_canonical_path() {
    # make the path start with '/' or './'. Otherwise the result for 'foo.txt' is an absolute path.
    local PATH_REFERENCE=$1
    # when ref=var; var=value; then ${!ref} returns value
    # echo the variable to resolve any wildcards in paths
    local PATH
    PATH=$( echo ${!PATH_REFERENCE} )
    if [[ ${PATH} != /* ]] ; then
      PATH=./${PATH}
    fi
    if [[ -d $PATH ]]; then
        PATH=$(cd $PATH && pwd)
    elif [[ -f $PATH ]]; then
        PATH=$(echo "$(cd ${PATH%/*} && pwd)/${PATH##*/}")
    else
        echo "Invalid path: [$PATH]. Working directory is [$(pwd)]." >&2
        exit 1
    fi
    export $PATH_REFERENCE=$PATH
}

source "${BASH_SOURCE%/*}/../../../bin/locations.sh"
source "${BASH_SOURCE%/*}/../../../conf/product-tests-defaults.sh"
source "${PRODUCT_TESTS_ROOT}/target/classes/presto.env"

# create CLI history file (otherwise Docker creates it as a directory)
export PRESTO_CLI_HISTORY_FILE="/tmp/presto_history_docker"
touch "${PRESTO_CLI_HISTORY_FILE}"

# The following variables are defined to enable running product tests with arbitrary/downloaded jars
# and without building the project. The `presto.env` file should only be sourced if any of the variables
# is undefined, otherwise running product tests without a build won't work.

if [[ -z "${PRESTO_SERVER:-}" ]]; then
    PRESTO_SERVER="${PROJECT_ROOT}/presto-server/target/presto-server-${PRESTO_VERSION}.tar.gz"
fi
export_canonical_path PRESTO_SERVER

if [[ -z "${PRESTO_CLI_JAR:-}" ]]; then
    PRESTO_CLI_JAR="${PROJECT_ROOT}/presto-cli/target/presto-cli-${PRESTO_VERSION}-executable.jar"
fi
export_canonical_path PRESTO_CLI_JAR

if [[ -z "${PRODUCT_TESTS_JAR:-}" ]]; then
    PRODUCT_TESTS_JAR="${PRODUCT_TESTS_ROOT}/target/presto-product-tests-${PRESTO_VERSION}-executable.jar"
fi
export_canonical_path PRODUCT_TESTS_JAR

export HIVE_PROXY_PORT=${HIVE_PROXY_PORT:-1180}

if [[ -z "${PRESTO_JDBC_DRIVER_JAR:-}" ]]; then
    PRESTO_JDBC_DRIVER_JAR="${PROJECT_ROOT}/presto-jdbc/target/presto-jdbc-${PRESTO_VERSION}.jar"
fi
export_canonical_path PRESTO_JDBC_DRIVER_JAR

export PRESTO_JDBC_DRIVER_JAR_DIR="$(dirname "${PRESTO_JDBC_DRIVER_JAR}")"
export PRESTO_JDBC_DRIVER_CLASS=${PRESTO_JDBC_DRIVER_CLASS:-"io.prestosql.jdbc.PrestoDriver"}
