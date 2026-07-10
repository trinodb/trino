#!/bin/bash
: '
环境变量注释说明
[worker]
jvm_xmx - jvm运行过程中分配的最大内存,自定义参数
hadoop_user_name - hadoop默认操作用户
is_debug_kerberos - 是否开启kerberos debug
'

# ${var:=DEFAULT}     如果var没有被声明或者其值为空，那么就以 DEFAULT 作为其默认值
JVM_XMX=${jvm_xmx:=5G}
HADOOP_USER_NAME=${hadoop_user_name:=ocdp}
IS_DEBUG_KERBEROS=${is_debug_kerberos:=false}

source /etc/profile
PWD=`pwd`
REAL_PATH=$(readlink -f "${PWD}/lib")
PACKAGE_FILE_NAME=$(basename "$REAL_PATH")
PACKAGE_NAME=${PACKAGE_FILE_NAME%%.tar.gz}
ROOT_PWD=${REAL_PATH}/${PACKAGE_NAME}
YARN_CONF=${PWD}/conf

TMP_LOG_PATH=${tmp_path:=/tmp}
log_file=${CONTAINER_ID}result.log
echo `date "+%Y-%m-%d %H:%M:%S"` > ${TMP_LOG_PATH}/${log_file}
echo "container_pwd="`pwd` >> ${TMP_LOG_PATH}/${log_file}
echo "hostname="`hostname` >> ${TMP_LOG_PATH}/${log_file}
echo "whoami="`whoami` >> ${TMP_LOG_PATH}/${log_file}
echo "trino_dir="$ROOT_PWD >> ${TMP_LOG_PATH}/${log_file}

TRINO_CONFIG_DIR=${ROOT_PWD}/etc
TRINO_LOG_DIR=${ROOT_PWD}/logs

function create_default_jvm_config() {
    file_path=$1

    cat > ${file_path}/jvm.config << EOF
-server
-Xmx${JVM_XMX}
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
-XX:+EnableDynamicAgentLoading
-XX:+UnlockDiagnosticVMOptions
-XX:G1NumCollectionsKeepPinned=10000000
-DHADOOP_USER_NAME=${HADOOP_USER_NAME}
-Dsun.security.krb5.debug=${IS_DEBUG_KERBEROS}
EOF

    chmod 644 ${file_path}/jvm.config
}

function create_default_node_properties() {
    file_path=$1

    cat > ${file_path}/node.properties << EOF
node.environment=production
node.id=`cat /proc/sys/kernel/random/uuid`
node.data-dir=${TRINO_LOG_DIR}
EOF

    chmod 644 ${file_path}/node.properties
}


rm -rf ${TRINO_LOG_DIR}
if [[ ! -d ${TRINO_LOG_DIR} ]]; then
    mkdir -p ${TRINO_LOG_DIR}
fi


if [[ ! -d ${TRINO_CONFIG_DIR} ]]; then
    mkdir -p ${TRINO_CONFIG_DIR}
fi


create_default_jvm_config ${TRINO_CONFIG_DIR}
create_default_node_properties ${TRINO_CONFIG_DIR}


source $ROOT_PWD/bin/tools.sh
dep_cp_dir ${YARN_CONF} ${TRINO_CONFIG_DIR}
sed -i "s/^node.id=.*$/node.id=`cat /proc/sys/kernel/random/uuid`/g" ${TRINO_CONFIG_DIR}/node.properties

${ROOT_PWD}/bin/launcher --random run
