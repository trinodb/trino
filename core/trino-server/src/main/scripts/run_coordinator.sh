#!/bin/bash
: '
环境变量注释说明
[coordinator]
jvm_xmx - jvm运行过程中分配的最大内存,自定义参数
trino_gateway_url - gateway代理地址
trino_ranger_url - trino对应的ranger地址
trino_repository_name - trino在ranger上注册的实例名
cluster_group - 对应presto gateway的RoutingGroup
coordinator_port - 协调节点服务端口
coordinator_protocol - 协调节点服务协议http/https
hadoop_user_name - hadoop默认操作用户
is_debug_kerberos - 是否开启kerberos debug
is_enable_ranger_plugin - 是否开启ranger plugin
'

# ${var:=DEFAULT} 如果var没有被声明或者其值为空，那么就以 DEFAULT 作为其默认值
JVM_XMX=${jvm_xmx:=5G}
TRINO_GATEWAY_URL=${trino_gateway_url}
TRINO_RANGER_URL=${trino_ranger_url}
TRINO_REPOSITORY_NAME=${trino_repository_name}
CLUSTER_GROUP=${cluster_group:=adhoc}
COORDINATOR_PORT=${coordinator_port}
COORDINATOR_PROTOCOL=${coordinator_protocol:=http}
HADOOP_USER_NAME=${hadoop_user_name:=ocdp}
IS_DEBUG_KERBEROS=${is_debug_kerberos:=false}
IS_ENABLE_RANGER_PLUGIN=${is_enable_ranger_plugin:=false}

cur_hostname=`hostname`
coordinator_url=${COORDINATOR_PROTOCOL}://${cur_hostname}:${COORDINATOR_PORT}

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
echo "container_home="`pwd` >> ${TMP_LOG_PATH}/${log_file}
echo "hostname="`hostname` >> ${TMP_LOG_PATH}/${log_file}
echo "whoami="`whoami` >> ${TMP_LOG_PATH}/${log_file}
echo "trino_home="$ROOT_PWD >> ${TMP_LOG_PATH}/${log_file}

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

function create_ranger_plugin_logback() {
    file_path=$1

    cat > ${file_path}/trino-ranger-plugin-logback.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>

<configuration scan="true" scanPeriod="30 seconds">
  <appender name="console" class="ch.qos.logback.core.ConsoleAppender">
    <Target>System.out</Target>
    <encoder>
      <pattern>%-5p - %m</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="console"/>
  </root>
</configuration>
EOF

    chmod 644 ${file_path}/trino-ranger-plugin-logback.xml
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
node_id=`cat /proc/sys/kernel/random/uuid`
sed -i "s/^node.id=.*$/node.id=${node_id}/g" ${TRINO_CONFIG_DIR}/node.properties

if [ "vtrue" == "v${IS_ENABLE_RANGER_PLUGIN,,}" ]; then
    if [ -z "${TRINO_REPOSITORY_NAME}" ] || [ -z "${TRINO_RANGER_URL}" ]; then
        echo "[ERROR] please check params [trino_repository_name trino_ranger_url]."
        exit 1
    fi

    ranger_install_properties=${ROOT_PWD}/extend/install.properties
    tmp_presto_home=`echo ${ROOT_PWD} | sed 's#\/#\\\/#g'`
    sed -i "s/^COMPONENT_INSTALL_DIR_NAME=.*$/COMPONENT_INSTALL_DIR_NAME=${tmp_presto_home}/g" ${ranger_install_properties}
    sed -i "s/^REPOSITORY_NAME=.*$/REPOSITORY_NAME=${TRINO_REPOSITORY_NAME}/g" ${ranger_install_properties}
    tmp_ranger_url=`echo ${TRINO_RANGER_URL} | sed 's#\/#\\\/#g'`
    sed -i "s/^POLICY_MGR_URL=.*$/POLICY_MGR_URL=${tmp_ranger_url}/g" ${ranger_install_properties}
    sed -i "s/^CUSTOM_USER=.*$/CUSTOM_USER=${HADOOP_USER_NAME}/g" ${ranger_install_properties}
    sed -i "s/^CUSTOM_GROUP=.*$/CUSTOM_GROUP=${HADOOP_USER_NAME}/g" ${ranger_install_properties}

    touch ${TRINO_CONFIG_DIR}/access-control.properties
    touch ${TRINO_CONFIG_DIR}/ranger-policymgr-ssl.xml
    touch ${TRINO_CONFIG_DIR}/ranger-security.xml
    touch ${TRINO_CONFIG_DIR}/ranger-trino-audit.xml
    touch ${TRINO_CONFIG_DIR}/ranger-trino-security.xml
    create_ranger_plugin_logback ${TRINO_CONFIG_DIR}

    ${ROOT_PWD}/extend/enable-trino-plugin.sh
fi

curl -s -k -X POST ${TRINO_GATEWAY_URL}/entity?entityType=GATEWAY_BACKEND -d '{  "name": "'${node_id}'", "proxyTo": "'${coordinator_url}'","active": true,"routingGroup": "'${CLUSTER_GROUP}'" }'
if [ $? -ne 0 ]; then
    echo "[ERROR] ${coordinator_url} register failed, please check service ${TRINO_GATEWAY_URL}."
    exit 1
fi

${ROOT_PWD}/bin/launcher run
