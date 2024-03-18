#!/usr/bin/env bash

VARADA_BASE_DIR=$1
SUFFIX_PACKAGE_NAME=$4
VARADA_SCRIPTS_DIR=${VARADA_BASE_DIR}/scripts
VARADA_XSLT_DIR=${VARADA_BASE_DIR}/src/main/xslt
VARADA_JAVA_DIR=${VARADA_BASE_DIR}/src/main/java/io/trino/plugin/warp/gen/${SUFFIX_PACKAGE_NAME}

XML_FILE=$2
TYPE=$3
XSL_PROC_FILE=stats_global_native_java.xml
OBJ_NAME=$(grep "<Object " ${XML_FILE} | cut -d "=" -f 2 | cut -d "\"" -f 2)
CLASS_NAME_PREFIX="VaradaStats"

if [ ${TYPE} = Instance ]; then
    XSL_PROC_FILE=stats_instance_native_java.xml
fi
if [ ${TYPE} = Java ]; then
    XSL_PROC_FILE=stats_java.xml
fi
if [ ${TYPE} = Constants ]; then
    XSL_PROC_FILE=constants_java.xml
    # Example: <Object Name="compound_func_type" java_name="CompoundFuncType">
    OBJ_NAME=$(grep "<Object " ${XML_FILE} | cut -d= -f 3 | cut -d "\"" -f 2)
    CLASS_NAME_PREFIX=""
fi

# make first char upper case
OBJ_NAME=$(echo "${OBJ_NAME}" | awk '{for(i=1;i<=NF;i++){ $i=toupper(substr($i,1,1)) substr($i,2) }}1')

source ${VARADA_SCRIPTS_DIR}/common.sh

mkdir -p ${VARADA_JAVA_DIR}
xsltproc ${VARADA_XSLT_DIR}/${XSL_PROC_FILE} ${XML_FILE} > ${VARADA_JAVA_DIR}/${CLASS_NAME_PREFIX}${OBJ_NAME}.java.t
safe_check

copy_file_if_changed ${VARADA_JAVA_DIR}/${CLASS_NAME_PREFIX}${OBJ_NAME}.java.t ${VARADA_JAVA_DIR}/${CLASS_NAME_PREFIX}${OBJ_NAME}.java
