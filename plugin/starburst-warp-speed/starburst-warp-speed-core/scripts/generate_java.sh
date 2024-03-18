#!/usr/bin/env bash

VARADA_BASE_DIR=$1
VARADA_XSLT_DIR=${VARADA_BASE_DIR}/src/main/xslt
VARADA_SCRIPTS_DIR=${VARADA_BASE_DIR}/scripts
source ${VARADA_SCRIPTS_DIR}/common.sh
VARADA_JAVA_DIR=${VARADA_BASE_DIR}/src/main/java/io/trino/plugin/warp/gen
CURR_GIT_COMMIT=$(get_git_rev)
CURR_SCRIPT_NAME=$(basename "$0")
TMP_AGG_XML=$(mktemp /tmp/tmp.${CURR_SCRIPT_NAME}.${CURR_GIT_COMMIT}.XXXXXX)


echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" >> ${TMP_AGG_XML}
echo "<Objects>" >> ${TMP_AGG_XML}
for i in $( ls ${VARADA_XSLT_DIR}/global_native_stats/*.xml); do
    grep "<Object\|</Object" ${i} | cut -d ":" -f2 >> ${TMP_AGG_XML}
    "${VARADA_SCRIPTS_DIR}"/generate_java_onefile.sh ${VARADA_BASE_DIR} ${i} Object stats
    safe_check
done
echo "</Objects>" >> ${TMP_AGG_XML}
xsltproc ${VARADA_XSLT_DIR}/stats_global_native_mgr.xml ${TMP_AGG_XML} > ${VARADA_JAVA_DIR}/VaradaStatsMgr.java.t
safe_check

copy_file_if_changed ${VARADA_JAVA_DIR}/VaradaStatsMgr.java.t ${VARADA_JAVA_DIR}/stats/VaradaStatsMgr.java

xsltproc ${VARADA_XSLT_DIR}/error_codes_java.xml ${VARADA_XSLT_DIR}/error_codes/error_codes.xml > ${VARADA_JAVA_DIR}/ErrorCodes.java.t

mkdir -p ${VARADA_JAVA_DIR}/errorcodes/
copy_file_if_changed ${VARADA_JAVA_DIR}/ErrorCodes.java.t ${VARADA_JAVA_DIR}/errorcodes/ErrorCodes.java

for i in $( ls ${VARADA_XSLT_DIR}/java_stats/*.xml); do
    ${VARADA_SCRIPTS_DIR}/generate_java_onefile.sh ${VARADA_BASE_DIR} ${i} Java stats
    safe_check
done

for i in $( ls ${VARADA_XSLT_DIR}/constants/*.xml); do
    ${VARADA_SCRIPTS_DIR}/generate_java_onefile.sh ${VARADA_BASE_DIR} ${i} Constants constants
    safe_check
done

rm -f /tmp/tmp.${CURR_SCRIPT_NAME}.${CURR_GIT_COMMIT}.*
