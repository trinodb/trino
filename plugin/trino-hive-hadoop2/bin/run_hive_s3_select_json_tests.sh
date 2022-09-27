#!/usr/bin/env bash

# Similar to run_hive_s3_tests.sh, but has only Amazon S3 Select JSON tests. This is in a separate file as the JsonSerDe
# class is only available in Hadoop 3.1 version, and so we would only test JSON pushdown against the 3.1 version.

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

abort_if_not_gib_impacted

check_vars S3_BUCKET S3_BUCKET_ENDPOINT \
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

test_directory="$(date '+%Y%m%d-%H%M%S')-$(uuidgen | sha1sum | cut -b 1-6)-s3select-json"

# insert AWS credentials
deploy_core_site_xml core-site.xml.s3-template \
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_BUCKET_ENDPOINT

# create test tables
# can't use create_test_tables because the first table is created with different commands
table_path="s3a://${S3_BUCKET}/${test_directory}/trino_s3select_test_external_fs_json/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container /docker/files/hadoop-put.sh /docker/files/test_table.json{,.gz,.bz2} "${table_path}"
exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -e "
    CREATE EXTERNAL TABLE trino_s3select_test_external_fs_json(col_1 bigint, col_2 bigint)
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION '${table_path}'"

table_path="s3a://${S3_BUCKET}/${test_directory}/trino_s3select_test_json_scan_range_pushdown/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container /docker/files/hadoop-put.sh /docker/files/test_table_json_scan_range_select_pushdown_{1,2,3}.json "${table_path}"
exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -e "
    CREATE EXTERNAL TABLE trino_s3select_test_json_scan_range_pushdown(col_1 bigint, col_2 string, col_3 string,
    col_4 string, col_5 string, col_6 string, col_7 string, col_8 string, col_9 string, col_10 string, col_11 string,
    col_12 string, col_13 string, col_14 string)
        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        LOCATION '${table_path}'"
stop_unnecessary_hadoop_services

# restart hive-metastore to apply S3 changes in core-site.xml
docker exec "$(hadoop_master_container)" supervisorctl restart hive-metastore
retry check_hadoop

# run product tests
pushd "${PROJECT_ROOT}"
set +e
./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-s3-select-json \
    -DHADOOP_USER_NAME=hive \
    -Dhive.hadoop2.metastoreHost=localhost \
    -Dhive.hadoop2.metastorePort=9083 \
    -Dhive.hadoop2.databaseName=default \
    -Dhive.hadoop2.s3.awsAccessKey="${AWS_ACCESS_KEY_ID}" \
    -Dhive.hadoop2.s3.awsSecretKey="${AWS_SECRET_ACCESS_KEY}" \
    -Dhive.hadoop2.s3.writableBucket="${S3_BUCKET}" \
    -Dhive.hadoop2.s3.testDirectory="${test_directory}"
EXIT_CODE=$?
set -e
popd

cleanup_hadoop_docker_containers

exit "${EXIT_CODE}"
