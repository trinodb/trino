#!/usr/bin/env bash

set -euo pipefail -x

. "${BASH_SOURCE%/*}/common.sh"

abort_if_not_gib_impacted

check_vars S3_BUCKET S3_BUCKET_ENDPOINT \
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

cleanup_hadoop_docker_containers
start_hadoop_docker_containers

test_directory="$(date '+%Y%m%d-%H%M%S')-$(uuidgen | sha1sum | cut -b 1-6)"

# insert AWS credentials
deploy_core_site_xml core-site.xml.s3-template \
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY S3_BUCKET_ENDPOINT

# create test tables
# can't use create_test_tables because the first table is created with different commands
table_path="s3a://${S3_BUCKET}/${test_directory}/trino_test_external_fs/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container /docker/files/hadoop-put.sh /docker/files/test_table.csv{,.gz,.bz2,.lz4} "${table_path}"
exec_in_hadoop_master_container sudo -Eu hive beeline -u jdbc:hive2://localhost:10000/default -n hive -e "
    CREATE EXTERNAL TABLE trino_test_external_fs(t_bigint bigint)
    STORED AS TEXTFILE
    LOCATION '${table_path}'"

table_path="s3a://${S3_BUCKET}/${test_directory}/trino_test_external_fs_with_header/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container hadoop fs -put -f /docker/files/test_table_with_header.csv{,.gz,.bz2,.lz4} "${table_path}"
exec_in_hadoop_master_container /usr/bin/hive -e "
    CREATE EXTERNAL TABLE trino_test_external_fs_with_header(t_bigint bigint)
    STORED AS TEXTFILE
    LOCATION '${table_path}'
    TBLPROPERTIES ('skip.header.line.count'='1')"

table_path="s3a://${S3_BUCKET}/${test_directory}/trino_test_external_fs_with_header_and_footer/"
exec_in_hadoop_master_container hadoop fs -mkdir -p "${table_path}"
exec_in_hadoop_master_container hadoop fs -put -f /docker/files/test_table_with_header_and_footer.csv{,.gz,.bz2,.lz4} "${table_path}"
exec_in_hadoop_master_container /usr/bin/hive -e "
    CREATE EXTERNAL TABLE trino_test_external_fs_with_header_and_footer(t_bigint bigint)
    STORED AS TEXTFILE
    LOCATION '${table_path}'
    TBLPROPERTIES ('skip.header.line.count'='2', 'skip.footer.line.count'='2')"

stop_unnecessary_hadoop_services

# restart hive-metastore to apply S3 changes in core-site.xml
docker exec "$(hadoop_master_container)" supervisorctl restart hive-metastore
retry check_hadoop

# run product tests
pushd "${PROJECT_ROOT}"
set +e
./mvnw ${MAVEN_TEST:--B} -pl :trino-hive-hadoop2 test -P test-hive-hadoop2-s3 \
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
