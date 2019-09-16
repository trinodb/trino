#!/usr/bin/env bash

set -xeuo pipefail

# TODO enable avro_schema_url when adding Metastore impersonation
presto-product-tests/bin/run_on_docker.sh \
    multinode \
    -x quarantine,big_query,storage_formats,profile_specific_tests,tpcds,cassandra,mysql_connector,postgresql_connector,mysql,kafka,avro_schema_url,simba_jdbc
