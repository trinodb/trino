#!/bin/bash

set -xeuo pipefail

for hive_properties in $(find '/docker/presto-product-tests/conf/presto/etc/catalog' -name '*hive*.properties'); do
    echo "Updating $hive_properties for HDP3"
    # Add file format time zone properties
    echo "hive.parquet.time-zone=UTC" >> "${hive_properties}"
    echo "hive.rcfile.time-zone=UTC" >> "${hive_properties}"
done
