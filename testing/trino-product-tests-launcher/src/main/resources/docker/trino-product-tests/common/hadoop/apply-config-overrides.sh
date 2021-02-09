#!/usr/bin/env bash

OVERRIDES_DIR=${OVERRIDES_DIR:-/overrides}
IFS=':' read -r -a HADOOP_OVERRIDES_DIRS <<< "${OVERRIDES_DIR}"

for overrides_dir in "${HADOOP_OVERRIDES_DIRS[@]}"
do
    if test -d "${overrides_dir}"; then
        echo "Applying Hadoop conf overrides from dir ${overrides_dir}"
        /usr/local/bin/apply-all-site-xml-overrides "${overrides_dir}"
    else
      echo "Hadoop conf overrides dir ${overrides_dir} does not exist"
    fi
done
