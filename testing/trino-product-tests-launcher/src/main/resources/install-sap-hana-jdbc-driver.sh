#!/bin/bash
set -xeuo pipefail
cp --no-clobber --verbose /docker/sap-hana-jdbc-driver/* /docker/presto-server/plugin/sap-hana
