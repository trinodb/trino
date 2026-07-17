#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

# Supervisord is not running
if ! test -f /tmp/supervisor.sock; then
    exit 0
fi

# Check if all Hadoop services are running
FAILED=$(supervisorctl status | grep -v RUNNING || true)

if [ "$FAILED" != "" ]; then
  echo "Some of the services are failing: ${FAILED}"
  exit 1
fi

print_hdfs_diagnostics()
{
    echo "HDFS configuration:"
    for key in \
        dfs.replication \
        dfs.client.block.write.retries \
        dfs.client.block.write.locateFollowingBlock.retries \
        dfs.client.block.write.locateFollowingBlock.initial.delay.ms \
        dfs.client.block.write.replace-datanode-on-failure.policy \
        dfs.client.block.write.replace-datanode-on-failure.best-effort \
        dfs.namenode.avoid.write.stale.datanode \
        dfs.safemode.threshold.pct; do
        echo "${key}=$(hdfs getconf -confKey "${key}" 2>&1 || true)"
    done

    echo "HDFS report:"
    hdfs dfsadmin -report 2>&1 || true

    echo "HDFS disk usage:"
    hadoop fs -df -h / 2>&1 || true

    echo "NameNode log tail:"
    tail -n 200 /var/log/hadoop-hdfs/*namenode*.log 2>&1 || true

    echo "DataNode log tail:"
    tail -n 200 /var/log/hadoop-hdfs/*datanode*.log 2>&1 || true
}

check_hdfs_write_read_delete()
{
    local health_dir="/tmp"
    local health_file="${health_dir}/trino-hdfs-health-check-$(hostname)-$$"
    local expected="trino-hdfs-health-check"
    local actual
    local authentication

    authentication=$(hdfs getconf -confKey hadoop.security.authentication 2>&1 || true)
    if [ "${authentication}" = "kerberos" ]; then
        echo "Skipping HDFS write/read/delete health check for Kerberos-authenticated HDFS"
        return 0
    fi

    hadoop fs -mkdir -p "${health_dir}" || return 1
    hadoop fs -rm -f "${health_file}" >/dev/null 2>&1 || true
    printf '%s' "${expected}" | hadoop fs -put -f - "${health_file}" || return 1
    actual=$(hadoop fs -cat "${health_file}") || return 1
    hadoop fs -rm -f "${health_file}" || return 1

    if [ "${actual}" != "${expected}" ]; then
        echo "Unexpected HDFS health-check file content. Expected '${expected}', got '${actual}'"
        return 1
    fi
}

if ! check_hdfs_write_read_delete; then
    echo "HDFS write/read/delete health check failed"
    print_hdfs_diagnostics
    exit 1
fi
