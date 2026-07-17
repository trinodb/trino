#!/usr/bin/env bash
set -euo pipefail

if test $# -gt 0; then
    echo "$0 does not accept arguments" >&2
    exit 32
fi

HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}

configure_xml()
{
    local config_file="$1"

    if ! test -f "${config_file}"; then
        return
    fi

    python - "${config_file}" <<'PY'
import sys
import xml.etree.ElementTree as ElementTree

config_file = sys.argv[1]
properties = {
    "dfs.replication": "1",
    "dfs.client.block.write.retries": "6",
    "dfs.client.block.write.locateFollowingBlock.retries": "6",
    "dfs.client.block.write.locateFollowingBlock.initial.delay.ms": "400",
    "dfs.client.block.write.replace-datanode-on-failure.policy": "NEVER",
    "dfs.client.block.write.replace-datanode-on-failure.best-effort": "true",
    "dfs.namenode.avoid.write.stale.datanode": "false",
    "dfs.safemode.threshold.pct": "0",
}

tree = ElementTree.parse(config_file)
root = tree.getroot()
properties_by_name = {}

for property_element in root.findall("property"):
    name_element = property_element.find("name")
    if name_element is not None and name_element.text:
        properties_by_name[name_element.text.strip()] = property_element

for name, value in sorted(properties.items()):
    property_element = properties_by_name.get(name)
    if property_element is None:
        property_element = ElementTree.SubElement(root, "property")
        name_element = ElementTree.SubElement(property_element, "name")
        name_element.text = name
    value_element = property_element.find("value")
    if value_element is None:
        value_element = ElementTree.SubElement(property_element, "value")
    value_element.text = value

tree.write(config_file, encoding="UTF-8", xml_declaration=True)
with open(config_file, "a") as output:
    output.write("\n")
PY
}

configure_xml "${HADOOP_CONF_DIR}/core-site.xml"
configure_xml "${HADOOP_CONF_DIR}/hdfs-site.xml"
