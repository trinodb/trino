#!/bin/bash

set -euo pipefail

fail() {
  echo "$(basename "$0"): $*" >&2
  exit 1
}

if [ $# -ne 2 ]; then
  fail "Usage: $0 <some-site.xml> <overrides.xml>" >&2
fi

site_xml="$1"
overrides="$2"
site_xml_new="$1.new"

test -f "${site_xml}" || fail "${site_xml} does not exist or is not a file"
test -f "${overrides}" || fail "${overrides} does not exist or is not a file"
test ! -e "${site_xml_new}" || fail "${site_xml_new} already exists"

xsltproc --param override-path "'${overrides}'" /docker/presto-product-tests/conf/docker/files/site-override.xslt "${site_xml}" > "${site_xml_new}"
cat "${site_xml_new}" > "${site_xml}" # Preserve file owner & permissions
rm "${site_xml_new}"
