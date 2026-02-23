#!/bin/bash
set -euo pipefail

echo "Applying hive-site transactional overrides"
SITE_XML=/opt/hive/conf/hive-site.xml
TMP_XML=$(mktemp)

# Avoid dependency on xsltproc (not present in hive3.1 image); append properties directly.
sed '/<\/configuration>/d' "$SITE_XML" > "$TMP_XML"
cat >> "$TMP_XML" <<'EOF'
    <property>
        <name>metastore.create.as.acid</name>
        <value>true</value>
    </property>
</configuration>
EOF

mv "$TMP_XML" "$SITE_XML"
