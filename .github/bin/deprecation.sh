#!/usr/bin/env bash

set -euo pipefail

MAVEN="${MAVEN:-./mvnw}"
MAVEN_INSTALL_OPTS="-Xmx3G -XX:+ExitOnOutOfMemoryError -Dmaven.wagon.rto=60000"

GIT_ROOT=$(git rev-parse --show-toplevel)
DEPRECATION_LIST=".github/resources/deprecation.log"

export MAVEN_OPTS="${MAVEN_OPTS:-$MAVEN_INSTALL_OPTS}"

help() {
    echo "Usage: deprecation.sh <update|verify>"
    exit 1
}

maven_deprecation() {
    maven_log=$(mktemp tmp-maven.XXXXX)
    echo "[INFO] Maven log: $maven_log"

    $MAVEN clean install \
        -B --strict-checksums -V -T C1 -DskipTests -Dmaven.source.skip=true -Dair.check.skip-all \
        -P deprecation \
        -pl '!:trino-docs,!:trino-server,!:trino-server-rpm' \
        | tee -a "$maven_log"

    deprecation_list=$(mktemp tmp-deprecation.XXXXX)
    echo "[INFO] Deprecation list: $deprecation_list"
    # Strip redundant information
    grep ' has been deprecated$' "$maven_log" \
        | sed 's/^\[WARNING\] //' \
        | sed "s@$GIT_ROOT/@@" \
        | sed 's/:\[[0-9,]*\]//' \
        | sed 's/ has been deprecated//' \
        | sort >> "$deprecation_list"
    rm "$maven_log"
}

diff_deprecation() {
    diff_file=$(mktemp tmp-diff.XXXXX)
    diff -u "$deprecation_list" "$DEPRECATION_LIST" > "$diff_file" || true
    num_added=$(grep -c '^+' "$diff_file" || true)
    num_removed=$(grep -c '^-' "$diff_file" || true)
    echo "[INFO] Depecations: $num_added added, $num_removed removed"
    grep '^[+-]' "$diff_file" || true
    rm "$diff_file"
}

update() {
    echo "[INFO] Updating deprecation list"
    maven_deprecation
    diff_deprecation
    mv "$deprecation_list" "$DEPRECATION_LIST"
    echo "[INFO] $DEPRECATION_LIST updated"
}

verify() {
    echo "[INFO] Verifying deprecation list"
    maven_deprecation
    diff_deprecation
    rm "$deprecation_list"
    [ "$num_added" -ne 0 ] && echo "[ERROR] Deprecations added, fix them or run .github/bin/deprecation.sh to update the known usage list"
    [ "$num_removed" -ne 0 ] && echo "[ERROR] Deprecations removed, run .github/bin/deprecation.sh to update the known usage list"
    if [ "$num_added" -ne 0 ] || [ "$num_removed" -ne 0 ]; then
        exit 1
    fi
}

[ $# -ne 1 ] && help

case "$1" in
    update) update ;;
    verify) verify ;;
    *)
        echo "Unknown command $1"
        help
        ;;
esac

exit 0
BASE=$1
HEAD=$2

grep '^\[WARN\] ' "$BASE" | sed 's/\[WARN\] //' | sed 's/:\[[0-9,]*\]//' | sed "s@$PWD/@@" | sort > deprecation-base.log
grep '^\[WARN\] ' "$HEAD" | sed 's/\[WARN\] //' | sed 's/:\[[0-9,]*\]//' | sed "s@$PWD/@@" | sort > deprecation-head.log

diff -u deprecation-base.log deprecation-head.log > deprecation.diff || true
removed=$(grep -c '^-' deprecation.diff)
added=$(grep -c '^+' deprecation.diff)

echo "[INFO] Deprecations: $removed removed, +$added added"

if [ "$removed" -gt 0 ]; then
    echo "[INFO] Depreciations added"
    grep '^-' deprecation.diff | uniq -c
fi

if [ "$added" -gt 0 ]; then
    echo "[ERROR] Depreciations added"
    grep '^+' deprecation.diff | uniq -c
    exit 1
fi
