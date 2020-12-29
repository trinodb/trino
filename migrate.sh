#!/usr/bin/env bash
set -xeuo pipefail

echo "Migrate module dirs"

find . -type d -name 'presto-*' -maxdepth 1 | while read dir; do
    newdir="${dir//presto-/trino-}"
    echo "Changing ${dir} into ${newdir}"
    mv "${dir}" "${newdir}"
done

echo "Migrating files to new packages"

find . -type d -wholename '*/io/prestosql' | while read dir; do
    newdir="${dir%/*}/trino"
    echo "Migrating $dir to $newdir"
    mv "${dir}" "${newdir}"
done

git add .
git commit -m "Repackage as io.trino (step 1 of 4)" -m "Move files to new location"

echo "Migrating packages in Java files"

find . -type f -name '*.java' | while read file; do
    sed -i 's/package io\.prestosql/package io\.trino/g' "${file}"
done

git add .
git commit -m "Repackage as io.trino (step 2 of 4)" -m "Update package statements"

echo "Migrating imports in Java files"

find . -type f -name '*.java' | while read file; do
    echo "Fixing imports in ${file}"

    sed -i '
        s/io\.prestosql/io\.trino/g
        s/io\/prestosql/io\/trino/g
    ' "${file}"

    # Revert some incorrent changes
    sed -i '
        s/io\.trino\.tempto/io\.prestosql\.tempto/g
        s/io\.trino\.tpch/io\.prestosql\.tpch/g
        s/io\.trino\.tpcds/io\.prestosql\.tpcds/g
        s/io\.trino\.hive/io\.prestosql\.hive/g
        s/io\.trino\.cassandra/io\.prestosql\.cassandra/g
        s/io\.trino\.hadoop/io\.prestosql\.hadoop/g
    ' "${file}"
done

git add .
git commit -m "Repackage as io.trino (step 3 of 4)" -m "Update import statements"

echo "Migrating pom modules"

find . -type f -name 'pom.xml' | while read file; do
    echo "Fixing module names in ${file}"

    sed -i '
        s/<groupId>io\.prestosql/<groupId>io\.trino/g
        s/<artifactId>presto-/<artifactId>trino-/g
        s/<name>presto-/<name>trino-/g
        s/<main-class>io\.prestosql\./<main-class>io\.trino\./g
        s/io\.prestosql\.plugin\.thrift\.api\.PrestoThriftService/io\.trino\.plugin\.thrift\.api\.PrestoThriftService/g
        s/io\.prestosql\.sql\.ReservedIdentifiers/io\.trino\.sql\.ReservedIdentifiers/g
    ' "${file}"

    # Revert some incorrect changes
    sed -i '
        s/<artifactId>trino-hadoop/<artifactId>presto-hadoop/g
        s/<groupId>io\.trino\.tempto/<groupId>io\.prestosql\.tempto/g
        s/<groupId>io\.trino\.hadoop/<groupId>io\.prestosql\.hadoop/g
        s/<groupId>io\.trino\.tpch/<groupId>io\.prestosql\.tpch/g
        s/<groupId>io\.trino\.tpcds/<groupId>io\.prestosql\.tpcds/g
        s/<groupId>io\.trino\.cassandra/<groupId>io\.prestosql\.cassandra/g
        s/<groupId>io\.trino\.hive/<groupId>io\.prestosql\.hive/g
        s/<groupId>io\.trino\.orc/<groupId>io\.prestosql\.orc/g
        s/<groupId>io\.trino\.benchto/<groupId>io\.prestosql\.benchto/g
        s/<name>trino-benchto/<name>presto-benchto/g
    ' "${file}"
done

echo "Migrating parent pom modules"

sed -i '
    s/<module>presto-/<module>trino-/g
    s/<dependenciesGroupIdPriorities>io\.prestosql/<dependenciesGroupIdPriorities>io\.trino,io\.prestosql/g
    s/<dependencyManagementGroupIdPriorities>io\.prestosql/<dependencyManagementGroupIdPriorities>io\.trino,io\.prestosql/g
' "pom.xml"

echo "Applying various fixes"

sed -i 's/:presto-/:trino-/g' trino-server/src/main/provisio/presto.xml
sed -i 's/:presto-/:trino-/g' trino-server-rpm/src/main/provisio/presto.xml

git add .
git commit -m "Repackage as io.trino (step 4 of 4)" -m "Update package and module references"
