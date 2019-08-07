#!/usr/bin/env bash
echo "compiling project presto 317"
cd ../../
mvn clean install -DskipTests
cd presto-server/target
tar -zxf presto-server-317.tar.gz
cp -r ../../deploy/PRESTO/package presto-server-317
tar -czf presto-server-317.tar.gz presto-server-317