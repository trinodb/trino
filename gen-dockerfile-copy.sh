#!/bin/bash
echo COPY .git /build/.git
find . -name 'presto-*' -type d -d 1 -exec sh -c 'echo COPY ${0#"./"} /build/${0#"./"}' {} \;
echo COPY src /build/src
echo COPY pom.xml /build/pom.xml
