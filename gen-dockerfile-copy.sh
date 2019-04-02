#!/bin/bash
find . -name 'presto-*' -type d -d 1 -exec sh -c 'echo COPY ${0#"./"} /build/${0#"./"}' {} \;
