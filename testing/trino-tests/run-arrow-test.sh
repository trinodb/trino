#!/bin/bash

# Set classpath
CP="target/test-classes:target/classes"
for jar in target/dependency/*.jar; do
    CP="$CP:$jar"
done

# Run the test
java -cp "$CP" \
    -Duser.language=en \
    -Duser.region=US \
    -Dfile.encoding=UTF-8 \
    -Xmx3g \
    -Xms3g \
    -XX:+ExitOnOutOfMemoryError \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:-OmitStackTraceInFastThrow \
    -XX:G1HeapRegionSize=32M \
    -XX:+UnlockDiagnosticVMOptions \
    -XX:+EnableDynamicAgentLoading \
    --add-modules=jdk.incubator.vector \
    --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
    org.junit.platform.console.ConsoleLauncher \
    --select-class=io.trino.server.protocol.TestArrowSpooledDistributedQueries 