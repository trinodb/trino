/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.hdfs;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.PrintingLogConsumer;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.testcontainers.containers.Container;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.StandardSystemProperty.USER_NAME;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class Hadoop
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(Hadoop.class);

    private static final String IMAGE = "ghcr.io/trinodb/testing/hive3.1:" + getDockerImagesVersion();

    private static final int HDFS_PORT = 9000;

    public static final ConfigurationInitializer SINGLE_DATANODE_CONFIGURATION_INITIALIZER = config -> {
        config.setInt("dfs.replication", 1);
        config.setInt("dfs.client.block.write.retries", 6);
        config.setInt("dfs.client.block.write.locateFollowingBlock.retries", 6);
        config.setInt("dfs.client.block.write.locateFollowingBlock.initial.delay.ms", 400);
        config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        config.setBoolean("dfs.client.block.write.replace-datanode-on-failure.best-effort", true);
        config.setBoolean("dfs.namenode.avoid.write.stale.datanode", false);
    };

    public Hadoop()
    {
        super(IMAGE,
                "hadoop-master",
                Set.of(HDFS_PORT),
                emptyMap(),
                Map.of("HADOOP_USER_NAME", requireNonNull(USER_NAME.value())),
                Optional.empty(),
                1);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withLogConsumer(new PrintingLogConsumer("Hadoop"));
        withRunCommand(List.of("bash", "-e", "-c",
                """
                rm /etc/supervisord.d/{hive*,mysql*,socks*,sshd*}.conf
                hdfs namenode -format -nonInteractive
                sed -i "s|hdfs://localhost|hdfs://$(hostname)|g" /opt/hadoop/etc/hadoop/core-site.xml
                exec supervisord -c /etc/supervisord.conf
                """));
    }

    @Override
    public void start()
    {
        super.start();
        assertEventually(new Duration(1, MINUTES), new Duration(1, SECONDS), () ->
                assertThat(executeInContainerFailOnError("hdfs", "dfsadmin", "-report", "-live"))
                        .contains("Live datanodes (1)"));
        executeInContainerFailOnError("hadoop", "fs", "-rm", "-f", "-r", "/*");
        executeInContainerFailOnError("bash", "-e", "-c",
                """
                printf 'ready' | hadoop fs -put - /_trino_hdfs_ready
                hadoop fs -cat /_trino_hdfs_ready
                hadoop fs -rm -f /_trino_hdfs_ready
                """);
        log.info("Hadoop container started with HDFS endpoint: %s", getHdfsUri());
    }

    public String getHdfsUri()
    {
        return "hdfs://%s/".formatted(getMappedHostAndPortForExposedPort(HDFS_PORT));
    }

    public void printDiagnostics()
    {
        log.warn("Printing Hadoop container diagnostics");
        printDiagnostic("supervisor status", "supervisorctl", "status");
        printDiagnostic("container disk usage", "df", "-h");
        printDiagnostic("container inode usage", "df", "-i");
        printDiagnostic("HDFS report", "hdfs", "dfsadmin", "-report");
        printDiagnostic("HDFS single DataNode configuration", "bash", "-c",
                """
                for key in \\
                    dfs.replication \\
                    dfs.client.block.write.retries \\
                    dfs.client.block.write.locateFollowingBlock.retries \\
                    dfs.client.block.write.locateFollowingBlock.initial.delay.ms \\
                    dfs.client.block.write.replace-datanode-on-failure.policy \\
                    dfs.client.block.write.replace-datanode-on-failure.best-effort \\
                    dfs.namenode.avoid.write.stale.datanode; do
                    echo "${key}=$(hdfs getconf -confKey "${key}" 2>&1 || true)"
                done
                """);
        printDiagnostic("HDFS disk usage", "hadoop", "fs", "-df", "-h", "/");
        printDiagnostic("HDFS directory usage", "hadoop", "fs", "-du", "-h", "/");
        printDiagnostic("HDFS fsck", "hdfs", "fsck", "/", "-files", "-blocks", "-locations");
        printDiagnostic("NameNode log", "bash", "-c", "tail -n 500 /var/log/hadoop-hdfs/*namenode*.log 2>&1 || true");
        printDiagnostic("DataNode log", "bash", "-c", "tail -n 500 /var/log/hadoop-hdfs/*datanode*.log 2>&1 || true");
    }

    private void printDiagnostic(String name, String... command)
    {
        try {
            Container.ExecResult result = executeInContainer(command);
            log.warn(
                    "Hadoop diagnostic: %s%ncommand: %s%nexit code: %s%nstdout:%n%s%nstderr:%n%s",
                    name,
                    String.join(" ", command),
                    result.getExitCode(),
                    result.getStdout(),
                    result.getStderr());
        }
        catch (RuntimeException e) {
            log.warn(e, "Failed to print Hadoop diagnostic: %s", name);
        }
    }

    private static boolean isIncompleteExecution(Throwable failure)
    {
        for (Class<?> failureClass = failure.getClass(); failureClass != null; failureClass = failureClass.getSuperclass()) {
            if (failureClass.getName().equals("org.opentest4j.IncompleteExecutionException")) {
                return true;
            }
        }
        return false;
    }

    public static AfterTestExecutionCallback printDiagnosticsOnFailure(Supplier<Hadoop> hadoop)
    {
        return context -> context.getExecutionException()
                .filter(failure -> !isIncompleteExecution(failure))
                .ifPresent(_ -> hadoop.get().printDiagnostics());
    }
}
