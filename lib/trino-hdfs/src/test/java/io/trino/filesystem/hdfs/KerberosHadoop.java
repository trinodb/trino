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

import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.PrintingLogConsumer;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.StandardSystemProperty.USER_NAME;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static java.util.Objects.requireNonNull;

public class KerberosHadoop
        extends BaseTestContainer
{
    private static final String IMAGE = "ghcr.io/trinodb/testing/hive3.1-kerberos:" + getDockerImagesVersion();

    private static final int HDFS_PORT = 9000;

    private final Path keytabDirectory;

    public KerberosHadoop(Path keytabDirectory, Network network)
    {
        super(IMAGE,
                "hadoop-master",
                Set.of(HDFS_PORT),
                keytabMounts(keytabDirectory),
                Map.of("HADOOP_USER_NAME", requireNonNull(USER_NAME.value())),
                Optional.of(network),
                1);
        this.keytabDirectory = requireNonNull(keytabDirectory, "keytabDirectory is null");
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withLogConsumer(new PrintingLogConsumer("KerberosHadoop"));
    }

    @Override
    public void start()
    {
        super.start();
        executeInContainerFailOnError("bash", "-e", "-c",
                """
                kinit -kt /opt/hadoop/etc/hadoop/hdfs.keytab hdfs/hadoop-master@TRINO.TEST
                for attempt in {1..30}; do
                    if timeout 10s bash -c "printf ready | hadoop fs -put -f - /_trino_hdfs_ready && hadoop fs -cat /_trino_hdfs_ready | grep -q ready"; then
                        hadoop fs -rm -f /_trino_hdfs_ready
                        exit 0
                    fi
                    sleep 2
                done
                echo "HDFS did not become ready" >&2
                exit 1
                """);
    }

    public String getHdfsUri()
    {
        return "hdfs://%s/".formatted(getMappedHostAndPortForExposedPort(HDFS_PORT));
    }

    public byte[] getHdfsKeytab()
            throws Exception
    {
        return Files.readAllBytes(keytabDirectory.resolve("hdfs.keytab"));
    }

    public void printDiagnostics()
    {
        printDiagnostic("supervisor status", "supervisorctl", "status");
        printDiagnostic("container disk usage", "df", "-h");
        printDiagnostic("HDFS report", "hdfs", "dfsadmin", "-report");
        printDiagnostic("HDFS fsck", "hdfs", "fsck", "/", "-files", "-blocks", "-locations");
        printDiagnostic("NameNode log", "bash", "-c", "tail -n 500 /var/log/hadoop-hdfs/*namenode*.log 2>&1 || true");
        printDiagnostic("DataNode log", "bash", "-c", "tail -n 500 /var/log/hadoop-hdfs/*datanode*.log 2>&1 || true");
    }

    private static Map<String, String> keytabMounts(Path keytabDirectory)
    {
        return Map.of(
                "/etc/security/keytabs/hdfs.keytab", keytabDirectory.resolve("hdfs.keytab").toString(),
                "/etc/security/keytabs/hive.keytab", keytabDirectory.resolve("hive.keytab").toString(),
                "/etc/security/keytabs/HTTP.keytab", keytabDirectory.resolve("HTTP.keytab").toString(),
                "/etc/security/keytabs/mapred.keytab", keytabDirectory.resolve("mapred.keytab").toString(),
                "/etc/security/keytabs/yarn.keytab", keytabDirectory.resolve("yarn.keytab").toString());
    }

    private void printDiagnostic(String name, String... command)
    {
        try {
            Container.ExecResult result = executeInContainer(command);
            System.err.printf(
                    "KerberosHadoop diagnostic: %s%ncommand: %s%nexit code: %s%nstdout:%n%s%nstderr:%n%s%n",
                    name,
                    String.join(" ", command),
                    result.getExitCode(),
                    result.getStdout(),
                    result.getStderr());
        }
        catch (RuntimeException e) {
            System.err.printf("Failed to print KerberosHadoop diagnostic: %s%n%s%n", name, e);
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

    public static AfterTestExecutionCallback printDiagnosticsOnFailure(Supplier<KerberosHadoop> hadoop)
    {
        return context -> context.getExecutionException()
                .filter(failure -> !isIncompleteExecution(failure))
                .ifPresent(_ -> {
                    KerberosHadoop container = hadoop.get();
                    if (container != null) {
                        container.printDiagnostics();
                    }
                });
    }
}
