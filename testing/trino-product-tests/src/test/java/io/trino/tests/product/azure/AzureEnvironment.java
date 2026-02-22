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
package io.trino.tests.product.azure;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.lang.String.format;

/**
 * Product test environment for external Azure-backed Hive/Iceberg/Delta tests.
 *
 * This mirrors launcher's multinode-azure environment:
 * - Hadoop + Hive metastore configured for ABFS-backed warehouse
 * - Spark with ABFS credentials for cross-engine reads
 * - Trino catalogs: hive, iceberg, delta, tpch
 */
public class AzureEnvironment
        extends ProductTestEnvironment
{
    private static final String AZURE_CONFIG_DIR =
            "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/environment/multinode-azure";

    private static final Duration HIVE_METASTORE_STARTUP_TIMEOUT = Duration.ofMinutes(4);
    private static final Duration HIVE_METASTORE_STARTUP_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int HIVE_METASTORE_STABLE_SUCCESS_POLL_COUNT = 3;
    private static final Duration HADOOP_STARTUP_TIMEOUT = Duration.ofMinutes(12);
    private static final String AZURE_URI_SCHEME = "abfs";
    private static final String AZURE_ENDPOINT = "dfs.core.windows.net";
    private static final String HADOOP_AZURE_JAR_URL = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.2.4/hadoop-azure-3.2.4.jar";
    private static final String WILDFLY_OPENSSL_JAR_URL = "https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar";

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver", e);
        }
    }

    private Network network;
    private HadoopContainer hadoop;
    private SparkIcebergContainer spark;
    private TrinoContainer trino;

    private String schemaName;
    private String schemaLocation;
    private String abfsContainer;
    private String abfsAccount;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        abfsContainer = requireEnv("ABFS_CONTAINER");
        abfsAccount = requireEnv("ABFS_ACCOUNT");
        String abfsAccessKey = requireEnv("ABFS_ACCESS_KEY");
        schemaName = "test_" + UUID.randomUUID().toString().replace("-", "");
        schemaLocation = format("%s://%s@%s.%s/%s", AZURE_URI_SCHEME, abfsContainer, abfsAccount, AZURE_ENDPOINT, schemaName);

        network = Network.newNetwork();

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        configureHadoop(hadoop, abfsContainer, abfsAccount, abfsAccessKey, schemaName);
        hadoop.waitingFor(Wait.forLogMessage(".*success: socks-proxy entered RUNNING state.*", 1)
                .withStartupTimeout(HADOOP_STARTUP_TIMEOUT));
        hadoop.start();
        waitForHiveMetastoreStable();

        spark = new SparkIcebergContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
        spark.dependsOn(hadoop);
        spark.withCopyToContainer(
                Transferable.of(readConfigFile("spark-defaults.conf")
                        .replace("%ABFS_ACCOUNT%", abfsAccount)
                        .replace("%ABFS_ACCESS_KEY%", abfsAccessKey)),
                "/spark/conf/spark-defaults.conf");
        spark.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", Map.of(
                        "connector.name", "hive",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "false",
                        "fs.native-azure.enabled", "true",
                        "azure.auth-type", "ACCESS_KEY",
                        "azure.access-key", "${ENV:ABFS_ACCESS_KEY}",
                        "hive.non-managed-table-writes-enabled", "true",
                        "hive.translate-hive-views", "true",
                        "hive.parquet.time-zone", "UTC",
                        "hive.rcfile.time-zone", "UTC"))
                .withCatalog("delta", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "false",
                        "fs.native-azure.enabled", "true",
                        "azure.auth-type", "ACCESS_KEY",
                        "azure.access-key", "${ENV:ABFS_ACCESS_KEY}"))
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "iceberg.file-format", "PARQUET",
                        "fs.hadoop.enabled", "false",
                        "fs.native-azure.enabled", "true",
                        "azure.auth-type", "ACCESS_KEY",
                        "azure.access-key", "${ENV:ABFS_ACCESS_KEY}"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.withEnv("ABFS_ACCESS_KEY", abfsAccessKey);
        trino.withEnv("ABFS_ACCOUNT", abfsAccount);
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    public String getSchemaLocation()
    {
        return schemaLocation;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public Connection createHiveConnection()
            throws SQLException
    {
        String jdbcUrl = "jdbc:hive2://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT);
        return DriverManager.getConnection(jdbcUrl, "hive", "");
    }

    public QueryResult executeHive(String sql)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive query: " + sql, e);
        }
    }

    public int executeHiveUpdate(String sql)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive update: " + sql, e);
        }
    }

    public void executeHiveCommand(String sql)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive command: " + sql, e);
        }
    }

    public void executeHadoopFsCommand(String command)
    {
        try {
            ExecResult result = hadoop.execInContainer("bash", "-lc", command);
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Hadoop fs command failed: " + command + "\nstdout: " + result.getStdout() + "\nstderr: " + result.getStderr());
            }
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Failed to execute Hadoop fs command: " + command, e);
        }
    }

    public Connection createSparkConnection()
            throws SQLException
    {
        return DriverManager.getConnection(spark.getJdbcUrl(), "hive", "");
    }

    public QueryResult executeSpark(String sql)
    {
        try (Connection conn = createSparkConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark query: " + sql, e);
        }
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino != null ? trino.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (spark != null) {
            spark.close();
            spark = null;
        }
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void configureHadoop(HadoopContainer container, String containerName, String account, String accessKey, String schema)
    {
        String coreSiteOverrides = readConfigFile("core-site-overrides-template.xml")
                .replace("%ABFS_ACCOUNT%", account)
                .replace("%ABFS_ACCESS_KEY%", accessKey)
                .replace("</configuration>", """
                        
                            <property>
                                <name>fs.abfs.impl</name>
                                <value>org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem</value>
                            </property>
                            <property>
                                <name>fs.AbstractFileSystem.abfs.impl</name>
                                <value>org.apache.hadoop.fs.azurebfs.Abfs</value>
                            </property>
                        
                        </configuration>
                        """);
        String applyScript = """
                #!/bin/bash
                set -euxo pipefail
                if ! command -v xsltproc >/dev/null 2>&1; then
                    dnf install -y libxslt
                fi
                if ! command -v curl >/dev/null 2>&1; then
                    dnf install -y curl
                fi
                cp -f /opt/hadoop/share/hadoop/tools/lib/hadoop-azure-*.jar /opt/hadoop/share/hadoop/common/lib/
                cp -f /opt/hadoop/share/hadoop/tools/lib/hadoop-azure-datalake-*.jar /opt/hadoop/share/hadoop/common/lib/
                cp -f /opt/hadoop/share/hadoop/tools/lib/azure-*.jar /opt/hadoop/share/hadoop/common/lib/
                mkdir -p /tmp/azure-libs
                curl -fsSL --retry 5 --retry-delay 2 -o /tmp/azure-libs/hadoop-azure-3.2.4.jar "%s"
                curl -fsSL --retry 5 --retry-delay 2 -o /tmp/azure-libs/wildfly-openssl-1.0.7.Final.jar "%s"
                cp -f /tmp/azure-libs/hadoop-azure-3.2.4.jar /opt/hadoop/share/hadoop/common/lib/
                cp -f /tmp/azure-libs/wildfly-openssl-1.0.7.Final.jar /opt/hadoop/share/hadoop/common/lib/
                cp -f /opt/hadoop/share/hadoop/common/lib/hadoop-azure-*.jar /opt/hive/lib/
                cp -f /opt/hadoop/share/hadoop/common/lib/hadoop-azure-datalake-*.jar /opt/hive/lib/
                cp -f /opt/hadoop/share/hadoop/common/lib/azure-*.jar /opt/hive/lib/
                cp -f /opt/hadoop/share/hadoop/common/lib/wildfly-openssl-1.0.7.Final.jar /opt/hive/lib/
                CORE_SITE_PATH="/opt/hadoop/etc/hadoop/core-site.xml"
                if [ -f /etc/hadoop/conf/core-site.xml ]; then
                    CORE_SITE_PATH="/etc/hadoop/conf/core-site.xml"
                fi
                apply-site-xml-override "${CORE_SITE_PATH}" "/docker/trino-product-tests/conf/environment/multinode-azure/core-site-overrides.xml"
                """.formatted(HADOOP_AZURE_JAR_URL, WILDFLY_OPENSSL_JAR_URL);

        container.withEnv("ABFS_CONTAINER", containerName);
        container.withEnv("ABFS_ACCOUNT", account);
        container.withEnv("ABFS_SCHEMA", schema);
        container.withCopyToContainer(
                Transferable.of(coreSiteOverrides),
                "/docker/trino-product-tests/conf/environment/multinode-azure/core-site-overrides.xml");
        container.withCopyToContainer(
                Transferable.of(applyScript, 0777),
                "/etc/hadoop-init.d/apply-azure-config.sh");
        container.withCopyToContainer(
                Transferable.of(readConfigFile("update-location.sh"), 0777),
                "/etc/hadoop-init.d/update-location.sh");
    }

    private static String readConfigFile(String fileName)
    {
        Path configDir = locateAzureConfigDir();
        Path file = configDir.resolve(fileName);
        try {
            return Files.readString(file);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read Azure config file: " + file, e);
        }
    }

    private static Path locateAzureConfigDir()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(AZURE_CONFIG_DIR);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate Azure config directory: " + AZURE_CONFIG_DIR);
    }

    private void waitForHiveMetastoreStable()
    {
        long deadlineNanos = System.nanoTime() + HIVE_METASTORE_STARTUP_TIMEOUT.toNanos();
        int consecutiveSuccesses = 0;

        while (System.nanoTime() < deadlineNanos) {
            if (isHiveMetastoreRunningAndReachable()) {
                consecutiveSuccesses++;
                if (consecutiveSuccesses >= HIVE_METASTORE_STABLE_SUCCESS_POLL_COUNT) {
                    return;
                }
            }
            else {
                consecutiveSuccesses = 0;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(HIVE_METASTORE_STARTUP_POLL_INTERVAL.toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for Hive metastore readiness", e);
            }
        }

        throw new RuntimeException("Hive metastore did not become stable within " + HIVE_METASTORE_STARTUP_TIMEOUT);
    }

    private boolean isHiveMetastoreRunningAndReachable()
    {
        try {
            ExecResult status = hadoop.execInContainer(
                    "bash",
                    "-lc",
                    "set -euo pipefail; supervisorctl status hive-metastore | grep -q RUNNING; timeout 5 bash -lc 'echo > /dev/tcp/localhost/9083'");
            return status.getExitCode() == 0;
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while probing Hive metastore readiness", e);
            }
            return false;
        }
    }
}
