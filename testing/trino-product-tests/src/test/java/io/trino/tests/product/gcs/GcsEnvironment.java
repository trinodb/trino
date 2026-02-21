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
package io.trino.tests.product.gcs;

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
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Product test environment for external GCS-backed Hive/Iceberg/Delta tests.
 *
 * This environment mirrors launcher's multinode-gcs wiring:
 * - Hadoop + Hive metastore configured for GCS-backed warehouse
 * - Spark with GCS credentials for cross-engine reads
 * - Trino catalogs: hive, iceberg, delta, tpch
 */
public class GcsEnvironment
        extends ProductTestEnvironment
{
    private static final String GCS_CONFIG_DIR =
            "testing/trino-product-tests-launcher/src/main/resources/docker/trino-product-tests/conf/environment/multinode-gcs";

    private static final String HADOOP_GCP_CREDENTIALS_FILE = "/etc/trino/gcp-credentials.json";
    private static final String SPARK_GCP_CREDENTIALS_FILE = "/spark/conf/gcp-credentials.json";
    private static final Duration HIVE_METASTORE_STARTUP_TIMEOUT = Duration.ofMinutes(4);
    private static final Duration HIVE_METASTORE_STARTUP_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int HIVE_METASTORE_STABLE_SUCCESS_POLL_COUNT = 3;
    private static final Duration HADOOP_STARTUP_TIMEOUT = Duration.ofMinutes(6);

    private Network network;
    private HadoopContainer hadoop;
    private SparkIcebergContainer spark;
    private TrinoContainer trino;
    private String warehouseDirectory;

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver", e);
        }
    }

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        String gcpCredentialsKey = requireEnv("GCP_CREDENTIALS_KEY");
        String gcpStorageBucket = requireEnv("GCP_STORAGE_BUCKET");
        byte[] gcpCredentialsBytes = Base64.getDecoder().decode(gcpCredentialsKey);
        String gcpCredentialsJson = new String(gcpCredentialsBytes, UTF_8);
        String gcsTestDirectory = "env_multinode_gcs_" + UUID.randomUUID();
        warehouseDirectory = "gs://" + gcpStorageBucket + "/" + gcsTestDirectory;

        network = Network.newNetwork();

        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        configureHadoop(hadoop, gcpCredentialsJson, gcpStorageBucket, gcsTestDirectory);
        hadoop.waitingFor(Wait.forLogMessage(".*success: socks-proxy entered RUNNING state.*", 1)
                .withStartupTimeout(HADOOP_STARTUP_TIMEOUT));
        hadoop.start();
        waitForHiveMetastoreStable();

        spark = new SparkIcebergContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
        spark.dependsOn(hadoop);
        spark.withCopyToContainer(Transferable.of(readConfigFile("spark-defaults.conf")), "/spark/conf/spark-defaults.conf");
        spark.withCopyToContainer(Transferable.of(gcpCredentialsJson), SPARK_GCP_CREDENTIALS_FILE);
        spark.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", Map.of(
                        "connector.name", "hive",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "false",
                        "fs.native-gcs.enabled", "true",
                        "gcs.json-key", "${ENV:GCP_CREDENTIALS}",
                        "hive.non-managed-table-writes-enabled", "true",
                        "hive.parquet.time-zone", "UTC",
                        "hive.rcfile.time-zone", "UTC"))
                .withCatalog("delta", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "false",
                        "fs.native-gcs.enabled", "true",
                        "gcs.json-key", "${ENV:GCP_CREDENTIALS}"))
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "iceberg.file-format", "PARQUET",
                        "fs.hadoop.enabled", "false",
                        "fs.native-gcs.enabled", "true",
                        "gcs.json-key", "${ENV:GCP_CREDENTIALS}"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.withEnv("GCP_CREDENTIALS", gcpCredentialsJson);
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
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

    public String getWarehouseDirectory()
    {
        return warehouseDirectory;
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

    private void configureHadoop(HadoopContainer container, String gcpCredentialsJson, String gcpStorageBucket, String gcsTestDirectory)
    {
        String coreSiteOverrides = readConfigFile("core-site-overrides-template.xml")
                .replace("%GCP_CREDENTIALS_FILE_PATH%", HADOOP_GCP_CREDENTIALS_FILE);
        String hiveSiteOverrides = readConfigFile("hive-site-overrides-template.xml")
                .replace("%GCP_STORAGE_BUCKET%", gcpStorageBucket)
                .replace("%GCP_WAREHOUSE_DIR%", gcsTestDirectory);
        String applyScript = """
                #!/bin/bash
                set -euxo pipefail
                GCS_CONNECTOR_JAR=/opt/hadoop/share/hadoop/common/lib/gcs-connector-hadoop2-latest.jar
                if [ ! -f "${GCS_CONNECTOR_JAR}" ]; then
                    if command -v curl >/dev/null 2>&1; then
                        curl --retry 5 --retry-delay 10 --retry-all-errors -fLsS -o "${GCS_CONNECTOR_JAR}" \
                            https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar
                    elif command -v wget >/dev/null 2>&1; then
                        wget -nv -O "${GCS_CONNECTOR_JAR}" https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar
                    else
                        echo "Neither curl nor wget is available to fetch GCS connector jar" >&2
                        exit 1
                    fi
                fi
                cp -f "${GCS_CONNECTOR_JAR}" /opt/hive/lib/
                append_site_xml_properties() {
                    local site_xml=$1
                    local overrides_xml=$2
                    local tmp_xml
                    tmp_xml=$(mktemp)
                    sed '/<\\/configuration>/d' "${site_xml}" > "${tmp_xml}"
                    sed '1d;$d' "${overrides_xml}" >> "${tmp_xml}"
                    echo '</configuration>' >> "${tmp_xml}"
                    mv "${tmp_xml}" "${site_xml}"
                }
                append_site_xml_properties /opt/hadoop/etc/hadoop/core-site.xml "/docker/trino-product-tests/conf/environment/multinode-gcs/core-site-overrides.xml"
                append_site_xml_properties /opt/hive/conf/hive-site.xml "/docker/trino-product-tests/conf/environment/multinode-gcs/hive-site-overrides.xml"
                """;

        container.withCopyToContainer(
                Transferable.of(coreSiteOverrides),
                "/docker/trino-product-tests/conf/environment/multinode-gcs/core-site-overrides.xml");
        container.withCopyToContainer(
                Transferable.of(hiveSiteOverrides),
                "/docker/trino-product-tests/conf/environment/multinode-gcs/hive-site-overrides.xml");
        container.withCopyToContainer(
                Transferable.of(applyScript, 0777),
                "/etc/hadoop-init.d/zz-apply-gcs-config.sh");
        container.withCopyToContainer(
                Transferable.of(gcpCredentialsJson),
                HADOOP_GCP_CREDENTIALS_FILE);
    }

    private static String readConfigFile(String fileName)
    {
        Path configDir = locateGcsConfigDir();
        Path file = configDir.resolve(fileName);
        try {
            return Files.readString(file);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Unable to read GCS config file: " + file, e);
        }
    }

    private static Path locateGcsConfigDir()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(GCS_CONFIG_DIR);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate GCS config directory: " + GCS_CONFIG_DIR);
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
