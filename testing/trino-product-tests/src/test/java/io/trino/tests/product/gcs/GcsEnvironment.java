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

import io.trino.testing.containers.FlociGcp;
import io.trino.testing.containers.Hive4MetastoreContainer;
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.tests.product.TableFormatsTestEnvironment;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static io.trino.testing.containers.environment.QueryRetry.executeWithRetry;
import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;
import static io.trino.tests.product.iceberg.IcebergCatalogPropertiesBuilder.icebergCatalog;

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
        implements TableFormatsTestEnvironment
{
    private static final String GCS_CONFIG_DIR =
            "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/environment/multinode-gcs";

    private static final String HIVE_GCP_CREDENTIALS_FILE = "/opt/hive/conf/gcp-credentials.json";
    private static final String SPARK_GCP_CREDENTIALS_FILE = "/spark/conf/gcp-credentials.json";

    private Network network;
    private FlociGcp flociGcp;
    private Hive4MetastoreContainer metastore;
    private SparkIcebergContainer spark;
    private MultiNodeTrinoCluster trinoCluster;
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
            throws Exception
    {
        if (isRunning()) {
            return;
        }

        String gcpStorageBucket = "trino-product-tests-" + UUID.randomUUID();
        String gcsTestDirectory = "env_multinode_gcs_" + UUID.randomUUID();
        warehouseDirectory = "gs://" + gcpStorageBucket + "/" + gcsTestDirectory;

        network = Network.newNetwork();

        flociGcp = new FlociGcp()
                .withNetwork(network);
        flociGcp.start();
        flociGcp.createBucket(gcpStorageBucket);
        String gcpCredentialsJson = flociGcp.getContainerServiceAccountJson();

        metastore = new Hive4MetastoreContainer("trino-hive4-java17-gcs:gcs-test")
                .withNetwork(network)
                .withNetworkAliases(Hive4MetastoreContainer.HOST_NAME)
                .withWarehouseDir(warehouseDirectory);
        metastore.dependsOn(flociGcp);
        configureMetastore(metastore, gcpCredentialsJson);
        metastore.start();

        spark = new SparkIcebergContainer("trino-spark4-iceberg-gcs:gcs-test")
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
        spark.dependsOn(metastore);
        String sparkDefaults = readConfigFile("spark-defaults.conf")
                .replace("%GCS_ENDPOINT%", flociGcp.getContainerEndpoint().toString())
                .replace("%GCP_PROJECT_ID%", FLOCI_GCP_PROJECT_ID)
                .replace("%GCS_WAREHOUSE%", warehouseDirectory)
                .replace("%HIVE_METASTORE_URI%", metastore.getInternalHiveMetastoreUri());
        spark.withCopyToContainer(Transferable.of(sparkDefaults), "/spark/conf/spark-defaults.conf");
        spark.withCopyToContainer(Transferable.of(gcpCredentialsJson), SPARK_GCP_CREDENTIALS_FILE);
        spark.start();

        String metastoreUri = metastore.getInternalHiveMetastoreUri();
        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withConfigProperty("node-scheduler.include-coordinator", "false")
                .withCatalog("hive", hiveCatalog(metastoreUri)
                        .put("fs.native-gcs.enabled", "true")
                        .put("gcs.endpoint", flociGcp.getContainerEndpoint().toString())
                        .put("gcs.json-key", "${ENV:GCP_CREDENTIALS}")
                        .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.parquet.time-zone", "UTC")
                        .put("hive.rcfile.time-zone", "UTC")
                        .build())
                .withCatalog("delta", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", metastoreUri,
                        "fs.native-gcs.enabled", "true",
                        "gcs.endpoint", flociGcp.getContainerEndpoint().toString(),
                        "gcs.json-key", "${ENV:GCP_CREDENTIALS}",
                        "gcs.project-id", FLOCI_GCP_PROJECT_ID))
                .withCatalog("iceberg", icebergCatalog(metastoreUri)
                        .put("iceberg.file-format", "PARQUET")
                        .put("fs.native-gcs.enabled", "true")
                        .put("gcs.endpoint", flociGcp.getContainerEndpoint().toString())
                        .put("gcs.json-key", "${ENV:GCP_CREDENTIALS}")
                        .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCoordinatorCustomizer(container -> container.withEnv("GCP_CREDENTIALS", gcpCredentialsJson))
                .withWorkerCustomizer(container -> container.withEnv("GCP_CREDENTIALS", gcpCredentialsJson))
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection("hive");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoCluster != null ? trinoCluster.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trinoCluster != null && trinoCluster.getCoordinator().isRunning();
    }

    public Connection createSparkConnection()
            throws SQLException
    {
        return DriverManager.getConnection(spark.getJdbcUrl(), "hive", "");
    }

    @Override
    public QueryResult executeSpark(@Language("SQL") String sql)
    {
        try {
            return executeWithRetry(() -> {
                try (Connection conn = createSparkConnection();
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(sql)) {
                    return QueryResult.forResultSet(rs);
                }
            });
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark query: " + sql, e);
        }
    }

    @Override
    public String getWarehouseDirectory()
    {
        return warehouseDirectory;
    }

    @Override
    protected void doClose()
    {
        if (trinoCluster != null) {
            trinoCluster.close();
            trinoCluster = null;
        }
        if (spark != null) {
            spark.close();
            spark = null;
        }
        if (metastore != null) {
            metastore.close();
            metastore = null;
        }
        if (flociGcp != null) {
            flociGcp.close();
            flociGcp = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private void configureMetastore(Hive4MetastoreContainer container, String gcpCredentialsJson)
    {
        String coreSiteOverrides = readConfigFile("core-site-overrides-template.xml")
                .replace("%GCP_CREDENTIALS_FILE_PATH%", HIVE_GCP_CREDENTIALS_FILE)
                .replace("%GCS_ENDPOINT%", flociGcp.getContainerEndpoint().toString())
                .replace("%GCP_PROJECT_ID%", FLOCI_GCP_PROJECT_ID);

        container.withCopyToContainer(
                Transferable.of(coreSiteOverrides),
                "/opt/hadoop/etc/hadoop/core-site.xml");
        container.withCopyToContainer(
                Transferable.of(gcpCredentialsJson),
                HIVE_GCP_CREDENTIALS_FILE);
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
}
