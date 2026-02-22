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
package io.trino.tests.product;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.testcontainers.utility.MountableFile.forHostPath;

/**
 * Environment for SuiteAllConnectorsSmoke.
 *
 * Mirrors launcher's EnvMultinodeAllConnectors catalog configuration so the
 * configured-features smoke test can verify connector registration fidelity.
 */
public class AllConnectorsSmokeEnvironment
        extends ProductTestEnvironment
{
    private static final String MULTINODE_ALL_CONFIG_DIR = "testing/trino-product-tests/src/test/resources/docker/trino-product-tests/conf/environment/multinode-all";
    private static final String CATALOG_TARGET_DIR = "/etc/trino/catalog";
    private static final String JVM_CONFIG_TARGET = "/etc/trino/jvm.config";

    private static final List<String> ADDITIONAL_CATALOGS = List.of(
            "bigquery",
            "cassandra",
            "clickhouse",
            "delta_lake",
            "druid",
            "duckdb",
            "elasticsearch",
            "faker",
            "gsheets",
            "hive",
            "hudi",
            "iceberg",
            "ignite",
            "kafka",
            "loki",
            "mariadb",
            "memory",
            "mongodb",
            "mysql",
            "opensearch",
            "oracle",
            "pinot",
            "postgresql",
            "prometheus",
            "redis",
            "redshift",
            "singlestore",
            "snowflake",
            "sqlserver",
            "tpcds",
            "trino_thrift",
            "vertica");

    private static final Path CONFIG_DIR = locateConfigDir();
    private static final Set<String> EXPECTED_CONNECTORS = loadExpectedConnectors();

    private Network network;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        network = Network.newNetwork();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("blackhole", Map.of("connector.name", "blackhole"))
                .withCatalog("jmx", Map.of("connector.name", "jmx"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();

        for (String catalog : ADDITIONAL_CATALOGS) {
            trino.withCopyToContainer(
                    Transferable.of(formatProperties(loadCatalogProperties(catalog))),
                    CATALOG_TARGET_DIR + "/" + catalog + ".properties");
        }

        // These files are referenced by gsheets/prometheus catalog configs.
        trino.withCopyFileToContainer(
                forHostPath(CONFIG_DIR.resolve("google-sheets-auth.json").toString()),
                CATALOG_TARGET_DIR + "/google-sheets-auth.json");
        trino.withCopyFileToContainer(
                forHostPath(CONFIG_DIR.resolve("prometheus-bearer.txt").toString()),
                CATALOG_TARGET_DIR + "/prometheus-bearer.txt");

        // Keep JVM settings aligned with launcher's multinode-all profile.
        trino.withCopyFileToContainer(
                forHostPath(CONFIG_DIR.resolve("jvm.config").toString()),
                JVM_CONFIG_TARGET);

        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    public Set<String> getConfiguredConnectors()
    {
        return EXPECTED_CONNECTORS;
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
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private static Set<String> loadExpectedConnectors()
    {
        Set<String> connectors = new LinkedHashSet<>();
        connectors.add("blackhole");
        connectors.add("jmx");
        connectors.add("tpch");
        connectors.add("system");

        for (String catalog : ADDITIONAL_CATALOGS) {
            Path catalogFile = CONFIG_DIR.resolve(catalog + ".properties");
            connectors.add(loadConnectorName(catalogFile));
        }
        return Set.copyOf(connectors);
    }

    private static String loadConnectorName(Path catalogFile)
    {
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(catalogFile)) {
            properties.load(input);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read catalog config: " + catalogFile, e);
        }
        String connectorName = properties.getProperty("connector.name");
        if (connectorName == null || connectorName.isBlank()) {
            throw new IllegalStateException("Missing connector.name in " + catalogFile);
        }
        return connectorName;
    }

    private static Properties loadCatalogProperties(String catalog)
    {
        Path catalogFile = CONFIG_DIR.resolve(catalog + ".properties");
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(catalogFile)) {
            properties.load(input);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read catalog config: " + catalogFile, e);
        }

        // Launcher layouts mount catalog assets under /docker/trino-product-tests/... while
        // JUnit environments place them under /etc/trino/catalog.
        properties.replaceAll((key, value) -> normalizeCatalogPath((String) value));

        // gsheets in launcher config still uses legacy property names; normalize for current plugin.
        if (catalog.equals("gsheets")) {
            String credentialsPath = properties.getProperty("gsheets.credentials-path");
            if (credentialsPath == null) {
                credentialsPath = properties.getProperty("credentials-path");
            }
            String metadataSheetId = properties.getProperty("gsheets.metadata-sheet-id");
            if (metadataSheetId == null) {
                metadataSheetId = properties.getProperty("metadata-sheet-id");
            }

            properties.remove("credentials-path");
            properties.remove("metadata-sheet-id");

            if (credentialsPath != null) {
                properties.setProperty("gsheets.credentials-path", normalizeCatalogPath(credentialsPath));
            }
            if (metadataSheetId != null) {
                properties.setProperty("gsheets.metadata-sheet-id", metadataSheetId);
            }
        }

        return properties;
    }

    private static String formatProperties(Properties properties)
    {
        StringBuilder builder = new StringBuilder();
        for (String key : new TreeSet<>(properties.stringPropertyNames())) {
            builder.append(key).append('=').append(properties.getProperty(key)).append('\n');
        }
        return builder.toString();
    }

    private static String normalizeCatalogPath(String value)
    {
        return value.replace("/docker/trino-product-tests/conf/trino/etc/catalog/", CATALOG_TARGET_DIR + "/");
    }

    private static Path locateConfigDir()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(MULTINODE_ALL_CONFIG_DIR);
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate launcher config directory: " + MULTINODE_ALL_CONFIG_DIR);
    }
}
