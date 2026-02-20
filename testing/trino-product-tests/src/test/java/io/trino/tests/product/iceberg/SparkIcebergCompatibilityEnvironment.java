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
package io.trino.tests.product.iceberg;

import io.trino.testing.TestingProperties;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

/**
 * Spark/Iceberg environment with an additional compatibility Trino server.
 */
public class SparkIcebergCompatibilityEnvironment
        extends SparkIcebergEnvironment
{
    private static final int TRINO_HTTP_PORT = 8080;
    private static final int LEGACY_HIVE_CONNECTOR_RENAME_VERSION = 359;
    private static final int FIRST_TRINO_VERSION = 351;

    private static final String COMPATIBILITY_IMAGE_PROPERTY = "compatibility.testDockerImage";
    private static final String COMPATIBILITY_VERSION_PROPERTY = "compatibility.testVersion";

    private static final Pattern PROJECT_VERSION_PATTERN = Pattern.compile("(\\d+)(?:-SNAPSHOT)?");
    private static final Pattern NUMERIC_PREFIX_PATTERN = Pattern.compile("(\\d+).*");

    private TrinoContainer compatibilityTrino;
    private int compatibilityVersion;

    @Override
    public void start()
    {
        super.start();
        if (compatibilityTrino != null && compatibilityTrino.isRunning()) {
            return;
        }

        String image = System.getProperty(COMPATIBILITY_IMAGE_PROPERTY, defaultCompatibilityImage());
        compatibilityVersion = detectCompatibilityVersion(image);
        if (compatibilityVersion < FIRST_TRINO_VERSION) {
            throw new IllegalArgumentException(format(
                    "Spark Iceberg compatibility requires Trino image version >= %s, got %s (%s)",
                    FIRST_TRINO_VERSION,
                    compatibilityVersion,
                    image));
        }

        compatibilityTrino = createCompatibilityContainer(image, compatibilityVersion);
        compatibilityTrino.start();
        waitForCompatibilityServerReady();
    }

    public Connection createCompatibilityTrinoConnection()
            throws SQLException
    {
        return createCompatibilityTrinoConnection("hive");
    }

    public Connection createCompatibilityTrinoConnection(String user)
            throws SQLException
    {
        String jdbcUrl = getCompatibilityTrinoJdbcUrl();
        return DriverManager.getConnection(jdbcUrl, requireNonNull(user, "user is null"), null);
    }

    public QueryResult executeCompatibilityTrino(String sql)
    {
        try (Connection connection = createCompatibilityTrinoConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return QueryResult.forResultSet(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute query on compatibility test server: " + sql, e);
        }
    }

    public int executeCompatibilityTrinoUpdate(String sql)
    {
        try (Connection connection = createCompatibilityTrinoConnection();
                Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute update on compatibility test server: " + sql, e);
        }
    }

    public String getCompatibilityTrinoJdbcUrl()
    {
        if (compatibilityTrino == null) {
            return null;
        }
        return format(
                "jdbc:trino://%s:%s/iceberg/default",
                compatibilityTrino.getHost(),
                compatibilityTrino.getMappedPort(TRINO_HTTP_PORT));
    }

    @Override
    public boolean isRunning()
    {
        return super.isRunning() && compatibilityTrino != null && compatibilityTrino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (compatibilityTrino != null) {
            compatibilityTrino.close();
            compatibilityTrino = null;
        }
        super.doClose();
    }

    private TrinoContainer createCompatibilityContainer(String image, int version)
    {
        String configurationDirectory = getConfigurationDirectory(version);
        TrinoContainer container = new TrinoContainer(DockerImageName.parse(image))
                .withNetwork(getNetwork())
                .withNetworkAliases("compatibility-test-server", "compatibility-test-coordinator")
                .withExposedPorts(TRINO_HTTP_PORT)
                .withCopyToContainer(
                        Transferable.of(createIcebergCatalogProperties(version), 0644),
                        configurationDirectory + "catalog/iceberg.properties")
                .withCopyToContainer(
                        Transferable.of(createHiveCatalogProperties(version), 0644),
                        configurationDirectory + "catalog/hive.properties")
                .withCopyToContainer(
                        Transferable.of(getHadoopContainer().getHdfsClientSiteXml(), 0644),
                        "/etc/trino/hdfs-site.xml")
                .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
        return container;
    }

    private static String createIcebergCatalogProperties(int version)
    {
        StringBuilder properties = new StringBuilder()
                .append("connector.name=iceberg\n")
                .append("hive.metastore.uri=thrift://").append(HadoopContainer.HOST_NAME).append(':').append(HadoopContainer.HIVE_METASTORE_PORT).append('\n');
        if (version >= LEGACY_HIVE_CONNECTOR_RENAME_VERSION) {
            properties.append("fs.hadoop.enabled=true\n");
            properties.append("hive.config.resources=/etc/trino/hdfs-site.xml\n");
        }
        return properties.toString();
    }

    private static String createHiveCatalogProperties(int version)
    {
        String connectorName = version < LEGACY_HIVE_CONNECTOR_RENAME_VERSION ? "hive-hadoop2" : "hive";
        StringBuilder properties = new StringBuilder()
                .append("connector.name=").append(connectorName).append('\n')
                .append("hive.metastore.uri=thrift://").append(HadoopContainer.HOST_NAME).append(':').append(HadoopContainer.HIVE_METASTORE_PORT).append('\n')
                .append("hive.parquet.time-zone=UTC\n")
                .append("hive.rcfile.time-zone=UTC\n");
        if (version >= LEGACY_HIVE_CONNECTOR_RENAME_VERSION) {
            properties.append("fs.hadoop.enabled=true\n");
            properties.append("hive.config.resources=/etc/trino/hdfs-site.xml\n");
            properties.append("hive.hive-views.enabled=true\n");
        }
        return properties.toString();
    }

    private static String getConfigurationDirectory(int version)
    {
        if (version == 351) {
            return "/usr/lib/trino/default/etc/";
        }
        return "/etc/trino/";
    }

    private static int detectCompatibilityVersion(String image)
    {
        String configuredVersion = System.getProperty(COMPATIBILITY_VERSION_PROPERTY);
        if (configuredVersion != null && !configuredVersion.isBlank()) {
            return parseInt(configuredVersion);
        }

        String imageVersion = DockerImageName.parse(image).getVersionPart();
        Matcher matcher = NUMERIC_PREFIX_PATTERN.matcher(imageVersion);
        if (matcher.matches()) {
            return parseInt(matcher.group(1));
        }

        throw new IllegalArgumentException(format(
                "Cannot detect compatibility version from image '%s'; set -D%s=<version>",
                image,
                COMPATIBILITY_VERSION_PROPERTY));
    }

    private static String defaultCompatibilityImage()
    {
        Matcher matcher = PROJECT_VERSION_PATTERN.matcher(TestingProperties.getProjectVersion());
        if (!matcher.matches()) {
            throw new IllegalStateException("Unexpected project version: " + TestingProperties.getProjectVersion());
        }
        int currentVersion = parseInt(matcher.group(1));
        return "trinodb/trino:" + (currentVersion - 1);
    }

    private void waitForCompatibilityServerReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = createCompatibilityTrinoConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                if (resultSet.next()) {
                    return;
                }
            }
            catch (SQLException ignored) {
                // Retry until timeout.
            }

            try {
                sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for compatibility server readiness", e);
            }
        }

        throw new IllegalStateException("Compatibility server was not ready for queries within timeout");
    }
}
