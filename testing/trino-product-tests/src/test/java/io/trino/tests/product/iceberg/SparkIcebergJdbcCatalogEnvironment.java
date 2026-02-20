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
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import static java.util.Map.entry;

/**
 * Spark/Iceberg product test environment with PostgreSQL-backed JDBC catalog.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS for data storage</li>
 *   <li>PostgreSQL container for Iceberg JDBC catalog metadata</li>
 *   <li>Spark container configured with JDBC catalog</li>
 *   <li>Trino container with JDBC catalog configuration</li>
 * </ul>
 * <p>
 * Unlike the standard SparkIcebergEnvironment which uses Hive Metastore,
 * this environment stores Iceberg metadata in PostgreSQL tables, enabling
 * testing of JDBC catalog functionality.
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "iceberg" catalog with JDBC backend</li>
 *   <li>Spark uses "iceberg_test" catalog with JDBC backend</li>
 *   <li>Both share the same PostgreSQL database for metadata</li>
 *   <li>HDFS is used for data storage</li>
 * </ul>
 */
public class SparkIcebergJdbcCatalogEnvironment
        extends ProductTestEnvironment
{
    static {
        // Ensure the Hive JDBC driver is loaded for Spark Thrift Server connections
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver. " +
                    "Ensure hive-apache-jdbc dependency is on the classpath.", e);
        }
    }

    private static final String POSTGRESQL_HOST_NAME = "postgresql";
    private static final int POSTGRESQL_PORT = 25432;
    private static final String POSTGRESQL_DATABASE = "test";
    private static final String POSTGRESQL_USER = "test";
    private static final String POSTGRESQL_PASSWORD = "test";

    private static final String ICEBERG_CATALOG_NAME = "iceberg_test";
    private static final String WAREHOUSE_DIR = "hdfs://hadoop-master:9000/user/hive/warehouse";

    private static final String POSTGRESQL_INIT_SQL = """
            CREATE TABLE IF NOT EXISTS iceberg_tables (
                catalog_name VARCHAR(255) NOT NULL,
                table_namespace VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                metadata_location VARCHAR(1000),
                previous_metadata_location VARCHAR(1000),
                PRIMARY KEY (catalog_name, table_namespace, table_name)
            );
            CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
                catalog_name VARCHAR(255) NOT NULL,
                namespace VARCHAR(255) NOT NULL,
                property_key VARCHAR(255) NOT NULL,
                property_value VARCHAR(1000),
                PRIMARY KEY (catalog_name, namespace, property_key)
            );
            """;

    private Network network;
    private HadoopContainer hadoop;
    private GenericContainer<?> postgresql;
    private SparkIcebergJdbcCatalogContainer spark;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop first (provides HDFS for data storage)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        // Start PostgreSQL for JDBC catalog metadata
        postgresql = createPostgresqlContainer()
                .withNetwork(network)
                .withNetworkAliases(POSTGRESQL_HOST_NAME);
        postgresql.start();

        // Start Spark with JDBC catalog configuration
        spark = new SparkIcebergJdbcCatalogContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
        spark.dependsOn(hadoop, postgresql);
        spark.start();

        // Start Trino with JDBC catalog configuration
        String jdbcUrl = "jdbc:postgresql://" + POSTGRESQL_HOST_NAME + ":" + POSTGRESQL_PORT + "/" + POSTGRESQL_DATABASE;
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("iceberg", Map.ofEntries(
                        entry("connector.name", "iceberg"),
                        entry("iceberg.catalog.type", "jdbc"),
                        entry("iceberg.jdbc-catalog.driver-class", "org.postgresql.Driver"),
                        entry("iceberg.jdbc-catalog.connection-url", jdbcUrl),
                        entry("iceberg.jdbc-catalog.connection-user", POSTGRESQL_USER),
                        entry("iceberg.jdbc-catalog.connection-password", POSTGRESQL_PASSWORD),
                        entry("iceberg.jdbc-catalog.catalog-name", ICEBERG_CATALOG_NAME),
                        entry("iceberg.jdbc-catalog.default-warehouse-dir", WAREHOUSE_DIR),
                        entry("hive.hdfs.socks-proxy", HadoopContainer.HOST_NAME + ":1180"),
                        entry("fs.hadoop.enabled", "true"),
                        entry("iceberg.allowed-extra-properties", "custom.table-property")))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    private GenericContainer<?> createPostgresqlContainer()
    {
        return new GenericContainer<>(DockerImageName.parse("postgres:14.2"))
                .withExposedPorts(POSTGRESQL_PORT)
                .withEnv("POSTGRES_PASSWORD", POSTGRESQL_PASSWORD)
                .withEnv("POSTGRES_USER", POSTGRESQL_USER)
                .withEnv("POSTGRES_DB", POSTGRESQL_DATABASE)
                .withEnv("PGPORT", String.valueOf(POSTGRESQL_PORT))
                .withCopyToContainer(
                        Transferable.of(POSTGRESQL_INIT_SQL),
                        "/docker-entrypoint-initdb.d/create-tables.sql")
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }

    // PostgreSQL connection methods

    /**
     * Creates a JDBC connection to the PostgreSQL database.
     */
    public Connection createPostgresqlConnection()
            throws SQLException
    {
        String jdbcUrl = "jdbc:postgresql://" + postgresql.getHost() + ":" +
                postgresql.getMappedPort(POSTGRESQL_PORT) + "/" + POSTGRESQL_DATABASE;
        return DriverManager.getConnection(jdbcUrl, POSTGRESQL_USER, POSTGRESQL_PASSWORD);
    }

    /**
     * Executes a SQL query against PostgreSQL and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executePostgresql(String sql)
    {
        try (Connection conn = createPostgresqlConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute PostgreSQL query: " + sql, e);
        }
    }

    /**
     * Executes a DDL or DML statement against PostgreSQL.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for DDL statements
     */
    public int executePostgresqlUpdate(String sql)
    {
        try (Connection conn = createPostgresqlConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute PostgreSQL update: " + sql, e);
        }
    }

    // Spark JDBC methods

    /**
     * Creates a JDBC connection to the Spark Thrift Server.
     */
    public Connection createSparkConnection()
            throws SQLException
    {
        return DriverManager.getConnection(spark.getJdbcUrl(), "hive", "");
    }

    /**
     * Executes a SQL query against Spark and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
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

    /**
     * Executes a DDL or DML statement against Spark.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for DDL statements
     */
    public int executeSparkUpdate(String sql)
    {
        try (Connection conn = createSparkConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark update: " + sql, e);
        }
    }

    // Standard ProductTestEnvironment methods

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
        return trino.getJdbcUrl();
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
        if (postgresql != null) {
            postgresql.close();
            postgresql = null;
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

    /**
     * Inner class for Spark container with JDBC catalog configuration.
     */
    private static class SparkIcebergJdbcCatalogContainer
            extends GenericContainer<SparkIcebergJdbcCatalogContainer>
    {
        private static final String DEFAULT_IMAGE = "ghcr.io/trinodb/testing/spark4-iceberg";
        public static final int SPARK_THRIFT_PORT = 10213;

        private static final String SPARK_DEFAULTS_CONF = """
                spark.sql.catalog.iceberg_test.cache-enabled=false
                spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
                spark.sql.hive.thriftServer.singleSession=false

                spark.sql.catalog.iceberg_test=org.apache.iceberg.spark.SparkCatalog
                spark.sql.catalog.iceberg_test.warehouse=hdfs://hadoop-master:9000/user/hive/warehouse
                spark.sql.catalog.iceberg_test.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog
                spark.sql.catalog.iceberg_test.uri=jdbc:postgresql://postgresql:25432/test
                spark.sql.catalog.iceberg_test.jdbc.user=test
                spark.sql.catalog.iceberg_test.jdbc.password=test

                spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000
                """;

        private static final String LOG4J2_PROPERTIES = """
                rootLogger.level = WARN
                rootLogger.appenderRef.console.ref = console
                appender.console.type = Console
                appender.console.name = console
                appender.console.target = SYSTEM_ERR
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
                """;

        public SparkIcebergJdbcCatalogContainer()
        {
            this(DEFAULT_IMAGE + ":" + TestingProperties.getDockerImagesVersion());
        }

        public SparkIcebergJdbcCatalogContainer(String imageName)
        {
            super(DockerImageName.parse(imageName));
            withExposedPorts(SPARK_THRIFT_PORT);
            withEnv("HADOOP_USER_NAME", "hive");
            withCopyToContainer(
                    Transferable.of(SPARK_DEFAULTS_CONF),
                    "/spark/conf/spark-defaults.conf");
            withCopyToContainer(
                    Transferable.of(LOG4J2_PROPERTIES),
                    "/spark/conf/log4j2.properties");
            withCommand(
                    "spark-submit",
                    "--master", "local[*]",
                    "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                    "--name", "Thrift JDBC/ODBC Server",
                    "--packages", "org.apache.spark:spark-avro_2.12:3.2.1",
                    "--conf", "spark.hive.server2.thrift.port=" + SPARK_THRIFT_PORT,
                    "spark-internal");
            waitingFor(Wait.forListeningPort()
                    .withStartupTimeout(Duration.ofMinutes(3)));
        }

        /**
         * Returns the JDBC URL for connecting to Spark Thrift Server from the host.
         */
        public String getJdbcUrl()
        {
            return "jdbc:hive2://" + getHost() + ":" + getMappedPort(SPARK_THRIFT_PORT);
        }
    }
}
