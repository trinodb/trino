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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;

/**
 * JUnit product-test environment for external Databricks + Glue + S3 Delta Lake lanes.
 */
public class DeltaLakeDatabricksEnvironment
        extends ProductTestEnvironment
{
    @FunctionalInterface
    protected interface ConnectionFactory
    {
        Connection create()
                throws SQLException;
    }

    static {
        try {
            Class.forName("com.databricks.client.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Databricks JDBC driver", e);
        }
    }

    private Network network;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return;
        }

        String awsRegion = requireEnv("AWS_REGION");
        String awsAccessKeyId = requireEnv("TRINO_AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = requireEnv("TRINO_AWS_SECRET_ACCESS_KEY");
        Optional<String> awsSessionToken = Optional.ofNullable(System.getenv("TRINO_AWS_SESSION_TOKEN"));

        network = Network.newNetwork();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", Map.ofEntries(
                        entry("connector.name", "hive"),
                        entry("hive.metastore", "glue"),
                        entry("hive.metastore.glue.region", "${ENV:AWS_REGION}"),
                        entry("fs.hadoop.enabled", "false"),
                        entry("fs.native-s3.enabled", "true"),
                        entry("s3.canned-acl", "BUCKET_OWNER_FULL_CONTROL"),
                        entry("hive.non-managed-table-writes-enabled", "true"),
                        entry("hive.hive-views.enabled", "true"),
                        entry("hive.delta-lake-catalog-name", "delta"),
                        entry("hive.parquet.time-zone", "UTC"),
                        entry("hive.rcfile.time-zone", "UTC")))
                .withCatalog("delta", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore", "glue",
                        "hive.metastore.glue.region", "${ENV:AWS_REGION}",
                        "fs.hadoop.enabled", "false",
                        "fs.native-s3.enabled", "true",
                        "s3.canned-acl", "BUCKET_OWNER_FULL_CONTROL",
                        "delta.enable-non-concurrent-writes", "true",
                        "delta.hive-catalog-name", "hive"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();

        trino.withEnv("AWS_REGION", awsRegion);
        trino.withEnv("AWS_ACCESS_KEY_ID", awsAccessKeyId);
        trino.withEnv("AWS_SECRET_ACCESS_KEY", awsSecretAccessKey);
        awsSessionToken.ifPresent(token -> trino.withEnv("AWS_SESSION_TOKEN", token));
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    protected String databricksJdbcUrl()
    {
        String rawUrl = appendJdbcOption(requireEnv("DATABRICKS_133_JDBC_URL"), "EnableArrow=0");
        return appendJdbcOption(rawUrl, "SocketTimeout=120");
    }

    protected final String databricksLogin()
    {
        return Optional.ofNullable(System.getenv("DATABRICKS_LOGIN"))
                .filter(login -> !login.isBlank())
                .orElse("token");
    }

    protected final String databricksToken()
    {
        return requireEnv("DATABRICKS_TOKEN");
    }

    public final String getBucketName()
    {
        return requireEnv("S3_BUCKET");
    }

    public QueryResult executeTrinoSql(String sql)
    {
        return executeSql(this::createTrinoConnection, sql, "Trino");
    }

    public List<QueryResult> executeTrinoSqlStatements(String... sqlStatements)
    {
        try (Connection connection = createTrinoConnection();
                Statement statement = connection.createStatement()) {
            List<QueryResult> results = new ArrayList<>(sqlStatements.length);
            for (String sql : sqlStatements) {
                boolean hasResultSet = statement.execute(sql);
                if (hasResultSet) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        results.add(QueryResult.forResultSet(resultSet));
                    }
                    continue;
                }

                int updateCount = statement.getUpdateCount();
                if (updateCount < 0) {
                    updateCount = 0;
                }
                results.add(QueryResult.forRows(List.of(List.of(updateCount))));
            }
            return results;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Trino statements: " + String.join(" | ", sqlStatements) + " (" + e.getMessage() + ")", e);
        }
    }

    public QueryResult executeDatabricksSql(String sql)
    {
        return executeSql(this::createDatabricksConnection, sql, "Databricks");
    }

    public List<QueryResult> executeDatabricksSqlStatements(String... sqlStatements)
    {
        try (Connection connection = createDatabricksConnection();
                Statement statement = connection.createStatement()) {
            List<QueryResult> results = new ArrayList<>(sqlStatements.length);
            for (String sql : sqlStatements) {
                boolean hasResultSet = statement.execute(sql);
                if (hasResultSet) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        results.add(QueryResult.forResultSet(resultSet));
                    }
                    continue;
                }

                int updateCount = statement.getUpdateCount();
                if (updateCount < 0) {
                    updateCount = 0;
                }
                results.add(QueryResult.forRows(List.of(List.of(updateCount))));
            }
            return results;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Databricks statements: " + String.join(" | ", sqlStatements) + " (" + e.getMessage() + ")", e);
        }
    }

    public GlueClient createGlueClient()
    {
        return GlueClient.builder()
                .region(Region.of(requireEnv("AWS_REGION")))
                .credentialsProvider(glueCredentialsProvider())
                .build();
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

    public Connection createDatabricksConnection()
            throws SQLException
    {
        return DriverManager.getConnection(databricksJdbcUrl(), databricksLogin(), databricksToken());
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

    private static QueryResult executeSql(ConnectionFactory connectionFactory, String sql, String engine)
    {
        try (Connection connection = connectionFactory.create();
                Statement statement = connection.createStatement()) {
            boolean hasResultSet = statement.execute(sql);
            if (hasResultSet) {
                try (ResultSet resultSet = statement.getResultSet()) {
                    return QueryResult.forResultSet(resultSet);
                }
            }

            int updateCount = statement.getUpdateCount();
            if (updateCount < 0) {
                updateCount = 0;
            }
            return QueryResult.forRows(List.of(List.of(updateCount)));
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute " + engine + " query: " + sql + " (" + e.getMessage() + ")", e);
        }
    }

    protected static String appendJdbcOption(String jdbcUrl, String option)
    {
        requireNonNull(jdbcUrl, "jdbcUrl is null");
        requireNonNull(option, "option is null");

        int separator = option.indexOf('=');
        if (separator < 0) {
            throw new IllegalArgumentException("JDBC option must be key=value: " + option);
        }

        String key = option.substring(0, separator) + "=";
        if (jdbcUrl.contains(key)) {
            return jdbcUrl;
        }

        if (jdbcUrl.endsWith(";")) {
            return jdbcUrl + option;
        }
        return jdbcUrl + ";" + option;
    }

    private static AwsCredentialsProvider glueCredentialsProvider()
    {
        String accessKey = requireEnv("TRINO_AWS_ACCESS_KEY_ID");
        String secretKey = requireEnv("TRINO_AWS_SECRET_ACCESS_KEY");
        Optional<String> sessionToken = Optional.ofNullable(System.getenv("TRINO_AWS_SESSION_TOKEN"))
                .filter(token -> !token.isBlank());
        if (sessionToken.isPresent()) {
            return StaticCredentialsProvider.create(AwsSessionCredentials.create(accessKey, secretKey, sessionToken.orElseThrow()));
        }
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
    }
}
