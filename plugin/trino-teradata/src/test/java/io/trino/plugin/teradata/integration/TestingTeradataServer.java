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
package io.trino.plugin.teradata.integration;

import io.trino.plugin.teradata.integration.clearscape.ClearScapeSetup;
import io.trino.plugin.teradata.integration.clearscape.EnvironmentResponse;
import io.trino.plugin.teradata.integration.clearscape.Model;
import io.trino.testing.sql.SqlExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class TestingTeradataServer
        implements AutoCloseable, SqlExecutor
{
    private static final int MAX_RETRIES = 5;
    private static final long BASE_RETRY_DELAY_MS = 1500L;
    private static final long MAX_RETRY_DELAY_MS = 10_000L;
    private static final Random RANDOM = new Random();
    private static final int TERADATA_TRANSIENT_CONCURRENCY_ERROR_CODE = 3598;
    private static final int TERADATA_CLOSED_CONNECTION_ERROR_CODE = 1095;
    private static final int TERADATA_SOCKET_COMMUNICATION_FAILURE_ERROR_CODE = 804;

    private volatile Connection connection;
    private DatabaseConfig config;
    private ClearScapeSetup clearScapeSetup;

    public TestingTeradataServer(String envName, boolean destroyEnv)
    {
        requireNonNull(envName, "envName should not be null");
        config = DatabaseConfigFactory.create(envName);
        String hostName = config.getHostName();

        // Initialize ClearScape Instance and get hostname from ClearScape API when used
        if (config.isUseClearScape()) {
            if (isEnvSet("CLEARSCAPE_DESTROY_ENV")) {
                destroyEnv = Boolean.parseBoolean(requireEnv("CLEARSCAPE_DESTROY_ENV"));
            }
            clearScapeSetup = new ClearScapeSetup(
                    requireEnv("CLEARSCAPE_TOKEN"),
                    requireEnv("CLEARSCAPE_PASSWORD"),
                    config.getClearScapeEnvName(),
                    destroyEnv,
                    requireEnv("CLEARSCAPE_REGION"));
            Model model = clearScapeSetup.initialize();
            hostName = model.getHostName();
        }
        String jdbcUrl = buildJdbcUrl(hostName);
        config = config.toBuilder()
                .hostName(hostName)
                .jdbcUrl(jdbcUrl)
                .build();
        // Recreate the connection with retries to handle transient ClearScape socket or connection closure issues.
        connection = createConnectionWithRetries();
        createTestDatabaseIfAbsent();
    }

    public Map<String, String> fetchCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connection-url", config.getJdbcUrl());

        AuthenticationConfig auth = config.getAuthConfig();
        properties.put("connection-user", auth.userName());
        properties.put("connection-password", auth.password());

        return properties;
    }

    public void createTestDatabaseIfAbsent()
    {
        executeWithRetry(() -> {
            if (!schemaExists(config.getDatabaseName())) {
                execute(format("CREATE DATABASE \"%s\" AS PERM=100e6;", config.getDatabaseName()));
            }
        });
    }

    public void dropTestDatabaseIfExists()
    {
        executeWithRetry(() -> {
            if (schemaExists(config.getDatabaseName())) {
                execute(format("DELETE DATABASE \"%s\"", config.getDatabaseName()));
                execute(format("DROP DATABASE \"%s\"", config.getDatabaseName()));
            }
        });
    }

    public boolean tableExists(String tableName)
    {
        ensureConnection();
        String query = "SELECT count(1) FROM DBC.TablesV WHERE DataBaseName = ? AND TableName = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, config.getDatabaseName());
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            if (isConnectionException(e)) {
                connection = createConnectionWithRetries();
                try (PreparedStatement stmt = connection.prepareStatement(query)) {
                    stmt.setString(1, config.getDatabaseName());
                    stmt.setString(2, tableName);
                    try (ResultSet rs = stmt.executeQuery()) {
                        return rs.next() && rs.getInt(1) > 0;
                    }
                }
                catch (SQLException ex) {
                    throw new RuntimeException("Failed to check table existence: " + ex.getMessage(), ex);
                }
            }
            throw new RuntimeException("Failed to check table existence: " + e.getMessage(), e);
        }
    }

    @Override
    public void execute(String sql)
    {
        executeWithRetry(() -> doExecute(sql));
    }

    public String getDatabaseName()
    {
        return config.getDatabaseName();
    }

    public String getTMode()
    {
        return config.getTMode();
    }

    @Override
    public void close()
    {
        try {
            if (config.isUseClearScape()) {
                EnvironmentResponse.State state = clearScapeSetup.status();
                if (state == EnvironmentResponse.State.RUNNING) {
                    dropTestDatabaseIfExists();
                }
            }
            else {
                dropTestDatabaseIfExists();
            }
        }
        finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
            catch (SQLException ignored) {
            }
            connection = null;
            if (clearScapeSetup != null) {
                try {
                    clearScapeSetup.cleanup();
                }
                catch (Exception ignored) {
                }
            }
        }
    }

    @Override
    public boolean supportsMultiRowInsert()
    {
        return false;
    }

    private String buildJdbcUrl(String hostName)
    {
        String baseUrl = format("jdbc:teradata://%s/", hostName);
        String propertiesString = buildPropertiesString();
        return propertiesString.isEmpty() ? baseUrl : baseUrl + propertiesString;
    }

    private String buildPropertiesString()
    {
        Map<String, String> properties = config.getJdbcProperties();
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        return properties.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(java.util.stream.Collectors.joining(","));
    }

    private void doExecute(String sql)
    {
        ensureConnection();
        try (Statement stmt = connection.createStatement()) {
            if (config.getDatabaseName() != null && schemaExists(config.getDatabaseName())) {
                stmt.execute(format("DATABASE \"%s\"", config.getDatabaseName()));
            }
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("SQL execution failed: " + sql, e);
        }
    }

    private boolean schemaExists(String schemaName)
    {
        ensureConnection();
        String query = "SELECT COUNT(1) FROM DBC.DatabasesV WHERE DatabaseName = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schemaName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            if (isConnectionException(e)) {
                connection = createConnectionWithRetries();
                try (PreparedStatement stmt = connection.prepareStatement(query)) {
                    stmt.setString(1, schemaName);
                    try (ResultSet rs = stmt.executeQuery()) {
                        return rs.next() && rs.getInt(1) > 0;
                    }
                }
                catch (SQLException ex) {
                    throw new RuntimeException("Failed to check schema existence", ex);
                }
            }
            throw new RuntimeException("Failed to check schema existence", e);
        }
    }

    private synchronized void ensureConnection()
    {
        try {
            if (connection == null || connection.isClosed()) {
                connection = createConnectionWithRetries();
            }
        }
        catch (SQLException e) {
            connection = createConnectionWithRetries();
        }
    }

    private void executeWithRetry(Runnable operation)
    {
        int attempt = 0;

        while (true) {
            try {
                operation.run();
                return;
            }
            catch (RuntimeException e) {
                attempt++;
                Throwable cause = e.getCause();

                // Connection-related: recreate connection and retry
                if (cause instanceof SQLException sqlEx && isConnectionException(sqlEx) && attempt < MAX_RETRIES) {
                    connection = createConnectionWithRetries();
                    sleepUnchecked(computeBackoffDelay(attempt));
                    continue;
                }

                // Teradata transient concurrency error 3598: backoff & retry
                if (isTeradataError3598(e) && attempt < MAX_RETRIES) {
                    long delay = computeBackoffDelay(attempt);
                    sleepUnchecked(delay);
                    continue;
                }
                throw e;
            }
        }
    }

    private Connection createConnectionWithRetries()
    {
        int attempt = 0;
        while (true) {
            try {
                return createConnection();
            }
            catch (RuntimeException e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    throw new RuntimeException("Failed to create database connection after retries", e);
                }
                long delay = computeBackoffDelay(attempt);
                sleepUnchecked(delay);
            }
        }
    }

    private Connection createConnection()
    {
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
            Properties props = buildConnectionProperties(config.getAuthConfig());
            return DriverManager.getConnection(config.getJdbcUrl(), props);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to create database connection", e);
        }
    }

    private boolean isTeradataError3598(Throwable t)
    {
        if (t == null) {
            return false;
        }
        Throwable root = t;
        while (root.getCause() != null && !(root instanceof SQLException)) {
            root = root.getCause();
        }
        if (root instanceof SQLException sqlEx) {
            try {
                if (sqlEx.getErrorCode() == TERADATA_TRANSIENT_CONCURRENCY_ERROR_CODE) {
                    return true;
                }
            }
            catch (Exception ignored) {
            }
        }
        return false;
    }

    private boolean isConnectionException(SQLException e)
    {
        if (e == null) {
            return false;
        }
        try {
            int code = e.getErrorCode();
            if (code == TERADATA_CLOSED_CONNECTION_ERROR_CODE || code == TERADATA_SOCKET_COMMUNICATION_FAILURE_ERROR_CODE) {
                return true;
            }
        }
        catch (Exception ignored) {
        }

        try {
            return connection == null || connection.isClosed();
        }
        catch (SQLException ignored) {
        }

        return false;
    }

    private static Properties buildConnectionProperties(AuthenticationConfig auth)
    {
        Properties props = new Properties();
        props.setProperty("logmech", "TD2");
        props.setProperty("username", auth.userName());
        props.setProperty("password", auth.password());
        return props;
    }

    private static long computeBackoffDelay(int attempt)
    {
        // Calculates how long to wait before retrying an operation that failed
        long base = BASE_RETRY_DELAY_MS * (1L << Math.max(0, attempt - 1));
        long jitter = (long) (RANDOM.nextDouble() * BASE_RETRY_DELAY_MS);
        long delay = Math.min(base + jitter, MAX_RETRY_DELAY_MS);
        return Math.max(delay, BASE_RETRY_DELAY_MS);
    }

    private static void sleepUnchecked(long millis)
    {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry wait", ie);
        }
    }
}
