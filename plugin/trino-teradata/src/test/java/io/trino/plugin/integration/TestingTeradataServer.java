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
package io.trino.plugin.integration;

import io.trino.plugin.integration.clearscape.ClearScapeSetup;
import io.trino.plugin.integration.clearscape.Model;
import io.trino.plugin.integration.clearscape.Region;
import io.trino.plugin.integration.util.TeradataTestConstants;
import io.trino.plugin.teradata.LogonMechanism;
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

import static io.trino.testing.SystemEnvironmentUtils.isEnvSet;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static java.util.Objects.requireNonNull;

public class TestingTeradataServer
        implements AutoCloseable, SqlExecutor
{
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private final Connection connection;
    private DatabaseConfig config;
    private ClearScapeSetup clearScapeSetup;

    public TestingTeradataServer(String envName)
    {
        config = DatabaseConfigFactory.create(envName);
        String hostName = config.getHostName();
        // Initialize ClearScape Instance and Get the host name from ClearScape API in case config is using clearscape
        if (config.isUseClearScape()) {
            boolean destroyEnv = false;
            if (isEnvSet("CLEARSCAPE_DESTROY_ENV")) {
                String destroyEnvValue = requireEnv("CLEARSCAPE_DESTROY_ENV");
                destroyEnv = Boolean.parseBoolean(destroyEnvValue);
            }
            String region = TeradataTestConstants.ENV_CLEARSCAPE_REGION;
            if (isEnvSet("CLEARSCAPE_REGION")) {
                String envRegion = requireEnv("CLEARSCAPE_REGION");
                if (Region.isValid(envRegion)) {
                    region = envRegion;
                }
            }
            clearScapeSetup = new ClearScapeSetup(
                    requireEnv("CLEARSCAPE_TOKEN"),
                    requireEnv("CLEARSCAPE_PASSWORD"),
                    config.getClearScapeEnvName(),
                    destroyEnv,
                    region);
            Model model = clearScapeSetup.initialize();
            hostName = model.getHostName();
        }
        String jdbcUrl = buildJdbcUrl(hostName);
        config = config.toBuilder()
                .hostName(hostName)
                .jdbcUrl(jdbcUrl)
                .build();
        connection = createConnection();
        createTestDatabaseIfAbsent();
    }

    private String buildJdbcUrl(String hostName)
    {
        String baseUrl = String.format("jdbc:teradata://%s/", hostName);
        String propertiesString = buildPropertiesString();
        if (propertiesString.isEmpty()) {
            return baseUrl;
        }
        return baseUrl + propertiesString;
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

    private Connection createConnection()
    {
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
            Properties props = buildConnectionProperties();
            return DriverManager.getConnection(config.getJdbcUrl(), props);
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to create database connection", e);
        }
    }

    private Properties buildConnectionProperties()
    {
        Properties props = new Properties();
        props.put("logmech", config.getLogMech().getMechanism());

        if (requireNonNull(config.getLogMech()) == LogonMechanism.TD2) {
            AuthenticationConfig auth = config.getAuthConfig();
            props.put("username", auth.userName());
            props.put("password", auth.password());
        }
        else {
            throw new IllegalArgumentException("Unsupported logon mechanism: " + config.getLogMech());
        }

        return props;
    }

    public Map<String, String> getCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connection-url", config.getJdbcUrl());
        properties.put("logon-mechanism", config.getLogMech().getMechanism());

        if (requireNonNull(config.getLogMech()) == LogonMechanism.TD2) {
            AuthenticationConfig auth = config.getAuthConfig();
            properties.put("connection-user", auth.userName());
            properties.put("connection-password", auth.password());
        }

        return properties;
    }

    public void createTestDatabaseIfAbsent()
    {
        executeWithRetry(() -> {
            if (!schemaExists(config.getDatabaseName())) {
                execute(String.format("CREATE DATABASE \"%s\" AS PERM=100e6;", config.getDatabaseName()));
            }
        });
    }

    public void dropTestDatabaseIfExists()
    {
        executeWithRetry(() -> {
            if (schemaExists(config.getDatabaseName())) {
                execute(String.format("DELETE DATABASE \"%s\"", config.getDatabaseName()));
                execute(String.format("DROP DATABASE \"%s\"", config.getDatabaseName()));
            }
        });
    }

    private void executeWithRetry(Runnable operation)
    {
        int attempts = 0;
        while (attempts < MAX_RETRIES) {
            try {
                operation.run();
                return; // Success, exit retry loop
            }
            catch (RuntimeException e) {
                if (isTeradataError3598(e) && attempts < MAX_RETRIES - 1) {
                    attempts++;
                    try {
                        Thread.sleep(RETRY_DELAY_MS * attempts); // Exponential backoff
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
                else {
                    throw e; // Re-throw if not error 3598 or max retries reached
                }
            }
        }
    }

    private boolean isTeradataError3598(Exception e)
    {
        if (e.getCause() instanceof SQLException sqlException) {
            return sqlException.getErrorCode() == 3598;
        }
        return false;
    }

    private boolean schemaExists(String schemaName)
    {
        String query = "SELECT COUNT(1) FROM DBC.DatabasesV WHERE DatabaseName = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schemaName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to check schema existence", e);
        }
    }

    public boolean isTableExists(String tableName)
    {
        String query = "SELECT count(1) FROM DBC.TablesV WHERE DataBaseName = ? AND TableName = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, config.getDatabaseName());
            stmt.setString(2, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to check table existence: " + e.getMessage(), e);
        }
    }

    @Override
    public void execute(String sql)
    {
        try (Statement stmt = connection.createStatement()) {
            if (config.getDatabaseName() != null && schemaExists(config.getDatabaseName())) {
                stmt.execute(String.format("DATABASE \"%s\"", config.getDatabaseName()));
            }
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("SQL execution failed: " + sql, e);
        }
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
            dropTestDatabaseIfExists();
            if (!connection.isClosed()) {
                connection.close();
            }
            if (clearScapeSetup != null) {
                clearScapeSetup.cleanup();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean supportsMultiRowInsert()
    {
        return false;
    }
}
