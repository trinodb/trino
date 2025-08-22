package io.trino.plugin.teradata;

import io.trino.plugin.teradata.clearscape.ClearScapeSetup;
import io.trino.plugin.teradata.clearscape.Model;
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
        this.config = DatabaseConfigFactory.create(envName);
        String hostName = config.getHostName();
        // Initialize ClearScape Instance and Get the host name from ClearScape API in case config is using clearscape
        if (config.isUseClearScape()) {
            boolean destoryEnv = false;
            if (System.getenv("CLEARSCAPE_DESTORY_ENV") != null) {
                destoryEnv = Boolean.parseBoolean(System.getenv("CLEARSCAPE_DESTORY_ENV"));
            }
            this.clearScapeSetup = new ClearScapeSetup(
                    System.getenv("CLEARSCAPE_TOKEN"),
                    System.getenv("CLEARSCAPE_PASSWORD"),
                    config.getClearScapeEnvName(),
                    destoryEnv);
            Model model = this.clearScapeSetup.initialize();
            hostName = model.getHostName();
        }
        String jdbcUrl = buildJdbcUrl(hostName);
        this.config = config.toBuilder()
                .hostName(hostName)
                .jdbcUrl(jdbcUrl)
                .build();
        this.connection = createConnection();
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

        switch (config.getLogMech()) {
            case TD2:
                AuthenticationConfig auth = config.getAuthConfig();
                props.put("username", auth.getUserName());
                props.put("password", auth.getPassword());
                break;
            case BEARER:
                auth = config.getAuthConfig();
                props.put("jws_private_key", auth.getJwsPrivateKey());
                props.put("jws_cert", auth.getJwsCertificate());
                props.put("oidc_clientid", auth.getClientId());
                break;
            case JWT:
                props.put("logdata", "token=" + config.getAuthConfig().getJwtToken());
                break;
            case SECRET:
                props.put("oidc_clientid", config.getAuthConfig().getClientId());
                props.put("logdata", config.getAuthConfig().getClientSecret());
                break;
            default:
                throw new IllegalArgumentException("Unsupported logon mechanism: " + config.getLogMech());
        }

        return props;
    }

    public Map<String, String> getCatalogProperties()
    {
        Map<String, String> properties = new HashMap<>();
        properties.put("connection-url", config.getJdbcUrl());
        properties.put("logon-mechanism", config.getLogMech().getMechanism());

        switch (config.getLogMech()) {
            case TD2:
                AuthenticationConfig auth = config.getAuthConfig();
                properties.put("connection-user", auth.getUserName());
                properties.put("connection-password", auth.getPassword());
                break;
            case BEARER:
                auth = config.getAuthConfig();
                properties.put("oidc.client-id", auth.getClientId());
                properties.put("oidc.jws-private-key", auth.getJwsPrivateKey());
                properties.put("oidc.jws-certificate", auth.getJwsCertificate());
                break;
            case JWT:
                properties.put("jwt.token", config.getAuthConfig().getJwtToken());
                break;
            case SECRET:
                properties.put("oidc.client-id", config.getAuthConfig().getClientId());
                properties.put("oidc.client-secret", config.getAuthConfig().getClientSecret());
                break;
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

    // Getters
    public String getDatabaseName()
    {
        return config.getDatabaseName();
    }

    public String getTMode()
    {
        return config.getTMode();
    }

    public String getJdbcURL()
    {
        return config.getJdbcUrl();
    }

    public Connection getConnection()
    {
        return connection;
    }

    @Override
    public void close()
    {
        try {
            if (clearScapeSetup == null) {
                dropTestDatabaseIfExists();
            }
            if (!connection.isClosed()) {
                connection.close();
            }
            if (clearScapeSetup != null) {
                clearScapeSetup.cleanup();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to close connection", e);
        }
    }

    @Override
    public boolean supportsMultiRowInsert()
    {
        return false;
    }
}
