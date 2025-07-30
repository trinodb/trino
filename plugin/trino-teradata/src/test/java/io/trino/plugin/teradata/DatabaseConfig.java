package io.trino.plugin.teradata;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.plugin.teradata.clearscapeintegrations.TeradataConstants;

/**
 * Holds Teradata database connection configuration,
 * typically loaded from environment variables.
 */
public class DatabaseConfig
{
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private String databaseName;
    private boolean useClearScape;
    private ClearScapeManager clearScapeManager;
    private static final String TMODE = System.getenv("TMODE");
    private static final String CHARSET = System.getenv("CHARSET");


    public DatabaseConfig(String jdbcUrl, String username, String password, String databaseName)
    {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
    }

    /**
     * Loads config from environment variables:
     * hostname, user, password
     *
     * @throws IllegalStateException if required env vars are missing.
     */
    public ClearScapeManager getClearScapeManager() {
        return clearScapeManager;
    }
    public static DatabaseConfig fromEnvWithClearScape()
    {
        boolean useClearScape = Boolean.parseBoolean(
                System.getenv("test.clearscape.enabled"));
        ClearScapeManager clearScapeManager = null;

        String jdbcUrl;
        String username;
        String password;
        String databaseName;

        if (useClearScape) {
            // Handle ClearScape configuration similar to TestTeradataDatabase
            clearScapeManager = new ClearScapeManager();
            try {
                clearScapeManager.setup();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to setup ClearScape environment", e);
            }

            JsonNode configJSON = clearScapeManager.getConfigJSON();
            String dynamicHost = configJSON.get("host").asText();

            // Get credentials from ClearScape
            JsonNode authInfo = configJSON.get(TeradataConstants.LOG_MECH);
            username = authInfo.get("username").asText();
            password = authInfo.get("password").asText();

            // Use ClearScape username as database name
            databaseName = username;

            // Build JDBC URL with ClearScape host
            jdbcUrl = buildJdbcUrlWithHost(dynamicHost, databaseName);
        }
        else {
            // Handle regular environment variables
            String host = System.getenv("hostname");
            username = System.getenv("user");
            password = System.getenv("password");
            databaseName = System.getenv("database");

            if (databaseName == null || databaseName.isEmpty()) {
                databaseName = "trino";
            }

            if (host == null || username == null || password == null) {
                throw new IllegalStateException("Environment variables [hostname, user, password] must be set.");
            }

            jdbcUrl = buildJdbcUrl(host);
        }

        DatabaseConfig config = new DatabaseConfig(jdbcUrl, username, password, databaseName);
        config.setUseClearScape(useClearScape);
        config.clearScapeManager = clearScapeManager;
        return config;
    }

    private static String buildJdbcUrl(String host)
    {
        return String.format("jdbc:teradata://%s/TMODE = %s,CHARSET = %s", host, TMODE, CHARSET);
    }

    private static String buildJdbcUrlWithHost(String host, String databaseName)
    {
        return String.format("jdbc:teradata://%s/DBS_PORT=1025,DATABASE=%s,TMODE = %s,CHARSET = %s",
                host, databaseName, TMODE, CHARSET);
    }

    public boolean isUseClearScape()
    {
        return useClearScape;
    }

    public void setUseClearScape(boolean useClearScape)
    {
        this.useClearScape = useClearScape;
    }

    public String getJdbcUrl()
    {
        return jdbcUrl;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDatabaseName(String databaseName)
    {
        this.databaseName = databaseName;
    }
}
