package io.trino.plugin.teradata;

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
    public static DatabaseConfig fromEnv()
    {
        String host = System.getenv("hostname");
        String user = System.getenv("user");
        String pass = System.getenv("password");
        String database = System.getenv("database");
        if (database == null || database.isEmpty()) {
            database = "trino";
        }
        if (host == null || user == null || pass == null) {
            throw new IllegalStateException("Environment variables [hostname, user, password] must be set.");
        }
        String jdbcUrl = String.format("jdbc:teradata://%s/TMODE=ANSI,CHARSET=UTF8", host);
        return new DatabaseConfig(jdbcUrl, user, pass, database);
    }

    public static DatabaseConfig fromEnvWithClearScape()
    {
        DatabaseConfig config = fromEnv();
        config.setUseClearScape(Boolean.parseBoolean(
                System.getProperty("test.clearscape.enabled")));
        return config;
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
