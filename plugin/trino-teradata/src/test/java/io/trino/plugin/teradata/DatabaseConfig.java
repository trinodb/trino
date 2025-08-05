package io.trino.plugin.teradata;

import io.trino.plugin.teradata.clearscape.ClearScapeManager;

/**
 * Holds Teradata database connection configuration,
 * typically loaded from environment variables.
 */
public class DatabaseConfig
{
    private static final String TMODE = System.getenv("TMODE");
    private static final String CHARSET = System.getenv("CHARSET");
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private String databaseName;
    private boolean useClearScape;
    private ClearScapeManager clearScapeManager;

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
    public ClearScapeManager getClearScapeManager()
    {
        return clearScapeManager;
    }

    public void setClearScapeManager(ClearScapeManager clearScapeManager)
    {
        this.clearScapeManager = clearScapeManager;
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
