package io.trino.plugin.teradata;

import io.trino.plugin.teradata.clearscape.ClearScapeManager;
import io.trino.plugin.teradata.clearscape.Constants;
import io.trino.plugin.teradata.clearscape.Model;

public class DatabaseTestUtil
{
    private DatabaseTestUtil()
    {
        // Prevent instantiation
    }

    public static DatabaseConfig getDatabaseConfig()
    {
        boolean useClearScape = System.getenv("clearscape_token") != null && !System.getenv("clearscape_token").isEmpty();
        ClearScapeManager clearScapeManager = null;

        String jdbcUrl;
        String username;
        String password;
        String databaseName;

        if (useClearScape) {
            password = System.getenv("password");

            if (password == null) {
                throw new IllegalStateException("Environment variable password must be set for ClearScape.");
            }

            // Handle ClearScape configuration similar to TestTeradataDatabase
            clearScapeManager = new ClearScapeManager();
            Model model = new Model();
            model.setEnvName(Constants.ENV_CLEARSCAPE_NAME);
            model.setUserName(Constants.ENV_CLEARSCAPE_USERNAME);
            model.setPassword(password);
            model.setDatabaseName(Constants.ENV_CLEARSCAPE_USERNAME);
            model.setToken(System.getenv("clearscape_token"));
            clearScapeManager.init(model);
            try {
                clearScapeManager.setup();
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to setup ClearScape environment", e);
            }
            // Build JDBC URL with ClearScape host
            jdbcUrl = buildJdbcUrlWithHost(model);
            username = model.getUserName();
            databaseName = model.getDatabaseName();
            password = model.getPassword();
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
        if (clearScapeManager != null) {
            config.setClearScapeManager(clearScapeManager);
        }
        return config;
    }

    private static String buildJdbcUrl(String host)
    {
        Model model = new Model();
        model.setHostName(host);
        return buildJdbcUrlWithHost(model);
    }

    private static String buildJdbcUrlWithHost(Model model)
    {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append(String.format("jdbc:teradata://%s", model.getHostName()));

        boolean hasParams = false;
        String databaseName = model.getDatabaseName();
        // Add DATABASE only if databaseName is not null and not empty
        if (databaseName != null && !databaseName.isEmpty()) {
            urlBuilder.append("/DATABASE=").append(databaseName);
            hasParams = true;
        }
        return urlBuilder.toString();
    }
}
