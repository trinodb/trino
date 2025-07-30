package io.trino.plugin.teradata.clearscapeintegrations;

public class ClearScapeEnvVariables
{
    private ClearScapeEnvVariables()
    {
        // Utility class - prevent instantiation
    }

    public static final String ENV_CLEARSCAPE_URL = "https://api.clearscape.teradata.com";
    public static final String ENV_CLEARSCAPE_USERNAME = "demo_user";
    public static final String ENV_CLEARSCAPE_PASSWORD = "Devtools@003";
    public static final String ENV_CLEARSCAPE_NAME = "trino-teradata-test-env";
    public static final String ENV_CLEARSCAPE_REGION = "asia-south";
}
