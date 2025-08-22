package io.trino.plugin.teradata.clearscape;

public class ClearScapeEnvironmentUtils
{
    /**
     * Utility class for generating unique environment names for ClearScape tests.
     * The names are prefixed with "trino-test-" and truncated to fit within the
     * maximum length allowed by ClearScape.
     */
    private static final String PREFIX = "trino-test-";
    private static final int MAX_ENV_NAME_LENGTH = 30; // Adjust based on ClearScape limits

    private ClearScapeEnvironmentUtils()
    {
        // Prevent instantiation
    }

    public static String generateUniqueEnvName(Class<?> testClass)
    {
        String className = testClass.getSimpleName().toLowerCase();
        String envName = PREFIX + className;

        // Truncate if too long
        if (envName.length() > MAX_ENV_NAME_LENGTH) {
            envName = envName.substring(0, MAX_ENV_NAME_LENGTH);
        }

        return envName;
    }
}
