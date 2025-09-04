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

package io.trino.plugin.integration.clearscape;

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
