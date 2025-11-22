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
package io.trino.plugin.teradata.integration.clearscape;

import java.util.concurrent.ThreadLocalRandom;

import static java.util.Locale.ENGLISH;

public final class ClearScapeEnvironmentUtils
{
    private static final int MAX_ENV_NAME_LENGTH = 40; // Adjust based on ClearScape limits

    private ClearScapeEnvironmentUtils() {}

    public static String generateUniqueEnvName(Class<?> testClass)
    {
        String className = testClass.getSimpleName().toLowerCase(ENGLISH);
        String suffix = Long.toString(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), 36);
        String envName = className + "-" + suffix;
        // Truncate if too long
        if (envName.length() > MAX_ENV_NAME_LENGTH) {
            envName = envName.substring(0, MAX_ENV_NAME_LENGTH);
        }
        return envName;
    }
}
