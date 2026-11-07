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
    private static final int MAX_ENV_NAME_LENGTH = 20;

    private ClearScapeEnvironmentUtils() {}

    public static String generateUniqueEnvName(Class<?> testClass)
    {
        String prefix = testClass.getSimpleName().toLowerCase(ENGLISH);
        String suffix = Long.toString(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE), 36);
        int suffixLength = 6;
        if (suffix.length() > suffixLength) {
            suffix = suffix.substring(0, suffixLength);
        }
        int prefixLength = MAX_ENV_NAME_LENGTH - suffixLength - 1;
        if (prefix.length() > prefixLength) {
            prefix = prefix.substring(0, prefixLength);
        }
        return prefix + "-" + suffix;
    }
}
