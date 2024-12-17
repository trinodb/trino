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
package io.trino.testing;

import static java.util.Objects.requireNonNull;

public final class SystemEnvironmentUtils
{
    private SystemEnvironmentUtils() {}

    /**
     * Get the named environment variable, throwing an exception if it is not set.
     */
    public static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
    }

    public static boolean isEnvSet(String variable)
    {
        return System.getenv(variable) != null;
    }
}
