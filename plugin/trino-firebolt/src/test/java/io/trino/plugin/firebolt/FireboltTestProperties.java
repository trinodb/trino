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

package io.trino.plugin.firebolt;

import static java.util.Objects.requireNonNull;

public class FireboltTestProperties
{
    public static final String JDBC_ENDPOINT = requireSystemProperty("test.firebolt.jdbc.endpoint");
    public static final String JDBC_USER = requireSystemProperty("test.firebolt.jdbc.user");
    public static final String JDBC_PASSWORD = requireSystemProperty("test.firebolt.jdbc.password");

    private FireboltTestProperties()
    {
    }

    /**
     * Get the named system property, throwing an exception if it is not set.
     */
    private static String requireSystemProperty(String property)
    {
        return requireNonNull(System.getProperty(property), property + " is not set");
    }
}
