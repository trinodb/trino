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
package io.trino.plugin.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public final class ConnectionMetadataUtils
{
    private static Boolean UPPERCASE;

    private ConnectionMetadataUtils() {}

    /**
     * Allows to leverage LazyConnectionFactory. This property is vendor specific.
     * It cannot be changed via configuration or at runtime. So we can cache its value.
     */
    public static boolean storesUpperCaseIdentifiers(Connection connection)
            throws SQLException
    {
        if (UPPERCASE != null) {
            return UPPERCASE;
        }
        boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
        UPPERCASE = uppercase;
        return uppercase;
    }
}
