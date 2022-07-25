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
package io.trino.tests.product.utils;

import io.trino.jdbc.TrinoConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcDriverUtils
{
    public static void setRole(Connection connection, String role)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET ROLE " + role + " IN hive");
        }
    }

    public static String getSessionProperty(Connection connection, String key)
            throws SQLException
    {
        return getSessionProperty(connection, key, "Value");
    }

    public static String getSessionPropertyDefault(Connection connection, String key)
            throws SQLException
    {
        return getSessionProperty(connection, key, "Default");
    }

    private static String getSessionProperty(Connection connection, String key, String valueType)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW SESSION");
            while (rs.next()) {
                if (rs.getString("Name").equals(key)) {
                    return rs.getString(valueType);
                }
            }
        }
        return null;
    }

    public static void setSessionProperty(Connection connection, String key, String value)
            throws SQLException
    {
        @SuppressWarnings("resource")
        TrinoConnection trinoConnection = connection.unwrap(TrinoConnection.class);
        trinoConnection.setSessionProperty(key, value);
    }

    public static void resetSessionProperty(Connection connection, String key)
            throws SQLException
    {
        setSessionProperty(connection, key, getSessionPropertyDefault(connection, key));
    }

    private JdbcDriverUtils() {}
}
