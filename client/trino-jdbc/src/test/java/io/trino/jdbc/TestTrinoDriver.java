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
package io.trino.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;

public class TestTrinoDriver
        extends BaseTrinoDriverTest
{
    @Override
    protected Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    @Override
    protected Connection createConnection(String catalog)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s", server.getAddress(), catalog);
        return DriverManager.getConnection(url, "test", null);
    }

    @Override
    protected Connection createConnection(String catalog, String schema)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        return DriverManager.getConnection(url, "test", null);
    }

    @Override
    protected Connection createConnectionWithParameter(String parameter)
            throws SQLException
    {
        String url = format("jdbc:trino://%s?%s", server.getAddress(), parameter);
        return DriverManager.getConnection(url, "test", null);
    }
}
