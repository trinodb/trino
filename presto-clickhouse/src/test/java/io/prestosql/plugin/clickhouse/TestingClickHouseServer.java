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
package io.prestosql.plugin.clickhouse;

import org.testcontainers.containers.ClickHouseContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestingClickHouseServer
        extends ClickHouseContainer
        implements Closeable
{
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";
    private static final String dockerImageName = ClickHouseContainer.IMAGE + ":latest";
    private final ClickHouseContainer dockerContainer;

    public TestingClickHouseServer()
    {
        this(dockerImageName);
    }

    public TestingClickHouseServer(String dockerImageName)
    {
        dockerContainer = new ClickHouseContainer(dockerImageName);
        // withDatabaseName(DATABASE);
        dockerContainer.start();
        waitUntilContainerStarted();
    }

    public void execute(String sql)
            throws SQLException
    {
        execute(getJdbcUrl(), sql);
    }

    private static void execute(String url, String sql)
            throws SQLException
    {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
