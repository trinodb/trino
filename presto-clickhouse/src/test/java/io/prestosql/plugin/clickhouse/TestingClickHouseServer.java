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

import io.airlift.log.Logger;
import org.testcontainers.containers.ClickHouseContainer;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestingClickHouseServer
        extends ClickHouseContainer
            implements Closeable
{
    private static final Logger LOG = Logger.get(TestingClickHouseServer.class);
    private static final String USER = "test";
    private static final String PASSWORD = "test";
    private static final String DATABASE = "tpch";
    private final AtomicBoolean tpchLoaded = new AtomicBoolean();
    private final CountDownLatch tpchLoadComplete = new CountDownLatch(1);

    public TestingClickHouseServer()
    {
        super("yandex/clickhouse-server:latest");

        start();

        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute("select 1");
            statement.execute("select 2");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

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

    public boolean isTpchLoaded()
    {
        return tpchLoaded.getAndSet(true);
    }

    public void setTpchLoaded()
    {
        tpchLoadComplete.countDown();
    }

    public void waitTpchLoaded()
            throws InterruptedException
    {
        tpchLoadComplete.await(2, TimeUnit.MINUTES);
    }

    @Override
    public void close()
    {
        stop();
    }
}
