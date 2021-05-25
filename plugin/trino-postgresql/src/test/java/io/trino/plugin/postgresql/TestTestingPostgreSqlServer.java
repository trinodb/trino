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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTestingPostgreSqlServer
{
    private final ExecutorService threadPool = newCachedThreadPool(daemonThreadsNamed("TestTestingPostgreSqlServer-%d"));

    private TestingPostgreSqlServer postgreSqlServer;

    @BeforeClass
    public void setUp()
    {
        postgreSqlServer = new TestingPostgreSqlServer();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(
                postgreSqlServer,
                threadPool::shutdownNow);
    }

    @Test
    public void testCapturingSuccessfulStatement()
    {
        String sql = "SELECT 1";
        RemoteDatabaseEvent event = new RemoteDatabaseEvent(sql, RUNNING);

        // verify query was not run before
        assertThat(postgreSqlServer.getRemoteDatabaseEvents()).doesNotContain(event);

        postgreSqlServer.execute(sql);
        // logging events is asynchronous, it may take some time till it is available
        assertEventually(() -> assertThat(postgreSqlServer.getRemoteDatabaseEvents()).contains(event));
    }

    @Test(timeOut = 60_000)
    public void testCapturingCancelledStatement()
            throws Exception
    {
        String sql = "SELECT pg_sleep(60)";

        // verify query was not run before
        RemoteDatabaseEvent running = new RemoteDatabaseEvent(sql, RUNNING);
        RemoteDatabaseEvent cancelled = new RemoteDatabaseEvent(sql, CANCELLED);
        assertThat(postgreSqlServer.getRemoteDatabaseEvents()).doesNotContain(running, cancelled);

        try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties());
                Statement statement = connection.createStatement()) {
            Future<Boolean> executeFuture = threadPool.submit(() -> statement.execute(sql));

            // wait for query to become running
            assertEventually(() -> assertThat(postgreSqlServer.getRemoteDatabaseEvents()).contains(running));

            // cancel the query
            statement.cancel();
            assertThatThrownBy(executeFuture::get)
                    .hasRootCauseInstanceOf(SQLException.class)
                    .hasRootCauseMessage("ERROR: canceling statement due to user request");
        }

        assertEventually(() -> assertThat(postgreSqlServer.getRemoteDatabaseEvents()).contains(cancelled));
    }
}
