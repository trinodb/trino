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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.plugin.jdbc.RemoteDatabaseEvent;
import net.jodah.failsafe.function.CheckedRunnable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.CANCELLED;
import static io.trino.plugin.jdbc.RemoteDatabaseEvent.Status.RUNNING;
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
                () -> threadPool.shutdownNow());
    }

    @Test
    public void testCapturingSuccessfulStatement()
            throws Throwable
    {
        String sql = "SELECT 1";
        Set<RemoteDatabaseEvent> remoteDatabaseEvents = captureRemoteEventsDuring(() -> postgreSqlServer.execute(sql));
        assertThat(remoteDatabaseEvents).contains(new RemoteDatabaseEvent(sql, RUNNING));
    }

    @Test(timeOut = 60_000)
    public void testCapturingCancelledStatement()
            throws Throwable
    {
        String sql = "SELECT pg_sleep(60)";
        Set<RemoteDatabaseEvent> remoteDatabaseEvents = captureRemoteEventsDuring(() -> {
            try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl(), postgreSqlServer.getProperties());
                    Statement statement = connection.createStatement()) {
                Future<Boolean> executeFuture = threadPool.submit(() -> statement.execute(sql));
                Thread.sleep(5000);
                statement.cancel();
                assertThatThrownBy(() -> executeFuture.get())
                        .hasRootCauseInstanceOf(SQLException.class)
                        .hasRootCauseMessage("ERROR: canceling statement due to user request");
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        assertThat(remoteDatabaseEvents).contains(new RemoteDatabaseEvent(sql, RUNNING));
        assertThat(remoteDatabaseEvents).contains(new RemoteDatabaseEvent(sql, CANCELLED));
    }

    private Set<RemoteDatabaseEvent> captureRemoteEventsDuring(CheckedRunnable runnable)
            throws Throwable
    {
        List<RemoteDatabaseEvent> before = postgreSqlServer.getRemoteDatabaseEvents();
        runnable.run();
        List<RemoteDatabaseEvent> after = postgreSqlServer.getRemoteDatabaseEvents();
        return Sets.difference(ImmutableSet.copyOf(after), ImmutableSet.copyOf(before));
    }
}
