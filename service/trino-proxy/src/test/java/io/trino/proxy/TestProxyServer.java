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
package io.trino.proxy;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.Logging;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.execution.QueryState;
import io.trino.jdbc.TrinoResultSet;
import io.trino.jdbc.TrinoStatement;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.QueryState.FAILED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestProxyServer
{
    private Path sharedSecretFile;
    private TestingTrinoServer server;
    private LifeCycleManager lifeCycleManager;
    private HttpServerInfo httpServerInfo;
    private ExecutorService executorService;

    @BeforeAll
    public void setupServer()
            throws Exception
    {
        byte[] sharedSecret = Base64.getMimeEncoder().encode("test secret".getBytes(US_ASCII));
        sharedSecretFile = Files.createTempFile("secret", "txt");
        Files.write(sharedSecretFile, sharedSecret);

        Logging.initialize();
        server = TestingTrinoServer.create();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new TestingJmxModule(),
                new ProxyModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("proxy.uri", server.getBaseUrl().toString())
                .setRequiredConfigurationProperty("proxy.shared-secret-file", sharedSecretFile.toString())
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpServerInfo = injector.getInstance(HttpServerInfo.class);

        executorService = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));

        setupTestTable();
    }

    @AfterAll
    public void tearDownServer()
            throws IOException
    {
        server.close();
        server = null;
        lifeCycleManager.stop();
        executorService.shutdownNow();
        executorService = null;
        Files.delete(sharedSecretFile);
    }

    @Test
    public void testMetadata()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            assertThat(connection.getMetaData().getDatabaseProductVersion()).isEqualTo("testversion");
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT row_number() OVER () n FROM tpch.tiny.orders")) {
            long count = 0;
            long sum = 0;
            while (rs.next()) {
                count++;
                sum += rs.getLong("n");
            }
            assertThat(count).isEqualTo(15000);
            assertThat(sum).isEqualTo((count / 2) * (1 + count));
        }
    }

    @Test
    public void testSetSession()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate("SET SESSION query_max_run_time = '13s'");
            statement.executeUpdate("SET SESSION query_max_cpu_time = '42s'");
            Map<String, String> map = new HashMap<>();
            try (ResultSet rs = statement.executeQuery("SHOW SESSION")) {
                while (rs.next()) {
                    map.put(rs.getString(1), rs.getString(2));
                }
            }
            assertThat(map)
                    .containsEntry("query_max_run_time", "13s")
                    .containsEntry("query_max_cpu_time", "42s");
        }
    }

    @Test
    @Timeout(10)
    public void testCancel()
            throws Exception
    {
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryFinished = new CountDownLatch(1);
        AtomicReference<String> queryId = new AtomicReference<>();
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            // execute the slow query on another thread
            executorService.execute(() -> {
                try (ResultSet resultSet = statement.executeQuery("SELECT * FROM blackhole.test.slow")) {
                    queryId.set(resultSet.unwrap(TrinoResultSet.class).getQueryId());
                    queryStarted.countDown();
                    resultSet.next();
                }
                catch (SQLException t) {
                    queryFailure.set(t);
                }
                finally {
                    queryFinished.countDown();
                }
            });

            // start query and make sure it is not finished
            queryStarted.await(10, SECONDS);
            assertThat(queryId.get()).isNotNull();
            assertThat(getQueryState(queryId.get()).isDone()).isFalse();

            // cancel the query from this test thread
            statement.cancel();

            // make sure the query was aborted
            queryFinished.await(10, SECONDS);
            assertThat(queryFailure.get()).isNotNull();
            assertThat(getQueryState(queryId.get())).isEqualTo(FAILED);
        }
    }

    @Test
    @Timeout(1000)
    public void testPartialCancel()
            throws Exception
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM blackhole.test.slow")) {
            statement.unwrap(TrinoStatement.class).partialCancel();
            assertThat(resultSet.next()).isTrue();
            assertThat(resultSet.getLong(1)).isEqualTo(0);
        }
    }

    private QueryState getQueryState(String queryId)
            throws SQLException
    {
        String sql = format("SELECT state FROM system.runtime.queries WHERE query_id = '%s'", queryId);
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            assertThat(rs.next())
                    .describedAs("query not found")
                    .isTrue();
            return QueryState.valueOf(rs.getString("state"));
        }
    }

    private void setupTestTable()
            throws SQLException
    {
        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("CREATE SCHEMA blackhole.test")).isEqualTo(0);
            assertThat(statement.executeUpdate("CREATE TABLE blackhole.test.slow (x bigint) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 1, " +
                    "   rows_per_page = 1, " +
                    "   page_processing_delay = '1m'" +
                    ")"))
                    .isEqualTo(0);
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        URI uri = httpServerInfo.getHttpUri();
        String url = format("jdbc:trino://%s:%s", uri.getHost(), uri.getPort());
        return DriverManager.getConnection(url, "test", null);
    }
}
