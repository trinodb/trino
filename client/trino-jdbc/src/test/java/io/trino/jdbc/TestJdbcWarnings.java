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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.trino.client.Warning;
import io.trino.execution.warnings.WarningCollectorConfig;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.testing.TestingWarningCollector;
import io.trino.testing.TestingWarningCollectorConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestJdbcWarnings
{
    // Number of warnings preloaded to the testing warning collector before a query runs
    private static final int PRELOADED_WARNINGS = 5;

    private TestingTrinoServer server;
    private Connection connection;
    private Statement statement;
    private ExecutorService executor;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        server = TestingTrinoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("testing-warning-collector.add-warnings", "true")
                        .put("testing-warning-collector.preloaded-warnings", String.valueOf(PRELOADED_WARNINGS))
                        .buildOrThrow())
                .build();
        server.installPlugin(new BlackHolePlugin());
        server.createCatalog("blackhole", "blackhole");
        server.waitForNodeRefresh(Duration.ofSeconds(10));

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA blackhole.blackhole");
            statement.executeUpdate("" +
                    "CREATE TABLE slow_table (x int) " +
                    "WITH (" +
                    "   split_count = 1, " +
                    "   pages_per_split = 5, " +
                    "   rows_per_page = 3, " +
                    "   page_processing_delay = '1s'" +
                    ")");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        server.close();
    }

    @SuppressWarnings("JDBCResourceOpenedButNotSafelyClosed")
    @BeforeMethod
    public void setup()
            throws Exception
    {
        connection = createConnection();
        statement = connection.createStatement();
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        executor.shutdownNow();
        statement.close();
        connection.close();
    }

    @Test
    public void testStatementWarnings()
            throws SQLException
    {
        assertFalse(statement.execute("CREATE SCHEMA blackhole.test_schema"));
        SQLWarning warning = statement.getWarnings();
        assertNotNull(warning);
        TestingWarningCollectorConfig warningCollectorConfig = new TestingWarningCollectorConfig().setPreloadedWarnings(PRELOADED_WARNINGS);
        TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), warningCollectorConfig);
        List<TrinoWarning> expectedWarnings = warningCollector.getWarnings();
        assertStartsWithExpectedWarnings(warning, fromTrinoWarnings(expectedWarnings));
        statement.clearWarnings();
        assertNull(statement.getWarnings());
    }

    @Test
    public void testLongRunningStatement()
            throws Exception
    {
        Future<?> future = executor.submit(() -> {
            statement.execute("CREATE TABLE test_long_running AS SELECT * FROM slow_table");
            return null;
        });
        assertStatementWarnings(statement, future);
        statement.execute("DROP TABLE test_long_running");
    }

    @Test
    public void testLongRunningQuery()
            throws Exception
    {
        Future<?> future = executor.submit(() -> {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM slow_table");
            while (resultSet.next()) {
                // discard results
            }
            return null;
        });
        assertStatementWarnings(statement, future);
    }

    @Test
    public void testExecuteQueryWarnings()
            throws SQLException
    {
        try (ResultSet rs = statement.executeQuery("SELECT a FROM (VALUES 1, 2, 3) t(a)")) {
            assertNull(statement.getConnection().getWarnings());
            Set<WarningEntry> currentWarnings = new HashSet<>();
            assertWarnings(rs.getWarnings(), currentWarnings);
            while (rs.next()) {
                assertWarnings(statement.getWarnings(), currentWarnings);
            }

            TestingWarningCollectorConfig warningCollectorConfig = new TestingWarningCollectorConfig().setPreloadedWarnings(PRELOADED_WARNINGS).setAddWarnings(true);
            TestingWarningCollector warningCollector = new TestingWarningCollector(new WarningCollectorConfig(), warningCollectorConfig);
            List<TrinoWarning> expectedWarnings = warningCollector.getWarnings();
            for (TrinoWarning trinoWarning : expectedWarnings) {
                assertTrue(currentWarnings.contains(new WarningEntry(toTrinoSqlWarning(trinoWarning))));
            }
        }
    }

    @Test
    public void testSqlWarning()
    {
        ImmutableList.Builder<TrinoWarning> builder = ImmutableList.builder();
        for (int i = 0; i < 3; i++) {
            builder.add(new TrinoWarning(new WarningCode(i, "CODE_" + i), "warning message " + i));
        }
        List<TrinoWarning> warnings = builder.build();
        SQLWarning warning = fromTrinoWarnings(warnings);
        assertEquals(Iterators.size(warning.iterator()), warnings.size());
        assertWarningsEqual(warning, toTrinoSqlWarning(warnings.get(0)));
        assertWarningsEqual(warning.getNextWarning(), toTrinoSqlWarning(warnings.get(1)));
        assertWarningsEqual(warning.getNextWarning().getNextWarning(), toTrinoSqlWarning(warnings.get(2)));
    }

    private static void assertStatementWarnings(Statement statement, Future<?> future)
            throws Exception
    {
        // wait for initial warnings
        while (!future.isDone() && statement.getWarnings() == null) {
            Thread.sleep(100);
        }

        Set<WarningEntry> warnings = new HashSet<>();
        SQLWarning warning = statement.getWarnings();

        // collect initial set of warnings
        assertTrue(warnings.add(new WarningEntry(warning)));
        while (warning.getNextWarning() != null) {
            warning = warning.getNextWarning();
            assertTrue(warnings.add(new WarningEntry(warning)));
        }

        int initialSize = warnings.size();
        assertThat(initialSize).isGreaterThanOrEqualTo(PRELOADED_WARNINGS + 1);

        // collect additional warnings until query finish
        while (!future.isDone()) {
            if (warning.getNextWarning() == null) {
                Thread.sleep(100);
                continue;
            }
            warning = warning.getNextWarning();
            assertTrue(warnings.add(new WarningEntry(warning)));
        }

        int finalSize = warnings.size();
        assertThat(finalSize).isGreaterThan(initialSize);

        future.get();
    }

    private static SQLWarning fromTrinoWarnings(List<TrinoWarning> warnings)
    {
        requireNonNull(warnings, "warnings is null");
        assertFalse(warnings.isEmpty());
        Iterator<TrinoWarning> iterator = warnings.iterator();
        TrinoSqlWarning first = toTrinoSqlWarning(iterator.next());
        SQLWarning current = first;
        while (iterator.hasNext()) {
            current.setNextWarning(toTrinoSqlWarning(iterator.next()));
            current = current.getNextWarning();
        }
        return first;
    }

    private static TrinoSqlWarning toTrinoSqlWarning(TrinoWarning warning)
    {
        return new TrinoSqlWarning(toClientWarning(warning));
    }

    private static Warning toClientWarning(TrinoWarning warning)
    {
        WarningCode code = warning.getWarningCode();
        return new Warning(new Warning.Code(code.getCode(), code.getName()), warning.getMessage());
    }

    private static void assertWarningsEqual(SQLWarning actual, SQLWarning expected)
    {
        assertEquals(actual.getMessage(), expected.getMessage());
        assertEquals(actual.getSQLState(), expected.getSQLState());
        assertEquals(actual.getErrorCode(), expected.getErrorCode());
    }

    private static void addWarnings(Set<WarningEntry> currentWarnings, SQLWarning newWarning)
    {
        if (newWarning == null) {
            return;
        }
        for (Throwable warning : newWarning) {
            WarningEntry entry = new WarningEntry(warning);
            currentWarnings.add(entry);
        }
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = format("jdbc:trino://%s/blackhole/blackhole", server.getAddress());
        return DriverManager.getConnection(url, "test", null);
    }

    private static void assertWarnings(SQLWarning warning, Set<WarningEntry> currentWarnings)
    {
        if (warning == null) {
            return;
        }
        int previousSize = currentWarnings.size();
        addWarnings(currentWarnings, warning);
        assertTrue(currentWarnings.size() >= previousSize);
    }

    private static void assertStartsWithExpectedWarnings(SQLWarning warning, SQLWarning expected)
    {
        assertNotNull(expected);
        assertNotNull(warning);
        while (true) {
            assertWarningsEqual(warning, expected);
            warning = warning.getNextWarning();
            expected = expected.getNextWarning();
            if (expected == null) {
                return;
            }
            assertNotNull(warning);
        }
    }

    private static class WarningEntry
    {
        public final int vendorCode;
        public final String sqlState;
        public final String message;

        public WarningEntry(Throwable throwable)
        {
            requireNonNull(throwable, "throwable is null");
            assertTrue(throwable instanceof SQLWarning);
            SQLWarning warning = (SQLWarning) throwable;
            this.vendorCode = warning.getErrorCode();
            this.sqlState = requireNonNull(warning.getSQLState(), "SQLState is null");
            this.message = requireNonNull(warning.getMessage(), "message is null");
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other) {
                return true;
            }
            if (!(other instanceof WarningEntry)) {
                return false;
            }
            WarningEntry that = (WarningEntry) other;
            return vendorCode == that.vendorCode && sqlState.equals(that.sqlState) && message.equals(that.message);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(vendorCode, sqlState, message);
        }
    }
}
