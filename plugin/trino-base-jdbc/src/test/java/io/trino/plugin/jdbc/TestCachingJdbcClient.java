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

import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcProcedureHandle.ProcedureQuery;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
public class TestCachingJdbcClient
{
    private static final Duration FOREVER = Duration.succinctDuration(1, DAYS);
    private static final Duration ZERO = Duration.succinctDuration(0, MILLISECONDS);

    private static final ImmutableList<PropertyMetadata<?>> PROPERTY_METADATA = ImmutableList.of(
            stringProperty(
                    "session_name",
                    "Session name",
                    null,
                    false));

    private static final Set<SessionPropertiesProvider> SESSION_PROPERTIES_PROVIDERS = Set.of(() -> PROPERTY_METADATA);

    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(PROPERTY_METADATA)
            .build();

    private static final TableStatistics NON_EMPTY_STATS = TableStatistics.builder()
            .setRowCount(Estimate.zero())
            .build();

    private TestingDatabase database;
    private CachingJdbcClient cachingJdbcClient;
    private JdbcClient jdbcClient;
    private String schema;
    private ExecutorService executor;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        cachingJdbcClient = createCachingJdbcClient(true, 10000);
        jdbcClient = database.getJdbcClient();
        schema = jdbcClient.getSchemaNames(SESSION).iterator().next();
        executor = newCachedThreadPool(daemonThreadsNamed("TestCachingJdbcClient-%s"));
    }

    private CachingJdbcClient createCachingJdbcClient(
            Duration cacheTtl,
            boolean cacheMissing,
            long cacheMaximumSize)
    {
        return createCachingJdbcClient(cacheTtl, cacheTtl, cacheTtl, cacheMissing, cacheMaximumSize);
    }

    private CachingJdbcClient createCachingJdbcClient(
            Duration cacheTtl,
            Duration schemasCacheTtl,
            Duration tablesCacheTtl,
            boolean cacheMissing,
            long cacheMaximumSize)
    {
        return new CachingJdbcClient(
                database.getJdbcClient(),
                SESSION_PROPERTIES_PROVIDERS,
                new SingletonIdentityCacheMapping(),
                cacheTtl,
                schemasCacheTtl,
                tablesCacheTtl,
                cacheMissing,
                cacheMaximumSize);
    }

    private CachingJdbcClient createCachingJdbcClient(boolean cacheMissing, long cacheMaximumSize)
    {
        return createCachingJdbcClient(FOREVER, cacheMissing, cacheMaximumSize);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        executor.shutdownNow();
        executor = null;
        database.close();
        database = null;
    }

    @Test
    public void testSchemaNamesCached()
    {
        String phantomSchema = "phantom_schema";

        jdbcClient.createSchema(SESSION, phantomSchema);
        assertSchemaNamesCache(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getSchemaNames(SESSION)).contains(phantomSchema);
                });
        jdbcClient.dropSchema(SESSION, phantomSchema);

        assertThat(jdbcClient.getSchemaNames(SESSION)).doesNotContain(phantomSchema);
        assertSchemaNamesCache(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getSchemaNames(SESSION)).contains(phantomSchema);
                });
    }

    @Test
    public void testTableNamesCached()
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        createTable(phantomTable);
        assertTableNamesCache(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getTableNames(SESSION, Optional.of(schema))).contains(phantomTable);
                });
        dropTable(phantomTable);

        assertThat(jdbcClient.getTableNames(SESSION, Optional.of(schema))).doesNotContain(phantomTable);
        assertTableNamesCache(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getTableNames(SESSION, Optional.of(schema))).contains(phantomTable);
                });
    }

    @Test
    public void testTableHandleCached()
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        createTable(phantomTable);
        Optional<JdbcTableHandle> cachedTable = cachingJdbcClient.getTableHandle(SESSION, phantomTable);
        dropTable(phantomTable);

        assertThat(jdbcClient.getTableHandle(SESSION, phantomTable)).isEmpty();
        assertThat(cachingJdbcClient.getTableHandle(SESSION, phantomTable)).isEqualTo(cachedTable);
    }

    @Test
    public void testTableHandleOfQueryCached()
            throws Exception
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        createTable(phantomTable);
        PreparedQuery query = new PreparedQuery(format("SELECT * FROM %s.phantom_table", schema), ImmutableList.of());
        JdbcTableHandle cachedTable = assertTableHandleByQueryCache(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .calling(() -> cachingJdbcClient.getTableHandle(SESSION, query));
        assertCacheStats(cachingJdbcClient)
                // cache is not used, as the table handle has the columns list embedded
                .afterRunning(() -> {
                    cachingJdbcClient.getColumns(SESSION, cachedTable);
                });
        assertStatisticsCacheStats(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    cachingJdbcClient.getTableStatistics(SESSION, cachedTable);
                });
        dropTable(phantomTable); // not via CachingJdbcClient

        assertThatThrownBy(() -> jdbcClient.getTableHandle(SESSION, query))
                .hasMessageContaining("Failed to get table handle for prepared query");

        assertTableHandleByQueryCache(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getTableHandle(SESSION, query))
                            .isEqualTo(cachedTable);
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertCacheStats(cachingJdbcClient)
                // cache is not used, as the table handle has the columns list embedded
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertStatisticsCacheStats(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> {
                    cachingJdbcClient.getTableStatistics(SESSION, cachedTable);
                });

        cachingJdbcClient.createTable(SESSION, new ConnectorTableMetadata(phantomTable, emptyList()));

        assertTableHandleByQueryCache(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getTableHandle(SESSION, query))
                            .isEqualTo(cachedTable);
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertCacheStats(cachingJdbcClient)
                // cache is not used, as the table handle has the columns list embedded
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertStatisticsCacheStats(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    cachingJdbcClient.getTableStatistics(SESSION, cachedTable);
                });

        cachingJdbcClient.onDataChanged(phantomTable);

        assertTableHandleByQueryCache(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getTableHandle(SESSION, query))
                            .isEqualTo(cachedTable);
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertCacheStats(cachingJdbcClient)
                // cache is not used, as the table handle has the columns list embedded
                .afterRunning(() -> {
                    assertThat(cachingJdbcClient.getColumns(SESSION, cachedTable))
                            .hasSize(0); // phantom_table has no columns
                });
        assertStatisticsCacheStats(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .afterRunning(() -> {
                    cachingJdbcClient.getTableStatistics(SESSION, cachedTable);
                });

        dropTable(phantomTable);
    }

    @Test
    public void testProcedureHandleCached()
            throws Exception
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        createTable(phantomTable);
        createProcedure("test_procedure");

        ProcedureQuery query = new ProcedureQuery("CALL %s.test_procedure ('%s')".formatted(schema, phantomTable));
        JdbcProcedureHandle cachedProcedure = assertProcedureHandleByQueryCache(cachingJdbcClient)
                .misses(1)
                .loads(1)
                .calling(() -> cachingJdbcClient.getProcedureHandle(SESSION, query));
        assertThat(cachedProcedure.getColumns().orElseThrow())
                .hasSize(0);

        dropProcedure("test_procedure");

        assertThatThrownBy(() -> jdbcClient.getProcedureHandle(SESSION, query))
                .hasMessageContaining("Failed to get table handle for procedure query");

        assertProcedureHandleByQueryCache(cachingJdbcClient)
                .hits(1)
                .afterRunning(() -> assertThat(cachingJdbcClient.getProcedureHandle(SESSION, query))
                        .isEqualTo(cachedProcedure));

        dropTable(phantomTable);
    }

    @Test
    public void testTableHandleInvalidatedOnColumnsModifications()
    {
        JdbcTableHandle table = createTable(new SchemaTableName(schema, "a_table"));
        JdbcColumnHandle existingColumn = addColumn(table, "a_column");

        // warm-up cache
        assertTableHandlesByNameCacheIsInvalidated(table);
        JdbcColumnHandle newColumn = addColumn(cachingJdbcClient, table, "new_column");
        assertTableHandlesByNameCacheIsInvalidated(table);
        cachingJdbcClient.setColumnComment(SESSION, table, newColumn, Optional.empty());
        assertTableHandlesByNameCacheIsInvalidated(table);
        cachingJdbcClient.renameColumn(SESSION, table, newColumn, "new_column_name");
        assertTableHandlesByNameCacheIsInvalidated(table);
        cachingJdbcClient.dropColumn(SESSION, table, existingColumn);
        assertTableHandlesByNameCacheIsInvalidated(table);

        dropTable(table);
    }

    private void assertTableHandlesByNameCacheIsInvalidated(JdbcTableHandle table)
    {
        SchemaTableName tableName = table.asPlainTable().getSchemaTableName();

        assertTableHandleByNameCache(cachingJdbcClient).misses(1).loads(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(SESSION, tableName).orElseThrow()).isEqualTo(table);
        });
        assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(SESSION, tableName).orElseThrow()).isEqualTo(table);
        });
    }

    @Test
    public void testEmptyTableHandleIsCachedWhenCacheMissingIsTrue()
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        assertThat(cachingJdbcClient.getTableHandle(SESSION, phantomTable)).isEmpty();

        createTable(phantomTable);
        assertThat(cachingJdbcClient.getTableHandle(SESSION, phantomTable)).isEmpty();
        dropTable(phantomTable);
    }

    @Test
    public void testEmptyTableHandleNotCachedWhenCacheMissingIsFalse()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(false, 10000);
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        assertThat(cachingJdbcClient.getTableHandle(SESSION, phantomTable)).isEmpty();

        createTable(phantomTable);
        assertThat(cachingJdbcClient.getTableHandle(SESSION, phantomTable)).isPresent();
        dropTable(phantomTable);
    }

    private JdbcTableHandle createTable(SchemaTableName phantomTable)
    {
        jdbcClient.createTable(SESSION, new ConnectorTableMetadata(phantomTable, emptyList()));
        return jdbcClient.getTableHandle(SESSION, phantomTable).orElseThrow();
    }

    private void createProcedure(String procedureName)
            throws SQLException
    {
        try (Statement statement = database.getConnection().createStatement()) {
            statement.execute("CREATE ALIAS %s.%s FOR \"io.trino.plugin.jdbc.TestCachingJdbcClient.generateData\"".formatted(schema, procedureName));
        }
    }

    private void dropProcedure(String procedureName)
            throws SQLException
    {
        try (Statement statement = database.getConnection().createStatement()) {
            statement.execute("DROP ALIAS %s.%s".formatted(schema, procedureName));
        }
    }

    // Used by H2 for executing Stored Procedure
    public static ResultSet generateData(Connection connection, String table)
            throws SQLException
    {
        return connection.createStatement().executeQuery("SELECT * FROM " + table);
    }

    private void dropTable(JdbcTableHandle tableHandle)
    {
        jdbcClient.dropTable(SESSION, tableHandle);
    }

    private void dropTable(SchemaTableName phantomTable)
    {
        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(SESSION, phantomTable).orElseThrow();
        jdbcClient.dropTable(SESSION, tableHandle);
    }

    @Test
    public void testColumnsCached()
    {
        JdbcTableHandle table = getAnyTable(schema);
        JdbcColumnHandle phantomColumn = addColumn(table);

        // Read column into cache
        assertColumnCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(SESSION, table)).contains(phantomColumn);
        });

        jdbcClient.dropColumn(SESSION, table, phantomColumn);

        // Load column from cache
        assertThat(jdbcClient.getColumns(SESSION, table)).doesNotContain(phantomColumn);

        assertColumnCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(SESSION, table)).contains(phantomColumn);
        });
    }

    @Test
    public void testColumnsCachedPerSession()
    {
        ConnectorSession firstSession = createSession("first");
        ConnectorSession secondSession = createSession("second");
        JdbcTableHandle table = getAnyTable(schema);
        JdbcColumnHandle phantomColumn = addColumn(table);

        // Load columns in first session scope
        assertColumnCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, table)).contains(phantomColumn);
        });

        // Load columns in second session scope
        assertColumnCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(secondSession, table)).contains(phantomColumn);
        });

        // Check that columns are cached
        assertColumnCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(secondSession, table)).contains(phantomColumn);
        });

        // Drop first column and invalidate cache for all sessions
        cachingJdbcClient.dropColumn(firstSession, table, phantomColumn);
        assertThat(jdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);

        // Load columns into cache in both sessions scope
        assertColumnCacheStats(cachingJdbcClient).loads(2).misses(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, table)).doesNotContain(phantomColumn);
        });

        // Read columns from cache
        assertColumnCacheStats(cachingJdbcClient).hits(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, table)).doesNotContain(phantomColumn);
        });
    }

    @Test
    public void testColumnsCacheInvalidationOnTableDrop()
    {
        ConnectorSession firstSession = createSession("first");
        ConnectorSession secondSession = createSession("second");
        JdbcTableHandle firstTable = createTable(new SchemaTableName(schema, "first_table"));
        JdbcTableHandle secondTable = createTable(new SchemaTableName(schema, "second_table"));

        JdbcColumnHandle firstColumn = addColumn(firstTable, "first_column");
        JdbcColumnHandle secondColumn = addColumn(secondTable, "second_column");

        // Load columns for both tables into cache and assert cache loads (sessions x tables)
        assertColumnCacheStats(cachingJdbcClient).loads(4).misses(4).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).contains(firstColumn);
            assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).contains(secondColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).contains(firstColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        });

        // Load columns from cache
        assertColumnCacheStats(cachingJdbcClient).hits(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).contains(firstColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        });

        // Rename column
        cachingJdbcClient.renameColumn(firstSession, firstTable, firstColumn, "another_column");

        assertColumnCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(secondSession, firstTable))
                    .doesNotContain(firstColumn)
                    .containsAll(jdbcClient.getColumns(SESSION, firstTable));
        });

        // Drop columns and caches for first table
        cachingJdbcClient.dropTable(secondSession, firstTable);

        assertColumnCacheStats(cachingJdbcClient).loads(2).misses(2).afterRunning(() -> {
            assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, firstTable)).isInstanceOf(TableNotFoundException.class);
            assertThatThrownBy(() -> cachingJdbcClient.getColumns(secondSession, firstTable)).isInstanceOf(TableNotFoundException.class);
        });

        // Check if second table is still cached
        assertColumnCacheStats(cachingJdbcClient).hits(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).contains(secondColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        });

        cachingJdbcClient.dropTable(secondSession, secondTable);
    }

    @Test
    public void testColumnsNotCachedWhenCacheDisabled()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(ZERO, true, 10000);
        ConnectorSession firstSession = createSession("first");
        ConnectorSession secondSession = createSession("second");

        JdbcTableHandle firstTable = createTable(new SchemaTableName(schema, "first_table"));
        JdbcTableHandle secondTable = createTable(new SchemaTableName(schema, "second_table"));
        JdbcColumnHandle firstColumn = addColumn(firstTable, "first_column");
        JdbcColumnHandle secondColumn = addColumn(secondTable, "second_column");

        assertColumnCacheStats(cachingJdbcClient).loads(4).misses(4).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).containsExactly(firstColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).containsExactly(firstColumn);
            assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).containsExactly(secondColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).containsExactly(secondColumn);
        });

        assertColumnCacheStats(cachingJdbcClient).loads(4).misses(4).afterRunning(() -> {
            assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).containsExactly(firstColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).containsExactly(firstColumn);
            assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).containsExactly(secondColumn);
            assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).containsExactly(secondColumn);
        });

        // Drop tables by not using caching jdbc client
        jdbcClient.dropTable(SESSION, firstTable);
        jdbcClient.dropTable(SESSION, secondTable);

        // Columns are loaded bypassing a cache
        assertColumnCacheStats(cachingJdbcClient).loads(2).misses(2).afterRunning(() -> {
            assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, firstTable)).isInstanceOf(TableNotFoundException.class);
            assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, secondTable)).isInstanceOf(TableNotFoundException.class);
        });
    }

    @Test
    public void testGetTableStatistics()
    {
        CachingJdbcClient cachingJdbcClient = cachingStatisticsAwareJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("first");

        JdbcTableHandle first = createTable(new SchemaTableName(schema, "first"));
        JdbcTableHandle second = createTable(new SchemaTableName(schema, "second"));

        // load first
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first)).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first)).isEqualTo(NON_EMPTY_STATS);
        });

        // load second
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, second)).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first)).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate first
        cachingJdbcClient.dropTable(SESSION, first);
        JdbcTableHandle secondFirst = createTable(new SchemaTableName(schema, "first"));

        // load first again
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // cleanup
        this.jdbcClient.dropTable(SESSION, first);
        this.jdbcClient.dropTable(SESSION, second);
    }

    @Test
    public void testCacheGetTableStatisticsWithQueryRelationHandle()
    {
        CachingJdbcClient cachingJdbcClient = cachingStatisticsAwareJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("some test session name");

        JdbcTableHandle first = createTable(new SchemaTableName(schema, "first"));
        JdbcTableHandle second = createTable(new SchemaTableName(schema, "second"));
        JdbcTableHandle queryOnFirst = new JdbcTableHandle(
                new JdbcQueryRelationHandle(new PreparedQuery("SELECT * FROM first", List.of())),
                TupleDomain.all(),
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.of(Set.of(new SchemaTableName(schema, "first"))),
                0,
                Optional.empty());

        // load
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate 'second'
        cachingJdbcClient.dropTable(SESSION, second);

        // read from cache again (no invalidation)
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate 'first'
        cachingJdbcClient.dropTable(SESSION, first);

        // load again
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst)).isEqualTo(NON_EMPTY_STATS);
        });
    }

    @Test
    public void testTruncateTable()
    {
        CachingJdbcClient cachingJdbcClient = cachingStatisticsAwareJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("table");

        JdbcTableHandle table = createTable(new SchemaTableName(schema, "table"));

        // load
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate
        cachingJdbcClient.truncateTable(SESSION, table);

        // load again
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(NON_EMPTY_STATS);
        });

        // cleanup
        this.jdbcClient.dropTable(SESSION, table);
    }

    private CachingJdbcClient cachingStatisticsAwareJdbcClient(Duration duration, boolean cacheMissing, long cacheMaximumSize)
    {
        JdbcClient jdbcClient = database.getJdbcClient();
        JdbcClient statsAwareJdbcClient = new ForwardingJdbcClient()
        {
            @Override
            protected JdbcClient delegate()
            {
                return jdbcClient;
            }

            @Override
            public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
            {
                return NON_EMPTY_STATS;
            }
        };
        return new CachingJdbcClient(statsAwareJdbcClient, SESSION_PROPERTIES_PROVIDERS, new SingletonIdentityCacheMapping(), duration, cacheMissing, cacheMaximumSize);
    }

    @Test
    public void testCacheEmptyStatistics()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("table");
        JdbcTableHandle table = createTable(new SchemaTableName(schema, "table"));

        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(TableStatistics.empty());
        });

        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(TableStatistics.empty());
        });

        // cleanup
        this.jdbcClient.dropTable(SESSION, table);
    }

    @Test
    public void testGetTableStatisticsDoNotCacheEmptyWhenCachingMissingIsDisabled()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(FOREVER, false, 10000);
        ConnectorSession session = createSession("table");
        JdbcTableHandle table = createTable(new SchemaTableName(schema, "table"));

        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(TableStatistics.empty());
        });

        assertStatisticsCacheStats(cachingJdbcClient).loads(1).hits(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table)).isEqualTo(TableStatistics.empty());
        });

        // cleanup
        this.jdbcClient.dropTable(SESSION, table);
    }

    @Test
    public void testDifferentIdentityKeys()
    {
        CachingJdbcClient cachingJdbcClient = new CachingJdbcClient(
                database.getJdbcClient(),
                SESSION_PROPERTIES_PROVIDERS,
                new ExtraCredentialsBasedIdentityCacheMapping(new ExtraCredentialConfig()
                        .setUserCredentialName("user")
                        .setPasswordCredentialName("password")),
                FOREVER,
                true,
                10000);
        ConnectorSession alice = createUserSession("alice");
        ConnectorSession bob = createUserSession("bob");

        JdbcTableHandle table = createTable(new SchemaTableName(schema, "table"));

        assertTableNamesCache(cachingJdbcClient).loads(2).misses(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableNames(alice, Optional.empty())).contains(table.getRequiredNamedRelation().getSchemaTableName());
            assertThat(cachingJdbcClient.getTableNames(bob, Optional.empty())).contains(table.getRequiredNamedRelation().getSchemaTableName());
        });

        assertTableNamesCache(cachingJdbcClient).hits(2).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableNames(alice, Optional.empty())).contains(table.getRequiredNamedRelation().getSchemaTableName());
            assertThat(cachingJdbcClient.getTableNames(bob, Optional.empty())).contains(table.getRequiredNamedRelation().getSchemaTableName());
        });

        // Drop tables by not using caching jdbc client
        jdbcClient.dropTable(SESSION, table);
    }

    @Test
    public void testFlushCache()
    {
        CachingJdbcClient cachingJdbcClient = cachingStatisticsAwareJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("asession");

        JdbcTableHandle first = createTable(new SchemaTableName(schema, "atable"));

        // load table
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first)).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first)).isEqualTo(NON_EMPTY_STATS);
        });

        // flush cache
        cachingJdbcClient.flushCache();
        JdbcTableHandle secondFirst = createTable(new SchemaTableName(schema, "first"));

        // load table again
        assertStatisticsCacheStats(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // read table from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst)).isEqualTo(NON_EMPTY_STATS);
        });

        // cleanup
        jdbcClient.dropTable(SESSION, first);
    }

    @Test(timeOut = 60_000)
    public void testConcurrentSchemaCreateAndDrop()
    {
        CachingJdbcClient cachingJdbcClient = cachingStatisticsAwareJdbcClient(FOREVER, true, 10000);
        ConnectorSession session = createSession("asession");
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executor.submit(() -> {
                String schemaName = "schema_" + randomNameSuffix();
                assertThat(cachingJdbcClient.getSchemaNames(session)).doesNotContain(schemaName);
                cachingJdbcClient.createSchema(session, schemaName);
                assertThat(cachingJdbcClient.getSchemaNames(session)).contains(schemaName);
                cachingJdbcClient.dropSchema(session, schemaName);
                assertThat(cachingJdbcClient.getSchemaNames(session)).doesNotContain(schemaName);
                return null;
            }));
        }

        futures.forEach(Futures::getUnchecked);
    }

    @Test(timeOut = 60_000)
    public void testLoadFailureNotSharedWhenDisabled()
            throws Exception
    {
        AtomicBoolean first = new AtomicBoolean(true);
        CyclicBarrier barrier = new CyclicBarrier(2);

        CachingJdbcClient cachingJdbcClient = new CachingJdbcClient(
                new ForwardingJdbcClient()
                {
                    private final JdbcClient delegate = database.getJdbcClient();

                    @Override
                    protected JdbcClient delegate()
                    {
                        return delegate;
                    }

                    @Override
                    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
                    {
                        if (first.compareAndSet(true, false)) {
                            // first
                            try {
                                // increase chances that second thread calls cache.get before we return
                                Thread.sleep(5);
                            }
                            catch (InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                            throw new RuntimeException("first attempt is poised to fail");
                        }
                        return super.getTableHandle(session, schemaTableName);
                    }
                },
                SESSION_PROPERTIES_PROVIDERS,
                new SingletonIdentityCacheMapping(),
                // ttl is 0, cache is disabled
                new Duration(0, DAYS),
                true,
                10);

        SchemaTableName tableName = new SchemaTableName(schema, "test_load_failure_not_shared");
        createTable(tableName);
        ConnectorSession session = createSession("session");

        List<Future<JdbcTableHandle>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(executor.submit(() -> {
                barrier.await(10, SECONDS);
                return cachingJdbcClient.getTableHandle(session, tableName).orElseThrow();
            }));
        }

        List<String> results = new ArrayList<>();
        for (Future<JdbcTableHandle> future : futures) {
            try {
                results.add(future.get().toString());
            }
            catch (ExecutionException e) {
                results.add(e.getCause().toString());
            }
        }

        assertThat(results)
                .containsExactlyInAnyOrder(
                        "example.test_load_failure_not_shared " + database.getDatabaseName() + ".EXAMPLE.TEST_LOAD_FAILURE_NOT_SHARED",
                        "com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException: first attempt is poised to fail");
    }

    @Test
    public void testSpecificSchemaAndTableCaches()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(
                FOREVER,
                Duration.succinctDuration(3, SECONDS),
                Duration.succinctDuration(2, SECONDS),
                false, // decreased ttl for schema and table names mostly makes sense with cacheMissing == false
                10000);
        String secondSchema = schema + "_two";
        SchemaTableName firstName = new SchemaTableName(schema, "first_table");
        SchemaTableName secondName = new SchemaTableName(secondSchema, "second_table");

        ConnectorSession session = createSession("asession");
        JdbcTableHandle first = createTable(firstName);

        // load schema names, tables names, table handles
        assertSchemaNamesCache(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getSchemaNames(session))
                    .contains(schema)
                    .doesNotContain(secondSchema);
        });
        assertTableNamesCache(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableNames(session, Optional.empty()))
                    .contains(firstName)
                    .doesNotContain(secondName);
        });
        assertTableHandleByNameCache(cachingJdbcClient).misses(1).loads(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(session, firstName)).isNotEmpty();
        });
        assertTableHandleByNameCache(cachingJdbcClient).misses(1).loads(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(session, secondName)).isEmpty();
        });

        jdbcClient.createSchema(SESSION, secondSchema);
        JdbcTableHandle second = createTable(secondName);

        // cached schema names, table names, table handles
        assertSchemaNamesCache(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getSchemaNames(session))
                    .contains(schema)
                    .doesNotContain(secondSchema);
        });
        assertTableNamesCache(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableNames(session, Optional.empty()))
                    .contains(firstName)
                    .doesNotContain(secondName);
        });
        assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(session, firstName)).isNotEmpty();
        });
        assertTableHandleByNameCache(cachingJdbcClient).hits(1).misses(1).loads(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableHandle(session, secondName)).isNotEmpty();
        });

        // reloads table names, retains schema names and table handles
        assertEventually(Duration.succinctDuration(10, SECONDS), () -> {
            assertSchemaNamesCache(cachingJdbcClient).hits(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getSchemaNames(session))
                        .contains(schema)
                        .doesNotContain(secondSchema);
            });
            assertTableNamesCache(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableNames(session, Optional.empty()))
                        .contains(firstName, secondName);
            });
            assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableHandle(session, firstName)).isNotEmpty();
            });
            assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableHandle(session, secondName)).isNotEmpty();
            });
        });

        // reloads tables names and schema names, but retains table handles
        assertEventually(Duration.succinctDuration(10, SECONDS), () -> {
            assertSchemaNamesCache(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getSchemaNames(session))
                        .contains(schema, secondSchema);
            });
            assertTableNamesCache(cachingJdbcClient).loads(1).misses(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableNames(session, Optional.empty()))
                        .contains(firstName, secondName);
            });
            assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableHandle(session, firstName)).isNotEmpty();
            });
            assertTableHandleByNameCache(cachingJdbcClient).hits(1).afterRunning(() -> {
                assertThat(cachingJdbcClient.getTableHandle(session, secondName)).isNotEmpty();
            });
        });

        jdbcClient.dropTable(SESSION, first);
        jdbcClient.dropTable(SESSION, second);
        jdbcClient.dropSchema(SESSION, secondSchema);
    }

    private JdbcTableHandle getAnyTable(String schema)
    {
        SchemaTableName tableName = jdbcClient.getTableNames(SESSION, Optional.of(schema))
                .stream()
                .filter(schemaTableName -> !"public".equals(schemaTableName.getTableName()))
                .findAny()
                .orElseThrow();
        return jdbcClient.getTableHandle(SESSION, tableName).orElseThrow();
    }

    private JdbcColumnHandle addColumn(JdbcTableHandle tableHandle)
    {
        return addColumn(tableHandle, "phantom_column");
    }

    private JdbcColumnHandle addColumn(JdbcTableHandle tableHandle, String columnName)
    {
        return addColumn(jdbcClient, tableHandle, columnName);
    }

    private JdbcColumnHandle addColumn(JdbcClient client, JdbcTableHandle tableHandle, String columnName)
    {
        ColumnMetadata columnMetadata = new ColumnMetadata(columnName, INTEGER);
        client.addColumn(SESSION, tableHandle, columnMetadata);
        return client.getColumns(SESSION, tableHandle)
                .stream()
                .filter(jdbcColumnHandle -> jdbcColumnHandle.getColumnMetadata().equals(columnMetadata))
                .collect(onlyElement());
    }

    private static ConnectorSession createSession(String sessionName)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(PROPERTY_METADATA)
                .setPropertyValues(ImmutableMap.of("session_name", sessionName))
                .build();
    }

    private static ConnectorSession createUserSession(String userName)
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser(userName)
                        .withExtraCredentials(ImmutableMap.of("user", userName))
                        .build())
                .build();
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(JdbcClient.class, CachingJdbcClient.class);
    }

    private static SingleJdbcCacheStatsAssertions assertSchemaNamesCache(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.SCHEMA_NAMES_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertTableNamesCache(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.TABLE_NAMES_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertTableHandleByNameCache(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.TABLE_HANDLES_BY_NAME_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertTableHandleByQueryCache(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.TABLE_HANDLES_BY_QUERY_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertProcedureHandleByQueryCache(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.PROCEDURE_HANDLES_BY_QUERY_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertColumnCacheStats(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.COLUMNS_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertStatisticsCacheStats(CachingJdbcClient client)
    {
        return assertCacheStats(client, CachingJdbcCache.STATISTICS_CACHE);
    }

    private static SingleJdbcCacheStatsAssertions assertCacheStats(CachingJdbcClient client, CachingJdbcCache cache)
    {
        return new SingleJdbcCacheStatsAssertions(client, cache);
    }

    private static JdbcCacheStatsAssertions assertCacheStats(CachingJdbcClient client)
    {
        return new JdbcCacheStatsAssertions(client);
    }

    private static class SingleJdbcCacheStatsAssertions
    {
        private CachingJdbcCache chosenCache;
        private JdbcCacheStatsAssertions delegate;

        private SingleJdbcCacheStatsAssertions(CachingJdbcClient jdbcClient, CachingJdbcCache chosenCache)
        {
            this.chosenCache = requireNonNull(chosenCache, "chosenCache is null");
            delegate = new JdbcCacheStatsAssertions(jdbcClient);
        }

        public SingleJdbcCacheStatsAssertions loads(long value)
        {
            delegate.loads(chosenCache, value);
            return this;
        }

        public SingleJdbcCacheStatsAssertions hits(long value)
        {
            delegate.hits(chosenCache, value);
            return this;
        }

        public SingleJdbcCacheStatsAssertions misses(long value)
        {
            delegate.misses(chosenCache, value);
            return this;
        }

        public void afterRunning(Runnable runnable)
        {
            delegate.afterRunning(runnable);
        }

        public <T> T calling(Callable<T> callable)
                throws Exception
        {
            return delegate.calling(callable);
        }
    }

    private static class JdbcCacheStatsAssertions
    {
        private final CachingJdbcClient jdbcClient;

        private final Map<CachingJdbcCache, Long> loads = new HashMap<>();
        private final Map<CachingJdbcCache, Long> hits = new HashMap<>();
        private final Map<CachingJdbcCache, Long> misses = new HashMap<>();

        public JdbcCacheStatsAssertions(CachingJdbcClient jdbcClient)
        {
            this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        }

        public JdbcCacheStatsAssertions loads(CachingJdbcCache cache, long value)
        {
            loads.put(cache, value);
            return this;
        }

        public JdbcCacheStatsAssertions hits(CachingJdbcCache cache, long value)
        {
            hits.put(cache, value);
            return this;
        }

        public JdbcCacheStatsAssertions misses(CachingJdbcCache cache, long value)
        {
            misses.put(cache, value);
            return this;
        }

        public void afterRunning(Runnable runnable)
        {
            try {
                calling(() -> {
                    runnable.run();
                    return null;
                });
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public <T> T calling(Callable<T> callable)
                throws Exception
        {
            Map<CachingJdbcCache, CacheStats> beforeStats = Stream.of(CachingJdbcCache.values())
                    .collect(toImmutableMap(identity(), cache -> cache.statsGetter.apply(jdbcClient)));
            T value = callable.call();
            Map<CachingJdbcCache, CacheStats> afterStats = Stream.of(CachingJdbcCache.values())
                    .collect(toImmutableMap(identity(), cache -> cache.statsGetter.apply(jdbcClient)));

            for (CachingJdbcCache cache : CachingJdbcCache.values()) {
                long loadDelta = afterStats.get(cache).loadCount() - beforeStats.get(cache).loadCount();
                long missesDelta = afterStats.get(cache).missCount() - beforeStats.get(cache).missCount();
                long hitsDelta = afterStats.get(cache).hitCount() - beforeStats.get(cache).hitCount();

                assertThat(loadDelta).as(cache + " loads (delta)").isEqualTo(loads.getOrDefault(cache, 0L));
                assertThat(hitsDelta).as(cache + " hits (delta)").isEqualTo(hits.getOrDefault(cache, 0L));
                assertThat(missesDelta).as(cache + " misses (delta)").isEqualTo(misses.getOrDefault(cache, 0L));
            }

            return value;
        }
    }

    enum CachingJdbcCache
    {
        SCHEMA_NAMES_CACHE(CachingJdbcClient::getSchemaNamesCacheStats),
        TABLE_NAMES_CACHE(CachingJdbcClient::getTableNamesCacheStats),
        TABLE_HANDLES_BY_NAME_CACHE(CachingJdbcClient::getTableHandlesByNameCacheStats),
        TABLE_HANDLES_BY_QUERY_CACHE(CachingJdbcClient::getTableHandlesByQueryCacheStats),
        PROCEDURE_HANDLES_BY_QUERY_CACHE(CachingJdbcClient::getProcedureHandlesByQueryCacheStats),
        COLUMNS_CACHE(CachingJdbcClient::getColumnsCacheStats),
        STATISTICS_CACHE(CachingJdbcClient::getStatisticsCacheStats),
        /**/;

        private final Function<CachingJdbcClient, CacheStats> statsGetter;

        CachingJdbcCache(Function<CachingJdbcClient, CacheStats> statsGetter)
        {
            this.statsGetter = requireNonNull(statsGetter, "statsGetter is null");
        }
    }
}
