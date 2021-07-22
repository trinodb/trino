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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.credential.ExtraCredentialConfig;
import io.trino.spi.connector.ColumnHandle;
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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.builder;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        cachingJdbcClient = createCachingJdbcClient(true, 10000);
        jdbcClient = database.getJdbcClient();
        schema = jdbcClient.getSchemaNames(SESSION).iterator().next();
    }

    private CachingJdbcClient createCachingJdbcClient(Duration cacheTtl, boolean cacheMissing, long cacheMaximumSize)
    {
        return new CachingJdbcClient(database.getJdbcClient(), SESSION_PROPERTIES_PROVIDERS, new SingletonIdentityCacheMapping(), cacheTtl, cacheMissing, cacheMaximumSize);
    }

    private CachingJdbcClient createCachingJdbcClient(boolean cacheMissing, long cacheMaximumSize)
    {
        return createCachingJdbcClient(FOREVER, cacheMissing, cacheMaximumSize);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testSchemaNamesCached()
    {
        String phantomSchema = "phantom_schema";

        jdbcClient.createSchema(SESSION, phantomSchema);
        assertThat(cachingJdbcClient.getSchemaNames(SESSION)).contains(phantomSchema);
        jdbcClient.dropSchema(SESSION, phantomSchema);

        assertThat(jdbcClient.getSchemaNames(SESSION)).doesNotContain(phantomSchema);
        assertThat(cachingJdbcClient.getSchemaNames(SESSION)).contains(phantomSchema);
    }

    @Test
    public void testTableNamesCached()
    {
        SchemaTableName phantomTable = new SchemaTableName(schema, "phantom_table");

        createTable(phantomTable);
        assertThat(cachingJdbcClient.getTableNames(SESSION, Optional.of(schema))).contains(phantomTable);
        dropTable(phantomTable);

        assertThat(jdbcClient.getTableNames(SESSION, Optional.of(schema))).doesNotContain(phantomTable);
        assertThat(cachingJdbcClient.getTableNames(SESSION, Optional.of(schema))).contains(phantomTable);
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
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // load second
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, second, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate first
        cachingJdbcClient.dropTable(SESSION, first);
        JdbcTableHandle secondFirst = createTable(new SchemaTableName(schema, "first"));

        // load first again
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read first from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
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
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                Set.of(new SchemaTableName(schema, "first")),
                0);

        // load
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate 'second'
        cachingJdbcClient.dropTable(SESSION, second);

        // read from cache again (no invalidation)
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // invalidate 'first'
        cachingJdbcClient.dropTable(SESSION, first);

        // load again
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, queryOnFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });
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
            public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
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

        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table, TupleDomain.all())).isEqualTo(TableStatistics.empty());
        });

        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table, TupleDomain.all())).isEqualTo(TableStatistics.empty());
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

        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table, TupleDomain.all())).isEqualTo(TableStatistics.empty());
        });

        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, table, TupleDomain.all())).isEqualTo(TableStatistics.empty());
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
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, first, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // flush cache
        cachingJdbcClient.flushCache();
        JdbcTableHandle secondFirst = createTable(new SchemaTableName(schema, "first"));

        // load table again
        assertStatisticsCacheStats(cachingJdbcClient).misses(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // read table from cache
        assertStatisticsCacheStats(cachingJdbcClient).hits(1).afterRunning(() -> {
            assertThat(cachingJdbcClient.getTableStatistics(session, secondFirst, TupleDomain.all())).isEqualTo(NON_EMPTY_STATS);
        });

        // cleanup
        jdbcClient.dropTable(SESSION, first);
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
        ColumnMetadata columnMetadata = new ColumnMetadata(columnName, INTEGER);
        jdbcClient.addColumn(SESSION, tableHandle, columnMetadata);
        return jdbcClient.getColumns(SESSION, tableHandle)
                .stream()
                .filter(jdbcColumnHandle -> jdbcColumnHandle.getColumnMetadata().equals(columnMetadata))
                .findAny()
                .orElseThrow();
    }

    private static ConnectorSession createSession(String sessionName)
    {
        return builder()
                .setPropertyMetadata(PROPERTY_METADATA)
                .setPropertyValues(ImmutableMap.of("session_name", sessionName))
                .build();
    }

    private static ConnectorSession createUserSession(String userName)
    {
        return builder()
                .setIdentity(ConnectorIdentity.forUser(userName)
                        .withExtraCredentials(ImmutableMap.of("user", userName))
                        .build())
                .build();
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(JdbcClient.class, CachingJdbcClient.class, nonOverriddenMethods());
    }

    private static Set<Method> nonOverriddenMethods()
    {
        try {
            return ImmutableSet.<Method>builder()
                    .add(JdbcClient.class.getMethod("schemaExists", ConnectorSession.class, String.class))
                    .build();
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    private static CacheStatsAssertions assertTableNamesCache(CachingJdbcClient cachingJdbcClient)
    {
        return new CacheStatsAssertions(cachingJdbcClient::getTableNamesCacheStats);
    }

    private static CacheStatsAssertions assertColumnCacheStats(CachingJdbcClient client)
    {
        return new CacheStatsAssertions(client::getColumnsCacheStats);
    }

    private static CacheStatsAssertions assertStatisticsCacheStats(CachingJdbcClient client)
    {
        return new CacheStatsAssertions(client::getStatisticsCacheStats);
    }

    private static final class CacheStatsAssertions
    {
        private final Supplier<CacheStats> stats;

        private long loads;
        private long hits;
        private long misses;

        private CacheStatsAssertions(Supplier<CacheStats> stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        public CacheStatsAssertions loads(long value)
        {
            this.loads = value;
            return this;
        }

        public CacheStatsAssertions hits(long value)
        {
            this.hits = value;
            return this;
        }

        public CacheStatsAssertions misses(long value)
        {
            this.misses = value;
            return this;
        }

        public void afterRunning(Runnable runnable)
        {
            CacheStats beforeStats = stats.get();
            runnable.run();
            CacheStats afterStats = stats.get();

            long expectedLoad = beforeStats.loadCount() + loads;
            long expectedMisses = beforeStats.missCount() + misses;
            long expectedHits = beforeStats.hitCount() + hits;

            assertThat(afterStats.loadCount())
                    .withFailMessage("Expected load count is %d but actual is %d", expectedLoad, afterStats.loadCount())
                    .isEqualTo(expectedLoad);
            assertThat(afterStats.hitCount())
                    .withFailMessage("Expected hit count is %d but actual is %d", expectedHits, afterStats.hitCount())
                    .isEqualTo(expectedHits);
            assertThat(afterStats.missCount())
                    .withFailMessage("Expected miss count is %d but actual is %d", expectedMisses, afterStats.missCount())
                    .isEqualTo(expectedMisses);
        }
    }
}
