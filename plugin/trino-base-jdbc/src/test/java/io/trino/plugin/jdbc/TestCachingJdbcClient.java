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
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.builder;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test(singleThreaded = true)
@SuppressWarnings({"MultipleVariableDeclarations", "InnerAssignment"})
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

    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(PROPERTY_METADATA)
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
        cachingJdbcClient = createCachingJdbcClient(true);
        jdbcClient = database.getJdbcClient();
        schema = jdbcClient.getSchemaNames(SESSION).iterator().next();
    }

    private CachingJdbcClient createCachingJdbcClient(Duration cacheTtl, boolean cacheMissing)
    {
        return new CachingJdbcClient(database.getJdbcClient(), Set.of(getTestSessionPropertiesProvider()), cacheTtl, cacheMissing);
    }

    private CachingJdbcClient createCachingJdbcClient(boolean cacheMissing)
    {
        return createCachingJdbcClient(FOREVER, cacheMissing);
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
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(false);
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

        int expectedLoad = 0, expectedHit = 0, expectedMiss = 0;

        // Read column into cache
        assertThat(cachingJdbcClient.getColumns(SESSION, table)).contains(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 1, expectedHit, expectedMiss += 1);

        jdbcClient.dropColumn(SESSION, table, phantomColumn);

        // Load column from cache
        assertThat(jdbcClient.getColumns(SESSION, table)).doesNotContain(phantomColumn);
        assertThat(cachingJdbcClient.getColumns(SESSION, table)).contains(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad, expectedHit += 1, expectedMiss);
    }

    @Test
    public void testColumnsCachedPerSession()
    {
        ConnectorSession firstSession = createSession("first");
        ConnectorSession secondSession = createSession("second");
        JdbcTableHandle table = getAnyTable(schema);
        JdbcColumnHandle phantomColumn = addColumn(table);

        int expectedLoad = 0, expectedHit = 0, expectedMiss = 0;

        // Load columns in first session scope
        assertThat(cachingJdbcClient.getColumns(firstSession, table)).contains(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 1, expectedHit, expectedMiss += 1);

        // Load columns in second session scope
        assertThat(cachingJdbcClient.getColumns(secondSession, table)).contains(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 1, expectedHit, expectedMiss += 1);

        // Check that columns are cached
        assertThat(cachingJdbcClient.getColumns(secondSession, table)).contains(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad, expectedHit += 1, expectedMiss);

        // Drop first column and invalidate cache for all sessions
        cachingJdbcClient.dropColumn(firstSession, table, phantomColumn);
        assertThat(jdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);

        // Load columns into cache in both sessions scope
        assertThat(cachingJdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, table)).doesNotContain(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 2, expectedHit, expectedMiss += 2);

        // Read columns from cache
        assertThat(cachingJdbcClient.getColumns(firstSession, table)).doesNotContain(phantomColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, table)).doesNotContain(phantomColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad, expectedHit + 2, expectedMiss);
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

        int expectedLoad = 0, expectedHit = 0, expectedMiss = 0;

        // Load columns for both tables into cache and assert cache loads (sessions x tables)
        assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).contains(firstColumn);
        assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).contains(secondColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).contains(firstColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 4, expectedHit, expectedMiss += 4);

        // Load columns from cache
        assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).contains(firstColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad, expectedHit += 2, expectedMiss);

        // Rename column
        cachingJdbcClient.renameColumn(firstSession, firstTable, firstColumn, "another_column");
        assertThat(cachingJdbcClient.getColumns(secondSession, firstTable))
                .doesNotContain(firstColumn)
                .containsAll(jdbcClient.getColumns(SESSION, firstTable));
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 1, expectedHit, expectedMiss += 1);

        // Drop columns and caches for first table
        cachingJdbcClient.dropTable(secondSession, firstTable);
        assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, firstTable)).isInstanceOf(TableNotFoundException.class);
        assertThatThrownBy(() -> cachingJdbcClient.getColumns(secondSession, firstTable)).isInstanceOf(TableNotFoundException.class);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 2, expectedHit, expectedMiss += 2);

        // Check if second table is still cached
        assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).contains(secondColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).contains(secondColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad, expectedHit += 2, expectedMiss);

        cachingJdbcClient.dropTable(secondSession, secondTable);
    }

    @Test
    public void testColumnsNotCachedWhenCacheDisabled()
    {
        CachingJdbcClient cachingJdbcClient = createCachingJdbcClient(ZERO, true);
        ConnectorSession firstSession = createSession("first");
        ConnectorSession secondSession = createSession("second");

        JdbcTableHandle firstTable = createTable(new SchemaTableName(schema, "first_table"));
        JdbcTableHandle secondTable = createTable(new SchemaTableName(schema, "second_table"));
        JdbcColumnHandle firstColumn = addColumn(firstTable, "first_column");
        JdbcColumnHandle secondColumn = addColumn(secondTable, "second_column");

        int expectedLoad = 0, expectedHit = 0, expectedMiss = 0;

        assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).containsExactly(firstColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).containsExactly(firstColumn);
        assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).containsExactly(secondColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).containsExactly(secondColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 4, expectedHit, expectedMiss += 4);

        assertThat(cachingJdbcClient.getColumns(firstSession, firstTable)).containsExactly(firstColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, firstTable)).containsExactly(firstColumn);
        assertThat(cachingJdbcClient.getColumns(firstSession, secondTable)).containsExactly(secondColumn);
        assertThat(cachingJdbcClient.getColumns(secondSession, secondTable)).containsExactly(secondColumn);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 4, expectedHit, expectedMiss += 4);

        // Drop tables by not using caching jdbc client
        jdbcClient.dropTable(SESSION, firstTable);
        jdbcClient.dropTable(SESSION, secondTable);

        // Columns are loaded bypassing a cache
        assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, firstTable)).isInstanceOf(TableNotFoundException.class);
        assertThatThrownBy(() -> cachingJdbcClient.getColumns(firstSession, secondTable)).isInstanceOf(TableNotFoundException.class);
        assertCacheLoadsHitsAndMisses(cachingJdbcClient.getColumnsCacheStats(), expectedLoad += 2, expectedHit, expectedMiss += 2);
    }

    private void assertCacheLoadsHitsAndMisses(CacheStats stats, int expectedLoad, int expectedHit, int expectedMiss)
    {
        assertThat(stats.loadCount()).withFailMessage("Expected load count is %d but actual is %d", expectedLoad, stats.loadCount())
                .isEqualTo(expectedLoad);
        assertThat(stats.hitCount()).withFailMessage("Expected hit count is %d but actual is %d", expectedHit, stats.hitCount())
                .isEqualTo(expectedHit);
        assertThat(stats.missCount()).withFailMessage("Expected miss count is %d but actual is %d", expectedMiss, stats.missCount())
                .isEqualTo(expectedMiss);
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

    private static SessionPropertiesProvider getTestSessionPropertiesProvider()
    {
        return new SessionPropertiesProvider()
        {
            @Override
            public List<PropertyMetadata<?>> getSessionProperties()
            {
                return PROPERTY_METADATA;
            }
        };
    }

    private static ConnectorSession createSession(String sessionName)
    {
        return builder()
                .setPropertyMetadata(PROPERTY_METADATA)
                .setPropertyValues(ImmutableMap.of("session_name", sessionName))
                .build();
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(JdbcClient.class, CachingJdbcClient.class, nonOverridenMethods());
    }

    private static Set<Method> nonOverridenMethods()
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
}
