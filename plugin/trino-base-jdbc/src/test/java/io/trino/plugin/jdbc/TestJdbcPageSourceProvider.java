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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.RetryPolicy;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJdbcPageSourceProvider
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
            .build();

    private TestingDatabase database;
    private JdbcClient jdbcClient;
    private JdbcSplit split;

    private JdbcTableHandle table;
    private JdbcColumnHandle textColumn;
    private JdbcColumnHandle textShortColumn;
    private JdbcColumnHandle valueColumn;

    private ExecutorService executor;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        database = new TestingDatabase();
        jdbcClient = database.getJdbcClient();
        table = database.getTableHandle(SESSION, new SchemaTableName("example", "numbers"));
        split = database.getSplit(SESSION, table);

        Map<String, JdbcColumnHandle> columns = database.getColumnHandles(SESSION, table);
        textColumn = columns.get("text");
        textShortColumn = columns.get("text_short");
        valueColumn = columns.get("value");

        executor = newDirectExecutorService();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(
                database,
                () -> executor.shutdownNow());
        database = null;
        executor = null;
    }

    @Test
    public void testGetPageSource()
    {
        ConnectorTransactionHandle transaction = new JdbcTransactionHandle();
        JdbcPageSourceProvider pageSourceProvider = new JdbcPageSourceProvider(jdbcClient, executor, RetryPolicy.ofDefaults());
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(transaction, SESSION, split, table, ImmutableList.of(textColumn, textShortColumn, valueColumn), DynamicFilter.EMPTY);
        assertThat(pageSource).withFailMessage("pageSource is null").isNotNull();

        Map<String, Long> data = new LinkedHashMap<>();
        for (SourcePage page = pageSource.getNextSourcePage(); ; page = pageSource.getNextSourcePage()) {
            if (page == null) {
                continue;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertThat(VARCHAR.getSlice(page.getBlock(0), position))
                        .isEqualTo(VARCHAR.getSlice(page.getBlock(1), position));
                data.put(
                        VARCHAR.getSlice(page.getBlock(0), position).toStringUtf8(),
                        BIGINT.getLong(page.getBlock(2), position));
            }

            if (pageSource.isFinished()) {
                break;
            }
        }
        assertThat(data).isEqualTo(ImmutableMap.<String, Long>builder()
                .put("one", 1L)
                .put("two", 2L)
                .put("three", 3L)
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .buildOrThrow());
    }

    @Test
    public void testTupleDomain()
    {
        // single value
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.singleValue(VARCHAR, utf8Slice("foo")))));

        // multiple values (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.singleValue(VARCHAR, utf8Slice("bar")))))));

        // inequality (string)
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("foo"))), false))));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("foo"))), false))));

        // is null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.onlyNull(VARCHAR))));

        // not null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.notNull(VARCHAR))));

        // specific value or null
        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.onlyNull(VARCHAR))))));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true)), false))));

        getCursor(table, ImmutableList.of(textColumn, textShortColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        textColumn,
                        Domain.create(ValueSet.ofRanges(
                                Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true),
                                Range.range(VARCHAR, utf8Slice("hello"), false, utf8Slice("world"), false)),
                                false),

                        textShortColumn,
                        Domain.create(ValueSet.ofRanges(
                                Range.range(createVarcharType(32), utf8Slice("bar"), true, utf8Slice("foo"), true),
                                Range.range(createVarcharType(32), utf8Slice("hello"), false, utf8Slice("world"), false)),
                                false))));

        getCursor(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        textColumn,
                        Domain.create(ValueSet.ofRanges(
                                Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true),
                                Range.range(VARCHAR, utf8Slice("hello"), false, utf8Slice("world"), false),
                                Range.equal(VARCHAR, utf8Slice("apple")),
                                Range.equal(VARCHAR, utf8Slice("banana")),
                                Range.equal(VARCHAR, utf8Slice("zoo"))),
                                false),

                        valueColumn,
                        Domain.create(ValueSet.ofRanges(
                                Range.range(BIGINT, 1L, true, 5L, true),
                                Range.range(BIGINT, 10L, false, 20L, false)),
                                true))));
    }

    private ConnectorPageSource getCursor(JdbcTableHandle jdbcTableHandle, List<ColumnHandle> columns, TupleDomain<ColumnHandle> domain)
    {
        jdbcTableHandle = new JdbcTableHandle(
                jdbcTableHandle.getRelationHandle(),
                domain,
                ImmutableList.of(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                jdbcTableHandle.getOtherReferencedTables(),
                jdbcTableHandle.getNextSyntheticColumnId(),
                Optional.empty(),
                ImmutableList.of());

        ConnectorSplitSource splits = jdbcClient.getSplits(SESSION, jdbcTableHandle);
        JdbcSplit split = (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(1000)).getSplits());

        ConnectorTransactionHandle transaction = new JdbcTransactionHandle();
        JdbcPageSourceProvider pageSourceProvider = new JdbcPageSourceProvider(jdbcClient, executor, RetryPolicy.ofDefaults());
        return pageSourceProvider.createPageSource(transaction, SESSION, split, jdbcTableHandle, columns, DynamicFilter.EMPTY);
    }
}
