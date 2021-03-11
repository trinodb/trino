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
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.connector.DynamicFilter.EMPTY;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertNotNull;

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

    @BeforeClass
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
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        database.close();
    }

    @Test
    public void testReadPage()
    {
        ConnectorTransactionHandle transaction = new JdbcTransactionHandle();
        JdbcPageSourceProvider jdbcPageSourceProvider = new JdbcPageSourceProvider(jdbcClient);
        ConnectorPageSource pageSource = jdbcPageSourceProvider.createPageSource(transaction, SESSION, split, table, ImmutableList.of(textColumn, textShortColumn, valueColumn), EMPTY);
        assertNotNull(pageSource, "pageSource is null");

        Page page = pageSource.getNextPage();
        assertNotNull(page, "page is null");

        List<Type> types = ImmutableList.of(VARCHAR, VARCHAR, BIGINT);
        PageBuilder expected = new PageBuilder(types);
        append(expected, types, "one", "one", 1);
        append(expected, types, "two", "two", 2);
        append(expected, types, "three", "three", 3);
        append(expected, types, "ten", "ten", 10);
        append(expected, types, "eleven", "eleven", 11);
        append(expected, types, "twelve", "twelve", 12);

        assertPageEquals(types, page, expected.build());
    }

    @Test
    public void testTupleDomain()
    {
        // single value
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.singleValue(VARCHAR, utf8Slice("foo")))));

        // multiple values (string)
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.singleValue(VARCHAR, utf8Slice("bar")))))));

        // inequality (string)
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))));

        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("foo"))), false))));

        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(VARCHAR, utf8Slice("foo"))), false))));

        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.lessThan(VARCHAR, utf8Slice("foo"))), false))));

        // is null
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.onlyNull(VARCHAR))));

        // not null
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.notNull(VARCHAR))));

        // specific value or null
        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.union(ImmutableList.of(Domain.singleValue(VARCHAR, utf8Slice("foo")), Domain.onlyNull(VARCHAR))))));

        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
                ImmutableMap.of(textColumn, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("bar"), true, utf8Slice("foo"), true)), false))));

        getPage(table, ImmutableList.of(textColumn, textShortColumn, valueColumn), TupleDomain.withColumnDomains(
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

        getPage(table, ImmutableList.of(textColumn, valueColumn), TupleDomain.withColumnDomains(
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

    private Page getPage(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> domain)
    {
        jdbcTableHandle = new JdbcTableHandle(
                jdbcTableHandle.getRelationHandle(),
                domain,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                jdbcTableHandle.getNextSyntheticColumnId());

        ConnectorSplitSource splits = jdbcClient.getSplits(SESSION, jdbcTableHandle);
        JdbcSplit split = (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());

        ConnectorTransactionHandle transaction = new JdbcTransactionHandle();
        JdbcPageSourceProvider pageSourceProvider = new JdbcPageSourceProvider(jdbcClient);
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                transaction,
                SESSION,
                split,
                jdbcTableHandle,
                columns.stream()
                        .map(ColumnHandle.class::cast)
                        .collect(toImmutableList()),
                EMPTY);

        return pageSource.getNextPage();
    }

    public static void append(PageBuilder builder, List<Type> types, Object... values)
    {
        checkArgument(types.size() == values.length);
        builder.declarePosition();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            if (type instanceof VarcharType) {
                checkArgument(values[i] instanceof String);
                type.writeSlice(builder.getBlockBuilder(i), utf8Slice((String) values[i]));
                continue;
            }
            if (type == BIGINT) {
                checkArgument(values[i] instanceof Integer);
                type.writeLong(builder.getBlockBuilder(i), (Integer) values[i]);
                continue;
            }
            throw new IllegalArgumentException();
        }
    }
}
