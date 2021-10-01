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
package io.trino.server.protocol;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import io.trino.metadata.Metadata;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.testing.TestingSession;
import io.trino.tests.BogusType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.spi.type.TypeSignature.mapType;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.collections.Lists.newArrayList;

public class TestQueryResultRows
{
    private static final Function<String, Column> BOOLEAN_COLUMN = name -> new Column(name, BOOLEAN, new ClientTypeSignature(BOOLEAN));
    private static final Function<String, Column> BIGINT_COLUMN = name -> new Column(name, BIGINT, new ClientTypeSignature(BIGINT));
    private static final Function<String, Column> INT_COLUMN = name -> new Column(name, INTEGER, new ClientTypeSignature(INTEGER));

    private static final Metadata METADATA = createTestMetadataManager();

    @Test
    public void shouldNotReturnValues()
    {
        QueryResultRows rows = QueryResultRows.empty(getSession());

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isEmpty();
        assertThat(getAllValues(rows)).hasSize(0);
        assertThat(rows.getColumns()).isEmpty();
        assertThat(rows.iterator().hasNext()).isFalse();
    }

    @Test
    public void shouldReturnSingleValue()
    {
        Column column = BOOLEAN_COLUMN.apply("_col0");

        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withSingleBooleanValue(column, true)
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(getAllValues(rows)).hasSize(1).containsOnly(ImmutableList.of(true));
        assertThat(rows.getColumns().orElseThrow()).containsOnly(column);
    }

    @Test
    public void shouldReturnUpdateCount()
    {
        Column column = BIGINT_COLUMN.apply("_col0");
        long value = 10123;

        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(ImmutableList.of(column), ImmutableList.of(BigintType.BIGINT))
                .addPages(rowPagesBuilder(BigintType.BIGINT).row(value).build())
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(rows.getUpdateCount()).isPresent();
        assertThat(rows.getUpdateCount().get()).isEqualTo(value);

        assertThat(getAllValues(rows)).containsExactly(ImmutableList.of(value));
        assertThat(rows.getColumns().orElseThrow()).containsOnly(column);
    }

    @Test
    public void shouldNotHaveUpdateCount()
    {
        Column column = BOOLEAN_COLUMN.apply("_col0");

        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withSingleBooleanValue(column, false)
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(rows.getUpdateCount()).isEmpty();
        assertThat(rows.iterator()).hasNext();
    }

    @Test
    public void shouldReadAllValuesFromMultiplePages()
    {
        List<Column> columns = ImmutableList.of(INT_COLUMN.apply("_col0"), BIGINT_COLUMN.apply("_col1"));
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BigintType.BIGINT);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, 10L)
                .row(1, 11L)
                .row(2, 12L)
                .row(3, 13L)
                .row(4, 14L)
                .pageBreak()
                .row(100, 110L)
                .row(101, 111L)
                .row(102, 112L)
                .row(103, 113L)
                .row(104, 114L)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .addPages(pages)
                .withExceptionConsumer(exceptionConsumer)
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(rows.getTotalRowsCount()).isEqualTo(10);
        assertThat(rows.getColumns()).isEqualTo(Optional.of(columns));
        assertThat(rows.getUpdateCount()).isEmpty();

        assertThat(getAllValues(rows)).containsExactly(
                ImmutableList.of(0, 10L),
                ImmutableList.of(1, 11L),
                ImmutableList.of(2, 12L),
                ImmutableList.of(3, 13L),
                ImmutableList.of(4, 14L),
                ImmutableList.of(100, 110L),
                ImmutableList.of(101, 111L),
                ImmutableList.of(102, 112L),
                ImmutableList.of(103, 113L),
                ImmutableList.of(104, 114L));

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
    }

    @Test
    public void shouldOmitBadRows()
    {
        List<Column> columns = ImmutableList.of(BOOLEAN_COLUMN.apply("_col0"), BOOLEAN_COLUMN.apply("_col1"));
        List<Type> types = ImmutableList.of(BogusType.BOGUS, BogusType.BOGUS);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, 1)
                .row(0, 0)
                .row(0, 1)
                .row(1, 0)
                .row(0, 1)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(5);
        assertThat(rows.getColumns()).isEqualTo(Optional.of(columns));
        assertTrue(rows.getUpdateCount().isEmpty());

        assertThat(getAllValues(rows))
                .containsExactly(ImmutableList.of(0, 0));

        List<Throwable> exceptions = exceptionConsumer.getExceptions();

        assertThat(exceptions)
                .isNotEmpty();

        assertThat(exceptions)
                .hasSize(4);

        assertThat(exceptions.get(0))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not serialize column '_col1' of type 'Bogus' at position 1:2")
                .hasRootCauseMessage("This is bogus exception");

        assertThat(exceptions.get(1))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not serialize column '_col1' of type 'Bogus' at position 3:2")
                .hasRootCauseMessage("This is bogus exception");

        assertThat(exceptions.get(2))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not serialize column '_col0' of type 'Bogus' at position 4:1")
                .hasRootCauseMessage("This is bogus exception");

        assertThat(exceptions.get(3))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not serialize column '_col1' of type 'Bogus' at position 5:2")
                .hasRootCauseMessage("This is bogus exception");
    }

    @Test
    public void shouldHandleNullValues()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", INTEGER, new ClientTypeSignature(INTEGER)), new Column("_col1", BOOLEAN, new ClientTypeSignature(BOOLEAN)));
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BooleanType.BOOLEAN);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, null)
                .pageBreak()
                .row(1, null)
                .pageBreak()
                .row(2, true)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(3);

        assertThat(getAllValues(rows))
                .hasSize(3)
                .containsExactly(newArrayList(0, null), newArrayList(1, null), newArrayList(2, true));
    }

    @Test
    public void shouldHandleNullTimestamps()
    {
        List<Column> columns = ImmutableList.of(
                new Column("_col0", TIMESTAMP, new ClientTypeSignature(TIMESTAMP)),
                new Column("_col1", TIMESTAMP_WITH_TIME_ZONE, new ClientTypeSignature(TIMESTAMP_WITH_TIME_ZONE)));
        List<Type> types = ImmutableList.of(TimestampType.TIMESTAMP_MILLIS, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);

        List<Page> pages = rowPagesBuilder(types)
                .row(null, null)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(1);

        assertThat(getAllValues(rows))
                .hasSize(1)
                .containsExactly(newArrayList(null, null));
    }

    @Test
    public void shouldHandleNullValuesInArray()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", ARRAY, new ClientTypeSignature(ARRAY)));
        List<Type> types = ImmutableList.of(new ArrayType(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE));

        List<Page> pages = rowPagesBuilder(types)
                .row(singletonList(null))
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(1);

        assertThat(getAllValues(rows))
                .hasSize(1)
                .containsOnly(singletonList(singletonList(null)));

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
    }

    @Test
    public void shouldHandleNullValuesInMap()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", MAP, new ClientTypeSignature(MAP)));
        List<Type> types = ImmutableList.of(createMapType(BigintType.BIGINT, BigintType.BIGINT));

        List<Page> pages = rowPagesBuilder(types)
                .row(singletonMap(10, null))
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(1);

        assertThat(getAllValues(rows))
                .hasSize(1)
                .containsOnly(singletonList(singletonMap(10L, null)));

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
    }

    @Test
    public void shouldHandleNullValuesInRow()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", ROW, new ClientTypeSignature(ROW)));
        List<Type> types = ImmutableList.of(RowType.from(ImmutableList.of(RowType.field("first", SmallintType.SMALLINT), RowType.field("second", SmallintType.SMALLINT))));

        List<Object> values = new ArrayList<>();
        values.add(null);
        values.add((short) 1);

        List<Page> pages = rowPagesBuilder(types)
                .row(values)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(1);

        List<List<Object>> allValues = getAllValues(rows);

        assertThat(allValues)
                .hasSize(1)
                .containsOnly(singletonList(newArrayList(null, (short) 1)));

        assertThat(exceptionConsumer.getExceptions()).isEmpty();
    }

    @Test
    public void shouldNotThrowWhenDataAndColumnsAreMissing()
    {
        QueryResultRows.empty(getSession());
    }

    @Test
    public void shouldThrowWhenColumnsAndTypesSizeMismatch()
    {
        List<Column> columns = ImmutableList.of(INT_COLUMN.apply("_col0"));
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BooleanType.BOOLEAN);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, null)
                .build();

        assertThatThrownBy(() -> queryResultRowsBuilder(getSession()).addPages(pages).withColumnsAndTypes(columns, types).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columns and types size mismatch");
    }

    @Test
    public void shouldThrowWhenColumnsAreNull()
    {
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BooleanType.BOOLEAN);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, null)
                .build();

        assertThatThrownBy(() -> queryResultRowsBuilder(getSession()).addPages(pages).withColumnsAndTypes(null, types).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columns and types must be present at the same time");
    }

    @Test
    public void shouldAcceptNullColumnsAndTypes()
    {
        queryResultRowsBuilder(getSession())
                .withColumnsAndTypes(null, null)
                .build();
    }

    @Test
    public void shouldThrowWhenTypesAreNull()
    {
        List<Column> columns = ImmutableList.of(INT_COLUMN.apply("_col0"));
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BooleanType.BOOLEAN);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, null)
                .build();

        assertThatThrownBy(() -> queryResultRowsBuilder(getSession()).addPages(pages).withColumnsAndTypes(columns, null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("columns and types must be present at the same time");
    }

    @Test
    public void shouldThrowWhenDataIsPresentWithoutColumns()
    {
        List<Page> pages = rowPagesBuilder(ImmutableList.of(IntegerType.INTEGER, BooleanType.BOOLEAN))
                .row(0, null)
                .build();

        assertThatThrownBy(() -> queryResultRowsBuilder(getSession()).addPages(pages).build())
                .isInstanceOf(VerifyException.class)
                .hasMessage("data present without columns and types");
    }

    private static List<List<Object>> getAllValues(QueryResultRows rows)
    {
        ImmutableList.Builder<List<Object>> builder = ImmutableList.builder();

        for (List<Object> values : rows) {
            builder.add(values);
        }

        return builder.build();
    }

    private static Session getSession()
    {
        return TestingSession.testSessionBuilder()
                .build();
    }

    private static final class TestExceptionConsumer
            implements Consumer<Throwable>
    {
        private final List<Throwable> exceptions = new ArrayList<>();

        @Override
        public void accept(Throwable throwable)
        {
            exceptions.add(throwable);
        }

        public List<Throwable> getExceptions()
        {
            return exceptions;
        }
    }

    private static Type createMapType(Type keyType, Type valueType)
    {
        return METADATA.getType(mapType(keyType.getTypeSignature(), valueType.getTypeSignature()));
    }
}
