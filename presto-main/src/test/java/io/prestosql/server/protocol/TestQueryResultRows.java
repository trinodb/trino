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
package io.prestosql.server.protocol;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.client.ClientTypeSignature;
import io.prestosql.client.Column;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingSession;
import io.prestosql.tests.BogusType;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.client.ClientStandardTypes.BIGINT;
import static io.prestosql.client.ClientStandardTypes.BOOLEAN;
import static io.prestosql.client.ClientStandardTypes.INTEGER;
import static io.prestosql.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.collections.Lists.newArrayList;

public class TestQueryResultRows
{
    @Test
    public void shouldNotReturnValues()
    {
        QueryResultRows rows = QueryResultRows.empty(getSession());

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isEmpty();
        assertThat(getAllValues(rows)).hasSize(0);
        assertThat(rows.getColumns()).hasSize(0);
        assertThat(rows.iterator().hasNext()).isFalse();
    }

    @Test
    public void shouldReturnSingleValue()
    {
        Column column = new Column("_col0", BOOLEAN, new ClientTypeSignature(BOOLEAN));

        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withSingleBooleanValue(column, true)
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(getAllValues(rows)).hasSize(1).containsOnly(ImmutableList.of(true));
        assertThat(rows.getColumns()).containsOnly(column);
        assertThat(rows.iterator().hasNext()).isFalse();
    }

    @Test
    public void shouldReturnUpdateCount()
    {
        Column column = new Column("_col0", BIGINT, new ClientTypeSignature(BIGINT));
        long value = 10123;

        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumns(ImmutableList.of(column), ImmutableList.of(BigintType.BIGINT))
                .addPages(rowPagesBuilder(BigintType.BIGINT).row(value).build())
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(rows.getUpdateCount()).isPresent();
        assertThat(rows.getUpdateCount().get()).isEqualTo(value);

        assertThat(getAllValues(rows)).containsExactly(ImmutableList.of(value));
        assertThat(rows.getColumns()).containsOnly(column);
        assertThat(rows.iterator()).isExhausted();
    }

    @Test
    public void shouldNotHaveUpdateCount()
    {
        Column column = new Column("_col0", BOOLEAN, new ClientTypeSignature(BOOLEAN));

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
        List<Column> columns = ImmutableList.of(new Column("_col0", INTEGER, new ClientTypeSignature(INTEGER)), new Column("_col1", BIGINT, new ClientTypeSignature(BIGINT)));
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
                .withColumns(columns, types)
                .addPages(pages)
                .withExceptionConsumer(exceptionConsumer)
                .build();

        assertThat((Iterable<? extends List<Object>>) rows).as("rows").isNotEmpty();
        assertThat(rows.getTotalRowsCount()).isEqualTo(10);
        assertThat(rows.getColumns()).isEqualTo(columns);
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
        List<Column> columns = ImmutableList.of(new Column("_col0", INTEGER, new ClientTypeSignature(INTEGER)), new Column("_col1", BOOLEAN, new ClientTypeSignature(BOOLEAN)));
        List<Type> types = ImmutableList.of(IntegerType.INTEGER, BogusType.BOGUS);

        List<Page> pages = rowPagesBuilder(types)
                .row(0, 1)
                .row(1, 0)
                .row(2, 1)
                .row(3, 0)
                .row(4, 1)
                .build();

        TestExceptionConsumer exceptionConsumer = new TestExceptionConsumer();
        QueryResultRows rows = queryResultRowsBuilder(getSession())
                .withColumns(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(5);
        assertThat(rows.getColumns()).isEqualTo(columns);
        assertTrue(rows.getUpdateCount().isEmpty());

        assertThat(getAllValues(rows))
                .containsExactly(ImmutableList.of(1, 0), ImmutableList.of(3, 0));

        List<Throwable> exceptions = exceptionConsumer.getExceptions();

        assertThat(exceptions)
                .isNotEmpty();

        assertThat(exceptions)
                .hasSize(3);

        assertThat(exceptions.get(0))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Could not serialize type 'boolean' value at position 1:2")
                .hasRootCauseMessage("This is bogus exception");

        assertThat(exceptions.get(1))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Could not serialize type 'boolean' value at position 3:2")
                .hasRootCauseMessage("This is bogus exception");

        assertThat(exceptions.get(2))
                .isInstanceOf(PrestoException.class)
                .hasMessage("Could not serialize type 'boolean' value at position 5:2")
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
                .withColumns(columns, types)
                .withExceptionConsumer(exceptionConsumer)
                .addPages(pages)
                .build();

        assertFalse(rows.isEmpty(), "rows are empty");
        assertThat(rows.getTotalRowsCount()).isEqualTo(3);

        assertThat(getAllValues(rows))
                .hasSize(3)
                .containsExactly(newArrayList(0, null), newArrayList(1, null), newArrayList(2, true));
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
        private List<Throwable> exceptions = new ArrayList<>();

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
}
