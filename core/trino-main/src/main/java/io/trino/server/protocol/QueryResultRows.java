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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.Column;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class QueryResultRows
        implements Iterable<List<Object>>
{
    private final Session session;
    private final Optional<List<ColumnAndType>> columns;
    private final List<Page> pages;
    private final Consumer<Throwable> exceptionConsumer;
    private final long totalRows;

    private QueryResultRows(Session session, Optional<List<ColumnAndType>> columns, List<Page> pages, Consumer<Throwable> exceptionConsumer)
    {
        this.session = requireNonNull(session, "session is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.pages = ImmutableList.copyOf(pages);
        this.exceptionConsumer = requireNonNull(exceptionConsumer, "exceptionConsumer is null");
        this.totalRows = countRows(pages);

        verify(totalRows == 0 || (totalRows > 0 && columns.isPresent()), "data present without columns and types");
    }

    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    public Optional<List<Column>> getColumns()
    {
        return columns.map(columns -> columns.stream()
                .map(ColumnAndType::getColumn)
                .collect(toImmutableList()));
    }

    /**
     * Returns expected row count (we don't know yet if every row is serializable).
     */
    @VisibleForTesting
    public long getTotalRowsCount()
    {
        return totalRows;
    }

    public Optional<Long> getUpdateCount()
    {
        // We should have exactly single bigint value as an update count.
        if (totalRows != 1 || columns.isEmpty()) {
            return Optional.empty();
        }

        List<ColumnAndType> columns = this.columns.get();

        if (columns.size() != 1 || !columns.get(0).getType().equals(BIGINT)) {
            return Optional.empty();
        }

        checkState(!pages.isEmpty(), "no data pages available");
        Number value = (Number) columns.get(0).getType().getObjectValue(session.toConnectorSession(), pages.get(0).getBlock(0), 0);

        return Optional.ofNullable(value).map(Number::longValue);
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new JsonArrayResultsIterator(session.toConnectorSession(), pages, columns.orElseThrow(), session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString()), exceptionConsumer);
    }

    private static long countRows(List<Page> pages)
    {
        long rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columns", columns)
                .add("totalRowsCount", getTotalRowsCount())
                .add("pagesCount", this.pages.size())
                .toString();
    }

    public static QueryResultRows empty(Session session)
    {
        return new QueryResultRows(session, Optional.empty(), ImmutableList.of(), null);
    }

    public static Builder queryResultRowsBuilder(Session session)
    {
        return new Builder(session);
    }

    public static class Builder
    {
        private final Session session;
        private ImmutableList.Builder<Page> pages = ImmutableList.builder();
        private Optional<List<ColumnAndType>> columns = Optional.empty();
        private Consumer<Throwable> exceptionConsumer = (throwable) -> {};

        public Builder(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        public Builder addPage(Page page)
        {
            pages.add(page);
            return this;
        }

        public Builder addPages(List<Page> page)
        {
            pages.addAll(page);
            return this;
        }

        public Builder withColumnsAndTypes(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            if (columns != null || types != null) {
                this.columns = Optional.of(combine(columns, types));
            }

            return this;
        }

        public Builder withSingleBooleanValue(Column column, boolean value)
        {
            BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 1);
            BOOLEAN.writeBoolean(blockBuilder, value);
            pages = ImmutableList.<Page>builder().add(new Page(blockBuilder.build()));
            columns = Optional.of(combine(ImmutableList.of(column), ImmutableList.of(BOOLEAN)));

            return this;
        }

        public Builder withExceptionConsumer(Consumer<Throwable> exceptionConsumer)
        {
            this.exceptionConsumer = exceptionConsumer;
            return this;
        }

        public QueryResultRows build()
        {
            return new QueryResultRows(
                    session,
                    columns,
                    pages.build(),
                    exceptionConsumer);
        }

        private static List<ColumnAndType> combine(@Nullable List<Column> columns, @Nullable List<Type> types)
        {
            checkArgument(columns != null && types != null, "columns and types must be present at the same time");
            checkArgument(columns.size() == types.size(), "columns and types size mismatch");

            ImmutableList.Builder<ColumnAndType> builder = ImmutableList.builderWithExpectedSize(columns.size());

            for (int i = 0; i < columns.size(); i++) {
                builder.add(new ColumnAndType(i, columns.get(i), types.get(i)));
            }

            return builder.build();
        }
    }
}
