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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.client.ClientCapabilities;
import io.prestosql.client.Column;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class QueryResultRows
        extends AbstractIterator<List<Object>>
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final List<Type> types;
    private final List<Column> columns;
    private final Deque<Page> pages;
    private final Optional<Consumer<Throwable>> exceptionConsumer;
    private final long totalRows;
    private final boolean supportsParametricDateTime;

    private Page currentPage;
    private int rowPosition = -1;
    private int inPageIndex = -1;

    private QueryResultRows(Session session, List<Type> types, List<Column> columns, List<Page> pages)
    {
        this(session, types, columns, pages, null);
    }

    private QueryResultRows(Session session, List<Type> types, List<Column> columns, List<Page> pages, Consumer<Throwable> exceptionConsumer)
    {
        this.session = requireNonNull(session, "session is null").toConnectorSession();
        this.types = requireNonNull(types, "types is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.pages = new ArrayDeque<>(requireNonNull(pages, "pages is null"));
        this.exceptionConsumer = Optional.ofNullable(exceptionConsumer);
        this.totalRows = countRows(pages);
        this.currentPage = this.pages.pollFirst();
        this.supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());

        verify(this.types.size() == this.columns.size(), "columns and types sizes mismatch");
    }

    public boolean isEmpty()
    {
        return totalRows == 0;
    }

    public List<Column> getColumns()
    {
        return columns;
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
        if (totalRows != 1 || columns.size() != 1 || !columns.get(0).getType().equals(StandardTypes.BIGINT)) {
            return Optional.empty();
        }

        verifyNotNull(currentPage, "currentPage is null");
        Number value = (Number) types.get(0).getObjectValue(session, currentPage.getBlock(0), 0);

        return Optional.ofNullable(value).map(Number::longValue);
    }

    @Override
    protected List<Object> computeNext()
    {
        while (true) {
            if (currentPage == null) {
                return endOfData();
            }

            inPageIndex++;

            if (inPageIndex >= currentPage.getPositionCount()) {
                currentPage = pages.pollFirst();

                if (currentPage == null) {
                    return endOfData();
                }

                inPageIndex = 0;
            }

            rowPosition++;

            Optional<List<Object>> row = getRowValues();
            if (row.isEmpty()) {
                continue;
            }

            return row.get();
        }
    }

    private Optional<List<Object>> getRowValues()
    {
        List<Object> row = new ArrayList<>(columns.size());

        for (int channel = 0; channel < currentPage.getChannelCount(); channel++) {
            Type type = types.get(channel);
            Block block = currentPage.getBlock(channel);

            try {
                if (type instanceof TimestampType && !supportsParametricDateTime) {
                    row.add(channel, ((SqlTimestamp) type.getObjectValue(session, block, inPageIndex)).roundTo(3));
                }
                else if (type instanceof TimestampWithTimeZoneType && !supportsParametricDateTime) {
                    row.add(channel, ((SqlTimestampWithTimeZone) type.getObjectValue(session, block, inPageIndex)).roundTo(3));
                }
                else {
                    row.add(channel, type.getObjectValue(session, block, inPageIndex));
                }
            }
            catch (Throwable throwable) {
                propagateException(rowPosition, channel, throwable);
                // skip row as it contains non-serializable value
                return Optional.empty();
            }
        }

        return Optional.of(unmodifiableList(row));
    }

    private void propagateException(int row, int column, Throwable cause)
    {
        // columns and rows are 0-indexed
        String message = format("Could not serialize type '%s' value at position %d:%d", columns.get(column).getType(), row + 1, column + 1);
        exceptionConsumer.ifPresent(consumer -> consumer.accept(new PrestoException(SERIALIZATION_ERROR, message, cause)));
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return this;
    }

    private static long countRows(List<Page> pages)
    {
        return pages.stream()
                .map(Page::getPositionCount)
                .map(Integer::longValue)
                .reduce(Long::sum)
                .orElse(0L);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("types", types)
                .add("columns", columns)
                .add("totalRowsCount", getTotalRowsCount())
                .add("pagesCount", this.pages.size())
                .toString();
    }

    public static QueryResultRows empty(Session session)
    {
        return new QueryResultRows(session, ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    public static Builder queryResultRowsBuilder(Session session)
    {
        return new Builder(session);
    }

    public static class Builder
    {
        private final Session session;
        private ImmutableList.Builder<Page> pages = ImmutableList.builder();
        private List<Type> types = new ArrayList<>();
        private List<Column> columns = new ArrayList<>();
        private Consumer<Throwable> exceptionConsumer;

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

        public Builder withColumns(List<Column> columns, List<Type> types)
        {
            this.columns = firstNonNull(columns, emptyList());
            this.types = firstNonNull(types, emptyList());

            return this;
        }

        public Builder withSingleBooleanValue(Column column, boolean value)
        {
            BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, 1);
            BOOLEAN.writeBoolean(blockBuilder, value);
            pages = ImmutableList.<Page>builder().add(new Page(blockBuilder.build()));
            types = ImmutableList.of(BOOLEAN);
            columns = ImmutableList.of(column);

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
                    types,
                    columns,
                    pages.build(),
                    exceptionConsumer);
        }
    }
}
