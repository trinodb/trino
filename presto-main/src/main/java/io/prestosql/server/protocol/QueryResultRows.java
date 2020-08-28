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
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class QueryResultRows
        extends AbstractIterator<List<Object>>
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final Optional<List<ColumnAndType>> columns;
    private final Deque<Page> pages;
    private final Optional<Consumer<Throwable>> exceptionConsumer;
    private final long totalRows;
    private final boolean supportsParametricDateTime;

    private Page currentPage;
    private int rowPosition = -1;
    private int inPageIndex = -1;

    private QueryResultRows(Session session, Optional<List<ColumnAndType>> columns, List<Page> pages, Consumer<Throwable> exceptionConsumer)
    {
        this.session = requireNonNull(session, "session is null").toConnectorSession();
        this.columns = requireNonNull(columns, "columns is null");
        this.pages = new ArrayDeque<>(requireNonNull(pages, "pages is null"));
        this.exceptionConsumer = Optional.ofNullable(exceptionConsumer);
        this.totalRows = countRows(pages);
        this.currentPage = this.pages.pollFirst();
        this.supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());

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

        verifyNotNull(currentPage, "currentPage is null");
        Number value = (Number) columns.get(0).getType().getObjectValue(session, currentPage.getBlock(0), 0);

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
        // types are present if data is present
        List<ColumnAndType> columns = this.columns.orElseThrow();
        List<Object> row = new ArrayList<>(columns.size());

        for (int channel = 0; channel < currentPage.getChannelCount(); channel++) {
            ColumnAndType column = columns.get(channel);
            Type type = column.getType();
            Block block = currentPage.getBlock(channel);

            try {
                if (supportsParametricDateTime) {
                    row.add(channel, type.getObjectValue(session, block, inPageIndex));
                }
                else {
                    row.add(channel, getLegacyValue(type.getObjectValue(session, block, inPageIndex), type));
                }
            }
            catch (Throwable throwable) {
                propagateException(rowPosition, column, throwable);
                // skip row as it contains non-serializable value
                return Optional.empty();
            }
        }

        return Optional.of(unmodifiableList(row));
    }

    private Object getLegacyValue(Object value, Type type)
    {
        if (value == null) {
            return null;
        }

        if (!supportsParametricDateTime) {
            // for legacy clients we need to round timestamp and timestamp with timezone to default precision (3)

            if (type instanceof TimestampType) {
                return ((SqlTimestamp) value).roundTo(3);
            }

            if (type instanceof TimestampWithTimeZoneType) {
                return ((SqlTimestampWithTimeZone) value).roundTo(3);
            }

            if (type instanceof TimeType) {
                return ((SqlTime) value).roundTo(3);
            }
        }

        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();

            if (!(elementType instanceof TimestampType || elementType instanceof TimestampWithTimeZoneType)) {
                return value;
            }

            return unmodifiableList(((List<Object>) value).stream()
                    .map(element -> getLegacyValue(element, elementType))
                    .collect(toList()));
        }

        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();

            Map<Object, Object> result = new HashMap<>();
            ((Map<Object, Object>) value).forEach((key, val) -> result.put(getLegacyValue(key, keyType), getLegacyValue(val, valueType)));
            return unmodifiableMap(result);
        }

        if (type instanceof RowType) {
            List<RowType.Field> fields = ((RowType) type).getFields();
            List<Object> values = (List<Object>) value;
            List<Type> types = fields.stream()
                    .map(RowType.Field::getType)
                    .collect(toImmutableList());

            List<Object> result = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.add(i, getLegacyValue(values.get(i), types.get(i)));
            }
            return result;
        }

        return value;
    }

    private void propagateException(int row, ColumnAndType column, Throwable cause)
    {
        // columns and rows are 0-indexed
        String message = format("Could not serialize column '%s' of type '%s' at position %d:%d",
                column.getColumn().getName(),
                column.getType(),
                row + 1,
                column.getPosition() + 1);

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

            ImmutableList.Builder<ColumnAndType> builder = ImmutableList.builder();

            for (int i = 0; i < columns.size(); i++) {
                builder.add(new ColumnAndType(i, columns.get(i), types.get(i)));
            }

            return builder.build();
        }
    }

    private static class ColumnAndType
    {
        private final int position;
        private final Column column;
        private final Type type;

        private ColumnAndType(int position, Column column, Type type)
        {
            this.position = position;
            this.column = column;
            this.type = type;
        }

        public Column getColumn()
        {
            return column;
        }

        public Type getType()
        {
            return type;
        }

        public int getPosition()
        {
            return position;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("column", column)
                    .add("type", type)
                    .add("position", position)
                    .toString();
        }
    }
}
