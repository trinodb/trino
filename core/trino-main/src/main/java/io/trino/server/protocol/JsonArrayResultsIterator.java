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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class JsonArrayResultsIterator
        extends AbstractIterator<List<Object>>
        implements Iterable<List<Object>>
{
    private final Deque<Page> queue;
    private final Session session;
    private final ImmutableList<Page> pages;
    private final List<OutputColumn> columns;
    private final boolean supportsParametricDateTime;
    private final Consumer<TrinoException> exceptionConsumer;

    private Page currentPage;
    private int rowPosition = -1;
    private int inPageIndex = -1;

    public JsonArrayResultsIterator(Session session, List<Page> pages, List<OutputColumn> columns, Consumer<TrinoException> exceptionConsumer)
    {
        this.pages = ImmutableList.copyOf(pages);
        this.queue = new ArrayDeque<>(pages);
        this.session = requireNonNull(session, "session is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());
        this.exceptionConsumer = requireNonNull(exceptionConsumer, "exceptionConsumer is null");
        this.currentPage = queue.pollFirst();
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
                currentPage = queue.pollFirst();

                if (currentPage == null) {
                    return endOfData();
                }

                inPageIndex = 0;
            }

            rowPosition++;

            List<Object> row = getRowValues();
            if (row != null) {
                // row is not skipped, return it
                return row;
            }
        }
    }

    @Nullable
    private List<Object> getRowValues()
    {
        // types are present if data is present
        List<Object> row = new ArrayList<>(columns.size());
        for (OutputColumn outputColumn : columns) {
            Type type = outputColumn.type();
            Block block = currentPage.getBlock(outputColumn.sourcePageChannel());

            try {
                Object value = type.getObjectValue(session.toConnectorSession(), block, inPageIndex);
                if (!supportsParametricDateTime) {
                    value = getLegacyValue(value, type);
                }
                row.add(value);
            }
            catch (Throwable throwable) {
                propagateException(rowPosition, outputColumn.sourcePageChannel(), outputColumn.columnName(), outputColumn.type(), throwable);
                // skip row as it contains non-serializable value
                return null;
            }
        }
        return unmodifiableList(row);
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

            if (type instanceof TimeWithTimeZoneType) {
                return ((SqlTimeWithTimeZone) value).roundTo(3);
            }
        }

        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();

            if (!(elementType instanceof TimestampType || elementType instanceof TimestampWithTimeZoneType)) {
                return value;
            }

            List<Object> listValue = (List<Object>) value;
            List<Object> legacyValues = new ArrayList<>(listValue.size());
            for (Object element : listValue) {
                legacyValues.add(getLegacyValue(element, elementType));
            }

            return unmodifiableList(legacyValues);
        }

        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();

            Map<Object, Object> mapValue = (Map<Object, Object>) value;
            Map<Object, Object> result = Maps.newHashMapWithExpectedSize(mapValue.size());
            mapValue.forEach((key, val) -> result.put(getLegacyValue(key, keyType), getLegacyValue(val, valueType)));
            return unmodifiableMap(result);
        }

        if (type instanceof RowType) {
            List<RowType.Field> fields = ((RowType) type).getFields();
            List<Object> values = (List<Object>) value;

            List<Object> result = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                result.add(getLegacyValue(values.get(i), fields.get(i).getType()));
            }
            return unmodifiableList(result);
        }

        return value;
    }

    private void propagateException(int row, int channel, String name, Type type, Throwable cause)
    {
        // columns and rows are 0-indexed
        String message = format("Could not serialize column '%s' of type '%s' at position %d:%d",
                name,
                type,
                row + 1,
                channel + 1);

        exceptionConsumer.accept(new TrinoException(SERIALIZATION_ERROR, message, cause));
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new JsonArrayResultsIterator(session, pages, columns, exceptionConsumer);
    }

    public static Iterable<List<Object>> toIterableList(Session session, QueryResultRows rows, Consumer<TrinoException> serializationExceptionHandler)
    {
        if (rows.getOutputColumns().isEmpty()) {
            return emptyList();
        }

        List<OutputColumn> columnAndTypes = rows.getOutputColumns().orElseThrow();
        return new JsonArrayResultsIterator(
                session,
                rows.getPages(),
                columnAndTypes,
                serializationExceptionHandler);
    }
}
