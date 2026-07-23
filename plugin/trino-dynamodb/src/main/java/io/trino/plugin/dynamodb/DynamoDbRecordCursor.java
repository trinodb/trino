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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DynamoDbRecordCursor
        implements RecordCursor
{
    private final List<DynamoDbColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final Iterator<Map<String, AttributeValue>> items;

    private Map<String, AttributeValue> currentItem;

    public DynamoDbRecordCursor(DynamoDbSplit split, List<DynamoDbColumnHandle> columnHandles, DynamoDbService service)
    {
        requireNonNull(split, "split is null");
        requireNonNull(service, "service is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            fieldToColumnIndex[i] = columnHandles.get(i).getOrdinalPosition();
        }

        ScanIterable pages = service.scanTable(split.getTableName(), split.getSegment(), split.getTotalSegments());
        items = StreamSupport.stream(pages.spliterator(), false)
                .flatMap(page -> page.items().stream())
                .iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!items.hasNext()) {
            return false;
        }
        currentItem = items.next();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkArgument(getType(field).equals(BooleanType.BOOLEAN), "Expected BOOLEAN field");
        AttributeValue value = getAttributeValue(field);
        if (value == null || value.type() == AttributeValue.Type.NUL) {
            return false;
        }
        if (value.type() == AttributeValue.Type.BOOL) {
            return Boolean.TRUE.equals(value.bool());
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                format("Cannot convert DynamoDB attribute type %s to BOOLEAN", value.type()));
    }

    @Override
    public long getLong(int field)
    {
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "getLong is not used by this connector");
    }

    @Override
    public double getDouble(int field)
    {
        checkArgument(getType(field).equals(DoubleType.DOUBLE), "Expected DOUBLE field");
        AttributeValue value = getAttributeValue(field);
        if (value == null || value.type() == AttributeValue.Type.NUL) {
            return 0.0;
        }
        if (value.type() == AttributeValue.Type.N) {
            return Double.parseDouble(value.n());
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                format("Cannot convert DynamoDB attribute type %s to DOUBLE", value.type()));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkArgument(getType(field) instanceof VarcharType, "Expected VARCHAR field");
        AttributeValue value = getAttributeValue(field);
        if (value == null || value.type() == AttributeValue.Type.NUL) {
            return Slices.EMPTY_SLICE;
        }
        return Slices.utf8Slice(attributeValueToString(value));
    }

    @Override
    public Object getObject(int field)
    {
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "getObject is not supported");
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        AttributeValue value = getAttributeValue(field);
        return value == null || value.type() == AttributeValue.Type.NUL;
    }

    @Override
    public void close() {}

    private AttributeValue getAttributeValue(int field)
    {
        String columnName = columnHandles.get(field).getColumnName();
        return currentItem.get(columnName);
    }

    private static String attributeValueToString(AttributeValue value)
    {
        return switch (value.type()) {
            case S -> value.s();
            case N -> value.n();
            case BOOL -> Boolean.toString(Boolean.TRUE.equals(value.bool()));
            case NUL -> "";
            case B -> Base64.getEncoder().encodeToString(value.b().asByteArray());
            case SS -> toJsonArray(value.ss().stream().map(s -> "\"" + escapeJson(s) + "\"").iterator());
            case NS -> toJsonArray(value.ns().iterator());
            case BS -> toJsonArray(value.bs().stream()
                    .map(SdkBytes::asByteArray)
                    .map(Base64.getEncoder()::encodeToString)
                    .map(s -> "\"" + s + "\"")
                    .iterator());
            case M -> mapToJsonString(value.m());
            case L -> listToJsonString(value.l());
            case UNKNOWN_TO_SDK_VERSION -> value.toString();
        };
    }

    private static String mapToJsonString(Map<String, AttributeValue> map)
    {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, AttributeValue> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
            sb.append(attributeValueToString(entry.getValue()));
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private static String listToJsonString(List<AttributeValue> list)
    {
        ImmutableList.Builder<String> elements = ImmutableList.builder();
        for (AttributeValue element : list) {
            elements.add(attributeValueToString(element));
        }
        return toJsonArray(elements.build().iterator());
    }

    private static String toJsonArray(Iterator<String> elements)
    {
        StringBuilder sb = new StringBuilder("[");
        boolean first = true;
        while (elements.hasNext()) {
            if (!first) {
                sb.append(",");
            }
            sb.append(elements.next());
            first = false;
        }
        sb.append("]");
        return sb.toString();
    }

    private static String escapeJson(String value)
    {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
