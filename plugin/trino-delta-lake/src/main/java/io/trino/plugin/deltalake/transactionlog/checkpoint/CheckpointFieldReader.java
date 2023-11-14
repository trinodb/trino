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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import jakarta.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CheckpointFieldReader
{
    private final ConnectorSession session;
    private final SqlRow row;
    private final Map<String, Integer> fieldNameToIndex;

    public CheckpointFieldReader(ConnectorSession session, SqlRow row, RowType type)
    {
        this.session = requireNonNull(session, "session is null");
        this.row = requireNonNull(row, "row is null");
        checkArgument(row.getFieldCount() == type.getFields().size(), "row and type sizes don't match");
        Map<String, Integer> fieldNames = new HashMap<>();
        for (int i = 0; i < type.getFields().size(); i++) {
            String fieldName = type.getFields().get(i).getName().orElseThrow();
            checkState(!fieldNames.containsKey(fieldName), "Duplicated field '%s' exists in %s", fieldName, type);
            fieldNames.put(fieldName, i);
        }
        this.fieldNameToIndex = ImmutableMap.copyOf(fieldNames);
    }

    public boolean getBoolean(String fieldName)
    {
        int field = requireField(fieldName);
        ByteArrayBlock valueBlock = (ByteArrayBlock) row.getUnderlyingFieldBlock(field);
        return valueBlock.getByte(row.getUnderlyingFieldPosition(field)) != 0;
    }

    public int getInt(String fieldName)
    {
        int field = requireField(fieldName);
        IntArrayBlock valueBlock = (IntArrayBlock) row.getUnderlyingFieldBlock(field);
        return valueBlock.getInt(row.getUnderlyingFieldPosition(field));
    }

    public OptionalInt getOptionalInt(String fieldName)
    {
        OptionalInt index = findField(fieldName);
        if (index.isEmpty()) {
            return OptionalInt.empty();
        }

        IntArrayBlock valueBlock = (IntArrayBlock) row.getUnderlyingFieldBlock(index.getAsInt());
        int position = row.getUnderlyingFieldPosition(index.getAsInt());
        if (valueBlock.isNull(position)) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(valueBlock.getInt(position));
    }

    public long getLong(String fieldName)
    {
        int field = requireField(fieldName);
        LongArrayBlock valueBlock = (LongArrayBlock) row.getUnderlyingFieldBlock(field);
        return valueBlock.getLong(row.getUnderlyingFieldPosition(field));
    }

    @Nullable
    public String getString(String fieldName)
    {
        int field = requireField(fieldName);
        VariableWidthBlock valueBlock = (VariableWidthBlock) row.getUnderlyingFieldBlock(field);
        int index = row.getUnderlyingFieldPosition(field);
        if (valueBlock.isNull(index)) {
            return null;
        }
        return valueBlock.getSlice(index).toStringUtf8();
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(ArrayType stringList, String fieldName)
    {
        int field = requireField(fieldName);
        ArrayBlock valueBlock = (ArrayBlock) row.getUnderlyingFieldBlock(field);
        return (List<String>) stringList.getObjectValue(session, valueBlock, row.getUnderlyingFieldPosition(field));
    }

    @SuppressWarnings("unchecked")
    public Optional<Set<String>> getOptionalSet(ArrayType stringList, String fieldName)
    {
        OptionalInt index = findField(fieldName);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        ArrayBlock valueBlock = (ArrayBlock) row.getUnderlyingFieldBlock(index.getAsInt());
        int position = row.getUnderlyingFieldPosition(index.getAsInt());
        if (valueBlock.isNull(position)) {
            return Optional.empty();
        }
        List<String> list = (List<String>) stringList.getObjectValue(session, valueBlock, position);
        return Optional.of(ImmutableSet.copyOf(list));
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getMap(MapType stringMap, String fieldName)
    {
        int field = requireField(fieldName);
        MapBlock valueBlock = (MapBlock) row.getUnderlyingFieldBlock(field);
        return (Map<String, String>) stringMap.getObjectValue(session, valueBlock, row.getUnderlyingFieldPosition(field));
    }

    @Nullable
    public SqlRow getRow(String fieldName)
    {
        OptionalInt index = findField(fieldName);
        if (index.isEmpty()) {
            return null;
        }
        RowBlock valueBlock = (RowBlock) row.getUnderlyingFieldBlock(index.getAsInt());
        int position = row.getUnderlyingFieldPosition(index.getAsInt());
        if (valueBlock.isNull(position)) {
            return null;
        }
        return valueBlock.getRow(position);
    }

    private int requireField(String fieldName)
    {
        return findField(fieldName)
                .orElseThrow(() -> new IllegalArgumentException("Field '%s' doesn't exist in %s".formatted(fieldName, fieldNameToIndex.keySet())));
    }

    private OptionalInt findField(String fieldName)
    {
        Integer index = fieldNameToIndex.get(fieldName);
        if (index == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(index);
    }
}
