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
package io.prestosql.plugin.kafka.encoder.raw;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.RowEncoder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RawRowEncoder
        implements RowEncoder
{
    private enum FieldType
    {
        BYTE(Byte.SIZE),
        SHORT(Short.SIZE),
        INT(Integer.SIZE),
        LONG(Long.SIZE),
        FLOAT(Float.SIZE),
        DOUBLE(Double.SIZE);

        private final int size;

        FieldType(int bitSize)
        {
            this.size = bitSize / 8;
        }

        public int getSize()
        {
            return size;
        }
    }

    private static final Pattern MAPPING_PATTERN = Pattern.compile("(\\d+)(?::(\\d+))?");
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, BOOLEAN);

    public static final String NAME = "raw";

    private ByteBuffer buffer;
    private final int capacity;
    private int[] indexes;

    public RawRowEncoder(Set<EncoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        this.capacity = parseColumns(columnHandles);
        this.buffer = ByteBuffer.allocate(this.capacity);
    }

    // Performs checks on each column handle, set indexes for columns, returns expected capacity of ByteBuffer
    private int parseColumns(Set<EncoderColumnHandle> columnHandles)
    {
        int index = 0;
        indexes = new int[columnHandles.size()];
        for (EncoderColumnHandle columnHandle : columnHandles) {
            int length;
            try {
                requireNonNull(columnHandle, "columnHandle is null");
                checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnHandle.getName());
                checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnHandle.getName());

                FieldType fieldType = parseFieldType(columnHandle);

                length = parseMapping(columnHandle, fieldType);

                checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType().getDisplayName(), columnHandle.getName());

                checkFieldType(columnHandle, fieldType);
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(GENERIC_USER_ERROR, e);
            }

            indexes[getPosition(columnHandle)] = index;
            index += length;
        }
        return index;
    }

    private void checkFieldType(EncoderColumnHandle columnHandle, FieldType fieldType)
    {
        String columnName = columnHandle.getName();
        Type columnType = columnHandle.getType();
        if (columnType == BIGINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
        }
        if (columnType == INTEGER) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT);
        }
        if (columnType == SMALLINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT);
        }
        if (columnType == TINYINT) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
        }
        if (columnType == BOOLEAN) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE, FieldType.SHORT, FieldType.INT, FieldType.LONG);
        }
        if (columnType == DOUBLE) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.DOUBLE, FieldType.FLOAT);
        }
        if (isVarcharType(columnType)) {
            checkFieldTypeOneOf(fieldType, columnName, columnType, FieldType.BYTE);
        }
    }

    private void checkFieldTypeOneOf(FieldType declaredFieldType, String columnName, Type columnType, FieldType... allowedFieldTypes)
    {
        if (!Arrays.asList(allowedFieldTypes).contains(declaredFieldType)) {
            throw new IllegalArgumentException(format(
                    "Wrong dataFormat '%s' specified for column '%s'; %s type implies use of %s",
                    declaredFieldType.name(),
                    columnName,
                    columnType.getDisplayName(),
                    Joiner.on("/").join(allowedFieldTypes)));
        }
    }

    private FieldType parseFieldType(EncoderColumnHandle columnHandle)
    {
        FieldType fieldType;
        try {
            fieldType = Optional.ofNullable(columnHandle.getDataFormat())
                    .map(dataFormat -> FieldType.valueOf(dataFormat.toUpperCase(Locale.ENGLISH)))
                    .orElse(FieldType.BYTE);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(format("invalid dataFormat '%s' for column '%s'", columnHandle.getDataFormat(), columnHandle.getName()));
        }
        return fieldType;
    }

    private int parseMapping(EncoderColumnHandle columnHandle, FieldType fieldType)
    {
        String mapping = Optional.ofNullable(columnHandle.getMapping()).orElse("0");
        Matcher mappingMatcher = MAPPING_PATTERN.matcher(mapping);
        if (!mappingMatcher.matches()) {
            throw new IllegalArgumentException(format("invalid mapping format '%s' for column '%s'", mapping, columnHandle.getName()));
        }
        int start = parseInt(mappingMatcher.group(1));
        OptionalInt end;
        if (mappingMatcher.group(2) != null) {
            end = OptionalInt.of(parseInt(mappingMatcher.group(2)));
        }
        else {
            if (!isVarcharType(columnHandle.getType())) {
                end = OptionalInt.of(start + fieldType.getSize());
            }
            else {
                end = OptionalInt.empty();
            }
        }

        checkArgument(start >= 0, "start offset %s for column '%s' must be greater or equal 0", start, columnHandle.getName());
        end.ifPresent(endValue -> {
            checkArgument(endValue >= 0, "end offset %s for column '%s' must be greater or equal 0", endValue, columnHandle.getName());
            checkArgument(endValue > start, "end offset %s for column '%s' must greater than start offset", endValue, columnHandle.getName());
        });

        int length = end.isPresent() ? end.getAsInt() - start : fieldType.getSize();

        if (!isVarcharType(columnHandle.getType())) {
            checkArgument(!end.isPresent() || end.getAsInt() - start == length,
                    "Bytes mapping for column '%s' does not match dataFormat '%s'; expected %s bytes but got %s",
                    columnHandle.getName(),
                    length,
                    end.getAsInt() - start);
        }

        return length;
    }

    private static boolean isSupportedType(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    private int getPosition(EncoderColumnHandle columnHandle)
    {
        return columnHandle.getOrdinalPosition() - 1;
    }

    @Override
    public byte[] toByteArray()
    {
        return buffer.array();
    }

    @Override
    public void clear()
    {
        buffer = ByteBuffer.allocate(capacity);
    }

    @Override
    public RowEncoder putNullValue(EncoderColumnHandle columnHandle)
    {
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, long value)
    {
        buffer.putLong(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, int value)
    {
        buffer.putInt(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, short value)
    {
        buffer.putShort(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte value)
    {
        buffer.put(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, double value)
    {
        buffer.putDouble(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, float value)
    {
        buffer.putFloat(indexes[getPosition(columnHandle)], value);
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, boolean value)
    {
        buffer.put(indexes[getPosition(columnHandle)], (byte) (value ? 1 : 0));
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, String value)
    {
        int position = getPosition(columnHandle);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        if (position + 1 >= indexes.length) {
            buffer.put(valueBytes, indexes[position], valueBytes.length);
        }
        else {
            buffer.put(valueBytes, indexes[position], indexes[position + 1] - indexes[position]);
        }
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, ByteBuffer value)
    {
        int position = getPosition(columnHandle);
        byte[] valueBytes = value.array();
        if (position + 1 >= indexes.length) {
            buffer.put(valueBytes, indexes[position], valueBytes.length);
        }
        else {
            buffer.put(valueBytes, indexes[position], indexes[position + 1] - indexes[position]);
        }
        return this;
    }

    @Override
    public RowEncoder put(EncoderColumnHandle columnHandle, byte[] value)
    {
        int position = getPosition(columnHandle);
        if (position + 1 >= indexes.length) {
            buffer.put(value, indexes[position], value.length);
        }
        else {
            buffer.put(value, indexes[position], indexes[position + 1] - indexes[position]);
        }
        return this;
    }
}
