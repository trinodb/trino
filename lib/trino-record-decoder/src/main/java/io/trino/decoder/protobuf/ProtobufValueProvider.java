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
package io.trino.decoder.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtobufValueProvider
        extends FieldValueProvider
{
    @Nullable
    private final Object value;
    private final Type columnType;
    private final String columnName;
    private final Type jsonType;

    public ProtobufValueProvider(@Nullable Object value, Type columnType, String columnName, TypeManager typeManager)
    {
        this.value = value;
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
    }

    @Override
    public boolean isNull()
    {
        return value == null;
    }

    @Override
    public double getDouble()
    {
        requireNonNull(value, "value is null");
        if (value instanceof Double || value instanceof Float) {
            return ((Number) value).doubleValue();
        }
        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
    }

    @Override
    public boolean getBoolean()
    {
        requireNonNull(value, "value is null");
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
    }

    @Override
    public long getLong()
    {
        requireNonNull(value, "value is null");
        if (value instanceof Long || value instanceof Integer) {
            return ((Number) value).longValue();
        }
        if (value instanceof Float) {
            return Float.floatToIntBits((Float) value);
        }
        if (value instanceof DynamicMessage) {
            checkArgument(columnType instanceof TimestampType, "type should be an instance of Timestamp");
            return parseTimestamp(((TimestampType) columnType).getPrecision(), (DynamicMessage) value);
        }
        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), columnType, columnName));
    }

    @Override
    public Slice getSlice()
    {
        return getSlice(value, columnType, columnName);
    }

    @Override
    public Block getBlock()
    {
        return serializeObject(null, value, columnType, columnName);
    }

    private Slice getSlice(Object value, Type type, String columnName)
    {
        requireNonNull(value, "value is null");
        if ((type instanceof VarcharType && value instanceof CharSequence) || value instanceof EnumValueDescriptor) {
            return truncateToLength(utf8Slice(value.toString()), type);
        }

        if (type instanceof VarbinaryType && value instanceof ByteString) {
            return Slices.wrappedBuffer(((ByteString) value).toByteArray());
        }

        if (type.equals(jsonType)) {
            return (Slice) value;
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    @Nullable
    private Block serializeObject(BlockBuilder builder, Object value, Type type, String columnName)
    {
        if (type instanceof ArrayType) {
            return serializeList(builder, value, type, columnName);
        }
        if (type instanceof MapType mapType) {
            return serializeMap(builder, value, mapType, columnName);
        }
        if (type instanceof RowType) {
            return serializeRow(builder, value, type, columnName);
        }

        if (type.equals(jsonType)) {
            return serializeJson(builder, value, type);
        }

        serializePrimitive(builder, value, type, columnName);
        return null;
    }

    @Nullable
    private Block serializeList(BlockBuilder parentBlockBuilder, @Nullable Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }
        List<?> list = (List<?>) value;
        List<Type> typeParameters = type.getTypeParameters();
        Type elementType = typeParameters.get(0);

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            serializeObject(blockBuilder, element, elementType, columnName);
        }
        if (parentBlockBuilder != null) {
            type.writeObject(parentBlockBuilder, blockBuilder.build());
            return null;
        }
        return blockBuilder.build();
    }

    private void serializePrimitive(BlockBuilder blockBuilder, @Nullable Object value, Type type, String columnName)
    {
        requireNonNull(blockBuilder, "parent blockBuilder is null");

        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, (Boolean) value);
            return;
        }

        if ((value instanceof Integer || value instanceof Long) && (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType)) {
            type.writeLong(blockBuilder, ((Number) value).longValue());
            return;
        }

        if (type instanceof DoubleType && value instanceof Double) {
            type.writeDouble(blockBuilder, (Double) value);
            return;
        }

        if (type instanceof RealType && value instanceof Float) {
            type.writeLong(blockBuilder, floatToIntBits((Float) value));
            return;
        }

        if (type instanceof VarcharType || type instanceof VarbinaryType) {
            type.writeSlice(blockBuilder, getSlice(value, type, columnName));
            return;
        }

        if (type instanceof TimestampType && ((TimestampType) type).isShort()) {
            checkArgument(value instanceof DynamicMessage, "value should be an instance of DynamicMessage");
            type.writeLong(blockBuilder, parseTimestamp(((TimestampType) type).getPrecision(), (DynamicMessage) value));
            return;
        }

        throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED, format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
    }

    @Nullable
    private Block serializeMap(BlockBuilder parentBlockBuilder, @Nullable Object value, MapType type, String columnName)
    {
        if (value == null) {
            checkState(parentBlockBuilder != null, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        Collection<DynamicMessage> dynamicMessages = ((Collection<?>) value).stream()
                .map(DynamicMessage.class::cast)
                .collect(toImmutableList());
        Type keyType = type.getKeyType();
        Type valueType = type.getValueType();

        MapBlockBuilder blockBuilder;
        if (parentBlockBuilder != null) {
            blockBuilder = (MapBlockBuilder) parentBlockBuilder;
        }
        else {
            blockBuilder = type.createBlockBuilder(null, 1);
        }

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            for (DynamicMessage dynamicMessage : dynamicMessages) {
                if (dynamicMessage.getField(dynamicMessage.getDescriptorForType().findFieldByNumber(1)) != null) {
                    serializeObject(keyBuilder, dynamicMessage.getField(getFieldDescriptor(dynamicMessage, 1)), keyType, columnName);
                    serializeObject(valueBuilder, dynamicMessage.getField(getFieldDescriptor(dynamicMessage, 2)), valueType, columnName);
                }
            }
        });

        if (parentBlockBuilder == null) {
            return blockBuilder.getObject(0, Block.class);
        }
        return null;
    }

    @Nullable
    private Block serializeRow(BlockBuilder blockBuilder, @Nullable Object value, Type type, String columnName)
    {
        if (value == null) {
            checkState(blockBuilder != null, "parent block builder is null");
            blockBuilder.appendNull();
            return null;
        }

        RowType rowType = (RowType) type;
        if (blockBuilder == null) {
            return buildRowValue(rowType, fieldBuilders -> buildRow(rowType, columnName, (DynamicMessage) value, fieldBuilders));
        }
        ((RowBlockBuilder) blockBuilder).buildEntry((fieldBuilders) -> buildRow(rowType, columnName, (DynamicMessage) value, fieldBuilders));
        return null;
    }

    private void buildRow(RowType rowType, String columnName, DynamicMessage record, List<BlockBuilder> fieldBuilders)
    {
        List<RowType.Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            checkState(field.getName().isPresent(), "field name not found");
            FieldDescriptor fieldDescriptor = getFieldDescriptor(record, field.getName().get());
            checkState(fieldDescriptor != null, format("Unknown Field %s", field.getName().get()));
            serializeObject(
                    fieldBuilders.get(i),
                    record.getField(fieldDescriptor),
                    field.getType(),
                    columnName);
        }
    }

    @Nullable
    private static Block serializeJson(BlockBuilder builder, Object value, Type type)
    {
        if (builder != null) {
            type.writeObject(builder, value);
            return null;
        }
        return (Block) value;
    }

    private static long parseTimestamp(int precision, DynamicMessage timestamp)
    {
        long seconds = (Long) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("seconds"));
        int nanos = (Integer) timestamp.getField(timestamp.getDescriptorForType().findFieldByName("nanos"));
        long micros = seconds * MICROSECONDS_PER_SECOND;
        micros += roundDiv(nanos, NANOSECONDS_PER_MICROSECOND);
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision (" + MAX_SHORT_PRECISION + ")");
        return round(micros, MAX_SHORT_PRECISION - precision);
    }

    private static FieldDescriptor getFieldDescriptor(DynamicMessage message, String name)
    {
        return message.getDescriptorForType().findFieldByName(name);
    }

    private static FieldDescriptor getFieldDescriptor(DynamicMessage message, int index)
    {
        return message.getDescriptorForType().findFieldByNumber(index);
    }
}
