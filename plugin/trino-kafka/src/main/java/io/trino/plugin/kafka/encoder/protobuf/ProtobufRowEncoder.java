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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UninitializedMessageException;
import io.trino.plugin.kafka.encoder.AbstractRowEncoder;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.ENUM;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;
import static com.google.protobuf.util.Timestamps.checkValid;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_TIMESTAMP;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtobufRowEncoder
        extends AbstractRowEncoder
{
    public static final String NAME = "protobuf";

    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN, INTEGER, BIGINT, DOUBLE, REAL);

    private final Descriptor descriptor;
    private final DynamicMessage.Builder messageBuilder;

    public ProtobufRowEncoder(Descriptor descriptor, ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        super(session, columnHandles);
        for (EncoderColumnHandle columnHandle : this.columnHandles) {
            checkArgument(columnHandle.getFormatHint() == null, "formatHint must be null");
            checkArgument(columnHandle.getDataFormat() == null, "dataFormat must be null");

            checkArgument(isSupportedType(columnHandle.getType()), "Unsupported column type '%s' for column '%s'", columnHandle.getType(), columnHandle.getName());
        }
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.messageBuilder = DynamicMessage.newBuilder(this.descriptor);
    }

    private boolean isSupportedType(Type type)
    {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type instanceof ArrayType arrayType) {
            return isSupportedType(arrayType.getElementType());
        }

        if (type instanceof MapType mapType) {
            return isSupportedType(mapType.getKeyType()) && isSupportedType(mapType.getValueType());
        }

        if (type instanceof RowType rowType) {
            checkArgument(rowType.getFields().stream().allMatch(field -> field.getName().isPresent()), "expecting name for field in rows");
            for (Type fieldType : rowType.getFieldTypes()) {
                if (!isSupportedType(fieldType)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private boolean isSupportedPrimitive(Type type)
    {
        return (type instanceof TimestampType timestampType && timestampType.isShort()) ||
                type instanceof VarcharType ||
                type instanceof VarbinaryType ||
                SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    @Override
    protected void appendNullValue()
    {
        // a field that tracks presence is left unset, which decodes as NULL; the others have no value for NULL
        checkNullAllowed(findField(columnHandles.get(currentColumnIndex).getMapping()));
    }

    private FieldDescriptor findField(String columnMapping)
    {
        List<String> columnPath = Splitter.on("/")
                .omitEmptyStrings()
                .limit(2)
                .splitToList(columnMapping);
        FieldDescriptor fieldDescriptor = descriptor.findFieldByName(columnPath.get(0));
        checkState(fieldDescriptor != null, "Unknown Field %s", columnPath.get(0));
        if (columnPath.size() == 2) {
            checkState(fieldDescriptor.getJavaType() == MESSAGE, "Expected MESSAGE type, but got: %s", fieldDescriptor.getJavaType());
            fieldDescriptor = fieldDescriptor.getMessageType().findFieldByName(columnPath.get(1));
            checkState(fieldDescriptor != null, "Unknown Field %s", columnPath.get(1));
        }
        return fieldDescriptor;
    }

    private static void checkNullAllowed(FieldDescriptor fieldDescriptor)
    {
        if (fieldDescriptor.isRequired()) {
            // a message without a required field cannot be built
            throw new TrinoException(CONSTRAINT_VIOLATION, format("NULL value not allowed for required protobuf field '%s'", fieldDescriptor.getFullName()));
        }
        if (fieldDescriptor.hasPresence()) {
            return;
        }
        if (fieldDescriptor.isRepeated()) {
            throw new TrinoException(CONSTRAINT_VIOLATION, format("NULL value not allowed for repeated protobuf field '%s'", fieldDescriptor.getFullName()));
        }
        throw new TrinoException(CONSTRAINT_VIOLATION, format("NULL value not allowed for protobuf field '%s', which does not track presence; declare it optional to allow NULL", fieldDescriptor.getFullName()));
    }

    @Override
    protected void appendLong(long value)
    {
        append(value);
    }

    @Override
    protected void appendInt(int value)
    {
        append(value);
    }

    @Override
    protected void appendShort(short value)
    {
        append(value);
    }

    @Override
    protected void appendDouble(double value)
    {
        append(value);
    }

    @Override
    protected void appendFloat(float value)
    {
        append(value);
    }

    @Override
    protected void appendByte(byte value)
    {
        append(value);
    }

    @Override
    protected void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    protected void appendString(String value)
    {
        append(value);
    }

    @Override
    protected void appendByteBuffer(ByteBuffer value)
    {
        append(value);
    }

    @Override
    protected void appendArray(List<Object> value)
    {
        append(value);
    }

    @Override
    protected void appendSqlTimestamp(SqlTimestamp value)
    {
        append(value);
    }

    @Override
    protected void appendMap(Map<Object, Object> value)
    {
        append(value);
    }

    @Override
    protected void appendRow(List<Object> value)
    {
        append(value);
    }

    @Override
    public byte[] toByteArray()
    {
        resetColumnIndex();
        try {
            return messageBuilder.build().toByteArray();
        }
        catch (UninitializedMessageException e) {
            // a NULL left a message typed field unset whose message has required fields, such as through a flattened mapping
            throw new TrinoException(CONSTRAINT_VIOLATION, format("Protobuf message '%s' cannot be written: %s", descriptor.getFullName(), e.getMessage()), e);
        }
        finally {
            messageBuilder.clear();
        }
    }

    private void append(Object value)
    {
        setField(descriptor, messageBuilder, columnHandles.get(currentColumnIndex).getType(), columnHandles.get(currentColumnIndex).getMapping(), value);
    }

    private DynamicMessage setField(Descriptor descriptor, DynamicMessage.Builder messageBuilder, Type type, String columnMapping, Object value)
    {
        List<String> columnPath = Splitter.on("/")
                .omitEmptyStrings()
                .limit(2)
                .splitToList(columnMapping);
        FieldDescriptor fieldDescriptor = descriptor.findFieldByName(columnPath.get(0));
        checkState(fieldDescriptor != null, "Unknown Field %s", columnPath.get(0));
        if (columnPath.size() == 2) {
            checkState(fieldDescriptor.getJavaType() == MESSAGE, "Expected MESSAGE type, but got: %s", fieldDescriptor.getJavaType());
            value = setField(
                    fieldDescriptor.getMessageType(),
                    DynamicMessage.newBuilder((DynamicMessage) messageBuilder.getField(fieldDescriptor)),
                    type,
                    columnPath.get(1),
                    value);
        }
        else {
            value = encodeObject(fieldDescriptor, type, value);
        }
        setField(fieldDescriptor, messageBuilder, value);
        // required fields of a nested message may be set by later columns; the whole message is checked when it is written
        return messageBuilder.buildPartial();
    }

    private Object encodeObject(FieldDescriptor fieldDescriptor, Type type, Object value)
    {
        requireNonNull(value, "value is null");
        if (type instanceof VarbinaryType) {
            if (value instanceof SqlVarbinary sqlVarbinary) {
                return ByteString.copyFrom(sqlVarbinary.getBytes());
            }
            if (value instanceof ByteBuffer byteBuffer) {
                return ByteString.copyFrom(byteBuffer, byteBuffer.limit());
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("cannot decode object of '%s' as '%s'", value.getClass(), type));
        }
        if (type instanceof TimestampType) {
            checkArgument(value instanceof SqlTimestamp, "value should be an instance of SqlTimestamp");
            return encodeTimestamp((SqlTimestamp) value);
        }
        if (type instanceof ArrayType arrayType) {
            checkArgument(value instanceof List, "value should be an instance of List<Object>");
            return encodeArray(fieldDescriptor, arrayType, (List<Object>) value);
        }
        if (type instanceof MapType mapType) {
            checkArgument(value instanceof Map, "value should be an instance of Map<Object, Object>");
            return encodeMap(fieldDescriptor, mapType, (Map<Object, Object>) value);
        }
        if (type instanceof RowType rowType) {
            checkArgument(value instanceof List, "value should be an instance of List<Object>");
            return encodeRow(fieldDescriptor, rowType, (List<Object>) value);
        }
        return value;
    }

    private Timestamp encodeTimestamp(SqlTimestamp timestamp)
    {
        int nanos = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        try {
            return checkValid(Timestamp.newBuilder()
                    .setSeconds(floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND))
                    .setNanos(nanos)
                    .build());
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_TIMESTAMP, e.getMessage());
        }
    }

    private List<Object> encodeArray(FieldDescriptor fieldDescriptor, ArrayType type, List<Object> value)
    {
        return value.stream()
                .map(element -> {
                    if (element == null) {
                        throw new TrinoException(CONSTRAINT_VIOLATION, format("NULL element not allowed in repeated protobuf field '%s'", fieldDescriptor.getFullName()));
                    }
                    return encodeObject(fieldDescriptor, type.getElementType(), element);
                })
                .collect(toImmutableList());
    }

    private List<DynamicMessage> encodeMap(FieldDescriptor fieldDescriptor, MapType type, Map<Object, Object> value)
    {
        Descriptor descriptor = fieldDescriptor.getMessageType();
        ImmutableList.Builder<DynamicMessage> dynamicMessageListBuilder = ImmutableList.builder();
        for (Entry<Object, Object> entry : value.entrySet()) {
            if (entry.getValue() == null) {
                throw new TrinoException(CONSTRAINT_VIOLATION, format("NULL map value not allowed for protobuf field '%s'", fieldDescriptor.getFullName()));
            }
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
            setField(
                    descriptor.findFieldByNumber(1),
                    builder,
                    encodeObject(
                            descriptor.findFieldByNumber(1),
                            type.getKeyType(),
                            entry.getKey()));
            setField(
                    descriptor.findFieldByNumber(2),
                    builder,
                    encodeObject(
                            descriptor.findFieldByNumber(2),
                            type.getValueType(),
                            entry.getValue()));
            dynamicMessageListBuilder.add(builder.build());
        }
        return dynamicMessageListBuilder.build();
    }

    private DynamicMessage encodeRow(FieldDescriptor fieldDescriptor, RowType type, List<Object> value)
    {
        Descriptor descriptor = fieldDescriptor.getMessageType();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        int index = 0;
        for (RowType.Field field : type.getFields()) {
            checkArgument(field.getName().isPresent(), "FieldName is absent");
            FieldDescriptor rowFieldDescriptor = descriptor.findFieldByName(field.getName().get());
            checkState(rowFieldDescriptor != null, "Unknown Field %s", field.getName().get());
            Object fieldValue = value.get(index);
            index++;
            if (fieldValue == null) {
                checkNullAllowed(rowFieldDescriptor);
                continue;
            }
            setField(
                    rowFieldDescriptor,
                    builder,
                    encodeObject(rowFieldDescriptor, field.getType(), fieldValue));
        }
        return builder.build();
    }

    private void setField(FieldDescriptor fieldDescriptor, DynamicMessage.Builder builder, Object value)
    {
        if (fieldDescriptor.getJavaType() == ENUM) {
            if (value instanceof List<?> values) {
                value = values.stream()
                        .map(element -> toEnumValue(fieldDescriptor, element))
                        .collect(toImmutableList());
            }
            else {
                value = toEnumValue(fieldDescriptor, value);
            }
        }
        builder.setField(fieldDescriptor, value);
    }

    private static EnumValueDescriptor toEnumValue(FieldDescriptor fieldDescriptor, Object value)
    {
        EnumValueDescriptor enumValue = fieldDescriptor.getEnumType().findValueByName((String) value);
        if (enumValue == null) {
            throw new TrinoException(CONSTRAINT_VIOLATION, format("Value '%s' is not a member of enum %s of field %s", value, fieldDescriptor.getEnumType().getFullName(), fieldDescriptor.getFullName()));
        }
        return enumValue;
    }
}
