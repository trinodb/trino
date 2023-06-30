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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.StandardTypes.JSON;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtobufColumnDecoder
{
    private static final Slice EMPTY_JSON = Slices.utf8Slice("{}");

    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BooleanType.BOOLEAN,
            TinyintType.TINYINT,
            SmallintType.SMALLINT,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            VarbinaryType.VARBINARY);

    private final Type columnType;
    private final String columnMapping;
    private final String columnName;
    private final TypeManager typeManager;
    private final Type jsonType;

    public ProtobufColumnDecoder(DecoderColumnHandle columnHandle, TypeManager typeManager)
    {
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
            this.jsonType = typeManager.getType(new TypeSignature(JSON));
            this.columnType = columnHandle.getType();
            this.columnMapping = columnHandle.getMapping();
            this.columnName = columnHandle.getName();
            checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnName);
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
            checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
            checkArgument(columnHandle.getMapping() != null, "mapping not defined for column '%s'", columnName);

            checkArgument(isSupportedType(columnType), "Unsupported column type '%s' for column '%s'", columnType, columnName);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(GENERIC_USER_ERROR, e);
        }
    }

    private boolean isSupportedType(Type type)
    {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type instanceof ArrayType) {
            checkArgument(type.getTypeParameters().size() == 1, "expecting exactly one type parameter for array");
            return isSupportedType(type.getTypeParameters().get(0));
        }

        if (type instanceof MapType) {
            List<Type> typeParameters = type.getTypeParameters();
            checkArgument(typeParameters.size() == 2, "expecting exactly two type parameters for map");
            return isSupportedType(typeParameters.get(0)) && isSupportedType(type.getTypeParameters().get(1));
        }

        if (type instanceof RowType) {
            for (Type fieldType : type.getTypeParameters()) {
                if (!isSupportedType(fieldType)) {
                    return false;
                }
            }
            return true;
        }

        return type.equals(jsonType);
    }

    private static boolean isSupportedPrimitive(Type type)
    {
        return (type instanceof TimestampType && ((TimestampType) type).isShort()) ||
                type instanceof VarcharType ||
                SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public FieldValueProvider decodeField(DynamicMessage dynamicMessage)
    {
        return new ProtobufValueProvider(locateField(dynamicMessage, columnMapping), columnType, columnName, typeManager);
    }

    @Nullable
    private static Object locateField(DynamicMessage message, String columnMapping)
    {
        Object value = message;
        Optional<Descriptor> valueDescriptor = Optional.of(message.getDescriptorForType());
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(columnMapping)) {
            if (valueDescriptor.filter(descriptor -> descriptor.findFieldByName(pathElement) != null).isEmpty()) {
                // Search the message to see if this column is oneof type
                Optional<OneofDescriptor> oneofDescriptor = message.getDescriptorForType().getOneofs().stream()
                        .filter(descriptor -> descriptor.getName().equals(columnMapping))
                        .findFirst();

                return oneofDescriptor.map(descriptor -> createOneofJson(message, descriptor))
                        .orElse(null);
            }

            FieldDescriptor fieldDescriptor = valueDescriptor.get().findFieldByName(pathElement);
            value = ((DynamicMessage) value).getField(fieldDescriptor);
            valueDescriptor = getDescriptor(fieldDescriptor);
        }
        return value;
    }

    private static Optional<Descriptor> getDescriptor(FieldDescriptor fieldDescriptor)
    {
        if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
            return Optional.of(fieldDescriptor.getMessageType());
        }
        return Optional.empty();
    }

    private static Object createOneofJson(DynamicMessage message, OneofDescriptor descriptor)
    {
        // Collect all oneof field names from the descriptor
        Set<String> oneofColumns = descriptor.getFields().stream()
                .map(FieldDescriptor::getName)
                .collect(toImmutableSet());

        // Find the oneof field in the message; there will be at most one
        List<Entry<FieldDescriptor, Object>> oneofFields = message.getAllFields().entrySet().stream()
                .filter(entry -> oneofColumns.contains(entry.getKey().getName()))
                .collect(toImmutableList());

        if (oneofFields.size() > 1) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Expected to find at most one 'oneof' field in message, found fields: %s", oneofFields));
        }

        // If found, map the field to a JSON string containing a single field:value pair, else return an empty JSON string {}
        if (!oneofFields.isEmpty()) {
            try {
                // Create a new DynamicMessage where the only set field is the oneof field, so we can use the protobuf-java-util to encode the message as JSON
                // If we encoded the entire input message, it would include all fields
                Entry<FieldDescriptor, Object> oneofField = oneofFields.get(0);
                DynamicMessage oneofMessage = DynamicMessage.newBuilder(oneofField.getKey().getContainingType())
                        .setField(oneofField.getKey(), oneofField.getValue())
                        .build();
                return Slices.utf8Slice(JsonFormat.printer()
                        .omittingInsignificantWhitespace()
                        .print(oneofMessage));
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to convert oneof message to JSON", e);
            }
        }
        return EMPTY_JSON;
    }
}
