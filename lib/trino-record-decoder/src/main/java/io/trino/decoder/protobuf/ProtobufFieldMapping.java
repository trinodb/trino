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

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.protobuf.CodedInputStream.decodeZigZag32;
import static com.google.protobuf.CodedInputStream.decodeZigZag64;
import static com.google.protobuf.CodedOutputStream.encodeZigZag64;

/**
 * Maps the field a column names in the reader schema (the schema the table columns were derived
 * from) to the field of the schema a record was written with. A field is identified by its
 * number on the wire, so the writer field is the one with the reader field's number, provided
 * it is encoded the same way, and its value is read as the reader field's type would read it;
 * a renamed field keeps its column, and a column whose field the writer does not have, or has
 * reused for a different kind of value, is {@code NULL}.
 */
final class ProtobufFieldMapping
{
    // message types that are mapped to a column type of their own rather than to a ROW of their fields
    private static final Set<String> MAPPED_MESSAGE_TYPES = ImmutableSet.of("google.protobuf.Any", "google.protobuf.Timestamp");

    private ProtobufFieldMapping() {}

    /**
     * The reader type to map the fields of a record with: the reader descriptor, if the record is
     * of the same message type. A record of another message type has no field numbers in common
     * with the columns, so its fields are looked up by name.
     */
    static Optional<Descriptor> getReaderType(Optional<Descriptor> readerDescriptor, Descriptor writerType)
    {
        return readerDescriptor.filter(reader -> reader.getFullName().equals(writerType.getFullName()));
    }

    /**
     * The reader field a column path element names, if there is a reader type that declares it.
     */
    static Optional<FieldDescriptor> findReaderField(Optional<Descriptor> readerType, String name)
    {
        return readerType.map(type -> type.findFieldByName(name));
    }

    /**
     * The writer field a column path element selects, or {@code null} if the writer has no
     * compatible field for it. Without a reader field, which is the case without a reader type
     * or for a {@code oneof} column, the name identifies the field.
     */
    @Nullable
    static FieldDescriptor findWriterField(Descriptor writerType, Optional<FieldDescriptor> readerField, String name)
    {
        if (readerField.isEmpty()) {
            return writerType.findFieldByName(name);
        }
        FieldDescriptor writerField = writerType.findFieldByNumber(readerField.get().getNumber());
        if (writerField == null || !isCompatible(readerField.get(), writerField)) {
            return null;
        }
        return writerField;
    }

    /**
     * The reader type of the values of a message-typed field, for mapping the fields nested in it.
     */
    static Optional<Descriptor> getReaderFieldType(Optional<FieldDescriptor> readerField)
    {
        return readerField
                .filter(field -> field.getJavaType() == JavaType.MESSAGE)
                .map(FieldDescriptor::getMessageType);
    }

    /**
     * Converts the value of a writer field to the value the reader field reads from the same
     * bytes, following the conversions of the "Updating A Message Type" rules of the protobuf
     * language guide. Values of a message type are not converted, as their fields are mapped
     * when the message is read.
     */
    @Nullable
    static Object toReaderValue(Optional<FieldDescriptor> readerField, FieldDescriptor writerField, @Nullable Object value)
    {
        if (value == null || readerField.isEmpty() || readerField.get().getJavaType() == JavaType.MESSAGE) {
            return value;
        }
        FieldDescriptor reader = readerField.get();
        if (reader == writerField || (reader.getType() == writerField.getType() && reader.getJavaType() != JavaType.ENUM)) {
            return value;
        }
        if (value instanceof List<?> list) {
            return list.stream()
                    .map(element -> toReaderScalar(reader, writerField, element))
                    .collect(toImmutableList());
        }
        return toReaderScalar(reader, writerField, value);
    }

    private static Object toReaderScalar(FieldDescriptor reader, FieldDescriptor writer, Object value)
    {
        return switch (encoding(reader.getType())) {
            case VARINT, ZIGZAG_VARINT -> fromVarint(reader, toVarint(writer, value));
            case LENGTH_DELIMITED -> {
                if (value instanceof ByteString bytes && reader.getJavaType() == JavaType.STRING) {
                    yield bytes.toStringUtf8();
                }
                if (value instanceof String string && reader.getJavaType() == JavaType.BYTE_STRING) {
                    yield ByteString.copyFromUtf8(string);
                }
                yield value;
            }
            // the other types replace only types that are read into the same Java type
            case FIXED32, FIXED64, FLOAT, DOUBLE, MESSAGE, GROUP -> value;
        };
    }

    // the value of a varint field as the 64 bit integer on the wire
    private static long toVarint(FieldDescriptor field, Object value)
    {
        return switch (field.getType()) {
            case UINT32 -> Integer.toUnsignedLong((Integer) value);
            case BOOL -> ((Boolean) value) ? 1 : 0;
            case ENUM -> ((EnumValueDescriptor) value).getNumber();
            case SINT32, SINT64 -> encodeZigZag64(((Number) value).longValue());
            default -> ((Number) value).longValue();
        };
    }

    // the value a varint field reads from the 64 bit integer on the wire; 32 bit fields read its low 32 bits
    private static Object fromVarint(FieldDescriptor field, long value)
    {
        return switch (field.getType()) {
            case INT32, UINT32 -> (int) value;
            case BOOL -> value != 0;
            case ENUM -> field.getEnumType().findValueByNumberCreatingIfUnknown((int) value);
            case SINT32 -> decodeZigZag32((int) value);
            case SINT64 -> decodeZigZag64(value);
            default -> value;
        };
    }

    private static boolean isCompatible(FieldDescriptor readerField, FieldDescriptor writerField)
    {
        if (readerField.isRepeated() != writerField.isRepeated()
                || readerField.isMapField() != writerField.isMapField()
                || encoding(readerField.getType()) != encoding(writerField.getType())) {
            return false;
        }
        if (readerField.isMapField()) {
            // the key and value of a map are read like fields of their own
            Descriptor readerEntry = readerField.getMessageType();
            Descriptor writerEntry = writerField.getMessageType();
            return isCompatible(readerEntry.findFieldByNumber(1), writerEntry.findFieldByNumber(1))
                    && isCompatible(readerEntry.findFieldByNumber(2), writerEntry.findFieldByNumber(2));
        }
        if (readerField.getJavaType() == JavaType.MESSAGE) {
            String readerTypeName = readerField.getMessageType().getFullName();
            String writerTypeName = writerField.getMessageType().getFullName();
            return readerTypeName.equals(writerTypeName)
                    || (!MAPPED_MESSAGE_TYPES.contains(readerTypeName) && !MAPPED_MESSAGE_TYPES.contains(writerTypeName));
        }
        return true;
    }

    /**
     * Field types that can replace each other without changing how the field's values are encoded,
     * per the "Updating A Message Type" rules of the protobuf language guide.
     */
    private static Encoding encoding(FieldDescriptor.Type type)
    {
        return switch (type) {
            case INT32, INT64, UINT32, UINT64, BOOL, ENUM -> Encoding.VARINT;
            case SINT32, SINT64 -> Encoding.ZIGZAG_VARINT;
            case STRING, BYTES -> Encoding.LENGTH_DELIMITED;
            case FIXED32, SFIXED32 -> Encoding.FIXED32;
            case FIXED64, SFIXED64 -> Encoding.FIXED64;
            case FLOAT -> Encoding.FLOAT;
            case DOUBLE -> Encoding.DOUBLE;
            case MESSAGE -> Encoding.MESSAGE;
            case GROUP -> Encoding.GROUP;
        };
    }

    private enum Encoding
    {
        VARINT,
        ZIGZAG_VARINT,
        LENGTH_DELIMITED,
        FIXED32,
        FIXED64,
        FLOAT,
        DOUBLE,
        MESSAGE,
        GROUP,
    }
}
