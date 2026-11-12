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
package io.trino.hive.formats.line.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.WireFormat;
import io.airlift.slice.Slices;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineBuffer;
import io.trino.hive.formats.line.LineDeserializer;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Deserializer based on the Twitter Elephantbird
 * <a href="https://github.com/twitter/elephant-bird/blob/master/hive/src/main/java/com/twitter/elephantbird/hive/serde/ProtobufDeserializer.java">ProtobufDeserializer.java</a>.
 */
public class ProtobufDeserializer
        implements LineDeserializer
{
    private final List<Column> columns;
    private final RecordNavigator recordNavigator;

    public ProtobufDeserializer(List<Column> columns, Descriptor descriptor)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns"));
        this.recordNavigator = new RecordNavigator(
                requireNonNull(descriptor, "descriptor is null"),
                columns.stream()
                        .map(column -> new FieldEntry(column.name(), column.type()))
                        .collect(toImmutableList()));
    }

    @Override
    public List<? extends Type> getTypes()
    {
        return columns.stream()
                .map(Column::type)
                .collect(toImmutableList());
    }

    @Override
    public void deserialize(LineBuffer lineBuffer, PageBuilder builder)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(lineBuffer.getBuffer(), 0, lineBuffer.getLength());
        MessageRecord message = recordNavigator.parse(input);

        builder.declarePosition();
        for (int columnIndex = 0; columnIndex < this.columns.size(); columnIndex++) {
            Column column = columns.get(columnIndex);
            BlockBuilder blockBuilder = builder.getBlockBuilder(columnIndex);
            writeObject(blockBuilder, column.type(), message.columnValue(column.name()));
        }
    }

    private void writeObject(BlockBuilder blockBuilder, Type type, Object value)
    {
        if (type instanceof ArrayType t) {
            ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
                // Always create an array, even when the value is null
                if (value instanceof Collection<?> collection) {
                    collection.forEach(element -> {
                        writeObject(elementBuilder, t.getElementType(), element);
                    });
                }
            });
        }
        else if (value == null) {
            blockBuilder.appendNull();
        }
        else if (type instanceof BigintType t && value instanceof Long l) {
            t.writeLong(blockBuilder, l);
        }
        else if (type instanceof BooleanType t && value instanceof Boolean b) {
            t.writeBoolean(blockBuilder, b);
        }
        else if (type instanceof DoubleType t && value instanceof Double d) {
            t.writeDouble(blockBuilder, d);
        }
        else if (type instanceof VarcharType && value instanceof EnumValueDescriptor e) {
            type.writeSlice(blockBuilder, utf8Slice(e.getName()));
        }
        else if (type instanceof IntegerType t && value instanceof Integer i) {
            t.writeInt(blockBuilder, i);
        }
        else if (type instanceof RealType t && value instanceof Float f) {
            t.writeLong(blockBuilder, floatToRawIntBits(f));
        }
        else if (type instanceof RowType rowType && value instanceof MessageRecord rowMessage) {
            if (rowMessage.isEmpty()) {
                // The message has no set values nor default values
                // In the Hive implementation, a struct where all fields are null is returned as null
                blockBuilder.appendNull();
            }
            else {
                ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
                    for (int i = 0; i < rowType.getFields().size(); i++) {
                        RowType.Field rowField = rowType.getFields().get(i);
                        if (rowField.getName().isPresent()) {
                            writeObject(fieldBuilders.get(i), rowField.getType(), rowMessage.rowFieldValue(rowField.getName().get()));
                        }
                        else {
                            throw new IllegalStateException("Unable to apply value to row field with no name: " + i);
                        }
                    }
                });
            }
        }
        else if (type instanceof VarcharType t && value instanceof String s) {
            t.writeSlice(blockBuilder, utf8Slice(s));
        }
        else if (type instanceof VarbinaryType t && value instanceof ByteString b) {
            t.writeSlice(blockBuilder, Slices.wrappedBuffer(b.toByteArray()));
        }
        else {
            throw new IllegalStateException("Unimplemented type " + type.getDisplayName());
        }
    }

    private record FieldEntry(String name, Type type) {}

    private static class RecordNavigator
    {
        // Fields are matched to columns by name, so the field numbers come from the descriptor and
        // are independent from the ordinals of the projected columns. An array indexed by field
        // number is faster than a map when skipping over the fields that are not requested.
        private static final int MAX_FIELD_NUMBER_FOR_ARRAY_LOOKUP = 4096;

        private final FieldNavigator[] fieldsByNumber;
        private final Map<Integer, FieldNavigator> fieldsByNumberMap;
        private final Map<String, FieldNavigator> fieldsByName;

        RecordNavigator(Descriptor descriptor, List<FieldEntry> fields)
        {
            Map<Integer, FieldNavigator> fieldsByNumber = new HashMap<>();
            Map<String, FieldNavigator> fieldsByName = new HashMap<>();
            for (FieldEntry field : fields) {
                FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.name());
                if (fieldDescriptor == null) {
                    continue;
                }
                FieldNavigator navigator = new FieldNavigator(fieldDescriptor, field.type());
                fieldsByNumber.putIfAbsent(fieldDescriptor.getNumber(), navigator);
                fieldsByName.putIfAbsent(field.name(), navigator);
            }
            this.fieldsByName = ImmutableMap.copyOf(fieldsByName);

            int maxFieldNumber = 0;
            for (int fieldNumber : fieldsByNumber.keySet()) {
                maxFieldNumber = max(maxFieldNumber, fieldNumber);
            }
            if (maxFieldNumber <= MAX_FIELD_NUMBER_FOR_ARRAY_LOOKUP) {
                FieldNavigator[] array = new FieldNavigator[maxFieldNumber + 1];
                fieldsByNumber.forEach((fieldNumber, navigator) -> array[fieldNumber] = navigator);
                this.fieldsByNumber = array;
                this.fieldsByNumberMap = null;
            }
            else {
                this.fieldsByNumber = null;
                this.fieldsByNumberMap = ImmutableMap.copyOf(fieldsByNumber);
            }
        }

        MessageRecord parse(CodedInputStream input)
                throws IOException
        {
            MessageRecord record = new MessageRecord(this);
            int tag;
            while ((tag = input.readTag()) != 0) {
                int fieldNumber = WireFormat.getTagFieldNumber(tag);
                FieldNavigator navigator = fieldNavigator(fieldNumber);
                if (navigator == null) {
                    input.skipField(tag);
                    continue;
                }
                boolean addedValue;
                if (navigator.repeated) {
                    addedValue = readRepeatedElement(input, tag, navigator, record);
                }
                else {
                    addedValue = readSingularField(input, navigator, record);
                }
                if (addedValue) {
                    record.markHasValue();
                }
            }
            return record;
        }

        private FieldNavigator fieldNavigator(int fieldNumber)
        {
            if (fieldsByNumber != null) {
                return fieldNumber < fieldsByNumber.length ? fieldsByNumber[fieldNumber] : null;
            }
            return fieldsByNumberMap.get(fieldNumber);
        }

        private static boolean readSingularField(CodedInputStream input, FieldNavigator navigator, MessageRecord record)
                throws IOException
        {
            Object value = readFieldValue(input, navigator);
            if (value == null) {
                // Unknown enum value, consistent with DynamicMessage the field is treated as unset
                return false;
            }
            record.addValue(navigator.name, value);
            return true;
        }

        private static boolean readRepeatedElement(CodedInputStream input, int tag, FieldNavigator navigator, MessageRecord record)
                throws IOException
        {
            List<Object> values = record.addRepeatedField(navigator.name);
            boolean added = false;
            if (WireFormat.getTagWireType(tag) == WireFormat.WIRETYPE_LENGTH_DELIMITED && navigator.packable) {
                // Packed representation of a repeated packable field
                int length = input.readRawVarint32();
                int oldLimit = input.pushLimit(length);
                while (input.getBytesUntilLimit() > 0) {
                    Object value = readFieldValue(input, navigator);
                    if (value != null) {
                        values.add(value);
                        added = true;
                    }
                }
                input.popLimit(oldLimit);
            }
            else {
                Object value = readFieldValue(input, navigator);
                if (value != null) {
                    values.add(value);
                    added = true;
                }
            }
            return added;
        }

        private static Object readFieldValue(CodedInputStream input, FieldNavigator navigator)
                throws IOException
        {
            return switch (navigator.fieldDescriptor.getType()) {
                case DOUBLE -> input.readDouble();
                case FLOAT -> input.readFloat();
                case INT64 -> input.readInt64();
                case UINT64 -> input.readUInt64();
                case INT32 -> input.readInt32();
                case FIXED64 -> input.readFixed64();
                case FIXED32 -> input.readFixed32();
                case BOOL -> input.readBool();
                case STRING -> input.readString();
                case GROUP -> throw new UnsupportedOperationException("group fields are not supported");
                case MESSAGE -> readMessageValue(input, navigator);
                case BYTES -> input.readBytes();
                case UINT32 -> input.readUInt32();
                case ENUM -> readEnumValue(input, navigator);
                case SFIXED32 -> input.readSFixed32();
                case SFIXED64 -> input.readSFixed64();
                case SINT32 -> input.readSInt32();
                case SINT64 -> input.readSInt64();
            };
        }

        private static MessageRecord readMessageValue(CodedInputStream input, FieldNavigator navigator)
                throws IOException
        {
            int length = input.readRawVarint32();
            int oldLimit = input.pushLimit(length);
            MessageRecord value = navigator.messageNavigator.parse(input);
            input.checkLastTagWas(0);
            input.popLimit(oldLimit);
            return value;
        }

        private static Object readEnumValue(CodedInputStream input, FieldNavigator navigator)
                throws IOException
        {
            int value = input.readEnum();
            return navigator.fieldDescriptor.getEnumType().findValueByNumber(value);
        }
    }

    private static final class FieldNavigator
    {
        private final String name;
        private final boolean repeated;
        private final boolean packable;
        private final boolean hasExplicitDefault;
        private final Object defaultValue;
        private final FieldDescriptor fieldDescriptor;
        private final RecordNavigator messageNavigator;

        FieldNavigator(FieldDescriptor fieldDescriptor, Type type)
        {
            this.fieldDescriptor = fieldDescriptor;
            this.name = fieldDescriptor.getName();
            this.repeated = fieldDescriptor.isRepeated();
            this.packable = repeated && fieldDescriptor.isPackable();
            this.hasExplicitDefault = fieldDescriptor.hasDefaultValue();

            if (fieldDescriptor.getJavaType() == MESSAGE) {
                this.messageNavigator = new RecordNavigator(fieldDescriptor.getMessageType(), wantedRowFields(type));
                this.defaultValue = MessageRecord.empty(this.messageNavigator);
            }
            else {
                this.messageNavigator = null;
                this.defaultValue = repeated ? null : fieldDescriptor.getDefaultValue();
            }
        }

        private static List<FieldEntry> wantedRowFields(Type type)
        {
            return switch (type) {
                case RowType rowType -> rowType.getFields().stream()
                        .map(FieldNavigator::rowFieldEntry)
                        .collect(toImmutableList());
                case ArrayType arrayType when arrayType.getElementType() instanceof RowType elementRowType -> elementRowType.getFields().stream()
                        .map(FieldNavigator::rowFieldEntry)
                        .collect(toImmutableList());
                default -> ImmutableList.of();
            };
        }

        private static FieldEntry rowFieldEntry(RowType.Field field)
        {
            return field.getName()
                    .map(name -> new FieldEntry(name, field.getType()))
                    .orElseThrow(() -> new IllegalStateException("Unable to apply value to row field with no name"));
        }
    }

    private static final class MessageRecord
    {
        private final RecordNavigator recordNavigator;
        private final Map<String, Object> values;
        private boolean anyValue;

        MessageRecord(RecordNavigator recordNavigator)
        {
            this(recordNavigator, new HashMap<>(recordNavigator.fieldsByName.size()));
        }

        private MessageRecord(RecordNavigator recordNavigator, Map<String, Object> values)
        {
            this.recordNavigator = requireNonNull(recordNavigator, "recordNavigator is null");
            this.values = requireNonNull(values, "values is null");
        }

        static MessageRecord empty(RecordNavigator recordNavigator)
        {
            return new MessageRecord(recordNavigator, ImmutableMap.of());
        }

        public void addValue(String fieldName, Object value)
        {
            values.put(fieldName, value);
        }

        public List<Object> addRepeatedField(String fieldName)
        {
            return (List<Object>) values.computeIfAbsent(fieldName, _ -> new ArrayList<>());
        }

        public void markHasValue()
        {
            anyValue = true;
        }

        public boolean isEmpty()
        {
            if (anyValue) {
                return false;
            }
            for (FieldNavigator field : recordNavigator.fieldsByName.values()) {
                if (field.hasExplicitDefault) {
                    return false;
                }
            }
            return true;
        }

        public Object columnValue(String fieldName)
        {
            if (values.containsKey(fieldName)) {
                return values.get(fieldName);
            }
            FieldNavigator navigator = recordNavigator.fieldsByName.get(fieldName);
            if (navigator == null) {
                return null;
            }
            if (navigator.repeated) {
                return ImmutableList.of();
            }
            return navigator.defaultValue;
        }

        public Object rowFieldValue(String fieldName)
        {
            if (values.containsKey(fieldName)) {
                return values.get(fieldName);
            }
            FieldNavigator navigator = recordNavigator.fieldsByName.get(fieldName);
            if (navigator == null || navigator.repeated || !navigator.hasExplicitDefault) {
                return null;
            }
            return navigator.defaultValue;
        }
    }
}
