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
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.function.Function.identity;

/**
 * Deserializer based on the Twitter Elephantbird
 * <a href="https://github.com/twitter/elephant-bird/blob/master/hive/src/main/java/com/twitter/elephantbird/hive/serde/ProtobufDeserializer.java">ProtobufDeserializer.java</a>.
 * To improve speed, it could be rewritten using {@link com.google.protobuf.CodedInputStream} instead of the {@link DynamicMessage}.
 */
public class ProtobufDeserializer
        implements LineDeserializer
{
    private final List<Column> columns;
    private final Descriptor descriptor;
    private final Map<String, FieldDescriptor> descriptorFields;

    public ProtobufDeserializer(List<Column> columns, Descriptor descriptor)
    {
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns"));
        this.descriptor = requireNonNull(descriptor, "descriptor");
        this.descriptorFields = descriptor.getFields().stream()
                .collect(toImmutableMap(FieldDescriptor::getName, identity()));
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
        DynamicMessage message;
        try (InputStream in = new ByteArrayInputStream(lineBuffer.getBuffer(), 0, lineBuffer.getLength())) {
            message = DynamicMessage.parseFrom(this.descriptor, in);
        }

        builder.declarePosition();
        for (int columnIndex = 0; columnIndex < this.columns.size(); columnIndex++) {
            Column column = columns.get(columnIndex);
            BlockBuilder blockBuilder = builder.getBlockBuilder(columnIndex);
            writeObject(blockBuilder, column.type(), getValue(message, this.descriptorFields, column.name()));
        }
    }

    private static Object getValue(DynamicMessage message, Map<String, FieldDescriptor> fieldNameLookup, String fieldName)
    {
        FieldDescriptor fieldDescriptor = fieldNameLookup.get(fieldName);
        if (fieldDescriptor == null) {
            return null;
        }
        return requireNonNullElseGet(message.getField(fieldDescriptor),
                () -> fieldDescriptor.hasDefaultValue() ? fieldDescriptor.getDefaultValue() : null);
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
        else if (type instanceof RowType rowType && value instanceof DynamicMessage rowMessage) {
            Map<String, FieldDescriptor> fieldNameLookup = collectFieldsToDeserialize(rowMessage);
            if (fieldNameLookup.isEmpty()) {
                // The message has no set values nor default values
                // In the Hive implementation, a struct where all fields are null is returned as null
                blockBuilder.appendNull();
            }
            else {
                ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
                    for (int i = 0; i < rowType.getFields().size(); i++) {
                        RowType.Field rowField = rowType.getFields().get(i);
                        if (rowField.getName().isPresent()) {
                            writeObject(fieldBuilders.get(i), rowField.getType(), getValue(rowMessage, fieldNameLookup, rowField.getName().get()));
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

    private static Map<String, FieldDescriptor> collectFieldsToDeserialize(DynamicMessage message)
    {
        return Stream.concat(message.getAllFields().keySet().stream(),
                        message.getDescriptorForType().getFields().stream().filter(FieldDescriptor::hasDefaultValue))
                .collect(toImmutableMap(FieldDescriptor::getName, identity(), (l, r) -> l));
    }
}
