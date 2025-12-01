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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.SqlIntervalDayTime;
import io.trino.type.SqlIntervalYearMonth;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class JsonEncodingUtils
{
    private JsonEncodingUtils() {}

    private static final BigintEncoder BIGINT_ENCODER = new BigintEncoder();
    private static final BooleanEncoder BOOLEAN_ENCODER = new BooleanEncoder();
    private static final IntegerEncoder INTEGER_ENCODER = new IntegerEncoder();
    private static final SmallintEncoder SMALLINT_ENCODER = new SmallintEncoder();
    private static final DoubleEncoder DOUBLE_ENCODER = new DoubleEncoder();
    private static final RealEncoder REAL_ENCODER = new RealEncoder();
    private static final TinyintEncoder TINYINT_ENCODER = new TinyintEncoder();
    private static final VarcharEncoder VARCHAR_ENCODER = new VarcharEncoder();
    private static final VarbinaryEncoder VARBINARY_ENCODER = new VarbinaryEncoder();

    public static TypeEncoder[] createTypeEncoders(Session session, List<Type> types)
    {
        verify(!types.isEmpty(), "Columns must not be empty");

        boolean supportsParametricDateTime = requireNonNull(session, "session is null")
                .getClientCapabilities()
                .contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());

        return types.stream()
                .map(type -> createTypeEncoder(type, supportsParametricDateTime))
                .toArray(TypeEncoder[]::new);
    }

    public static TypeEncoder createTypeEncoder(Type type, boolean supportsParametricDateTime)
    {
        return switch (type) {
            case BigintType _ -> BIGINT_ENCODER;
            case BooleanType _ -> BOOLEAN_ENCODER;
            case IntegerType _ -> INTEGER_ENCODER;
            case SmallintType _ -> SMALLINT_ENCODER;
            case DoubleType _ -> DOUBLE_ENCODER;
            case RealType _ -> REAL_ENCODER;
            case TinyintType _ -> TINYINT_ENCODER;
            case VarcharType _ -> VARCHAR_ENCODER;
            case VarbinaryType _ -> VARBINARY_ENCODER;
            case CharType charType -> new CharEncoder(charType.getLength());
            // TODO: add specialized Short/Long decimal encoders
            case ArrayType arrayType -> new ArrayEncoder(arrayType, createTypeEncoder(arrayType.getElementType(), supportsParametricDateTime));
            case MapType mapType -> new MapEncoder(mapType, createTypeEncoder(mapType.getValueType(), supportsParametricDateTime));
            case RowType rowType -> new RowEncoder(rowType, rowType.getTypeParameters()
                    .stream()
                    .map(elementType -> createTypeEncoder(elementType, supportsParametricDateTime))
                    .toArray(TypeEncoder[]::new));
            case Type _ -> new TypeObjectValueEncoder(type, supportsParametricDateTime);
        };
    }

    public static void writePagesToJsonGenerator(Consumer<TrinoException> throwableConsumer, JsonGenerator generator, TypeEncoder[] typeEncoders, int[] sourcePageChannels, List<Page> pages)
    {
        verify(typeEncoders.length == sourcePageChannels.length, "Source page channels and type encoders must have the same length");
        try {
            generator.writeStartArray();

            for (Page page : pages) {
                Block[] blocks = new Block[sourcePageChannels.length];
                for (int i = 0; i < sourcePageChannels.length; i++) {
                    blocks[i] = page.getBlock(sourcePageChannels[i]);
                }

                validateBlockTypeEncoders(typeEncoders, blocks);

                for (int position = 0; position < page.getPositionCount(); position++) {
                    generator.writeStartArray();
                    for (int column = 0; column < typeEncoders.length; column++) {
                        typeEncoders[column].encode(generator, blocks[column], position);
                    }
                    generator.writeEndArray();
                }
            }
            generator.writeEndArray();
            generator.flush(); // final flush to have the data written to the output stream
        }
        catch (Exception e) {
            throwableConsumer.accept(new TrinoException(SERIALIZATION_ERROR, "Could not serialize data to JSON", e));
        }
    }

    private static String describeBlock(Block block)
    {
        return switch (block) {
            case DictionaryBlock dictionaryBlock -> "DictionaryBlock{%s}".formatted(describeBlock(dictionaryBlock.getDictionary()));
            case RunLengthEncodedBlock runLengthEncodedBlock -> "RleBlock{%s}".formatted(runLengthEncodedBlock.getValue());
            case ArrayBlock arrayBlock -> "ArrayBlock{%s}".formatted(describeBlock(arrayBlock.getElementsBlock()));
            case MapBlock mapBlock -> "MapBlock{keys=%s, values=%s}".formatted(describeBlock(mapBlock.getKeyBlock()), describeBlock(mapBlock.getValueBlock()));
            case RowBlock rowBlock -> "RowBlock{fields=%s}".formatted(rowBlock.getFieldBlocks().stream().map(JsonEncodingUtils::describeBlock).collect(joining(", ")));
            default -> block.toString();
        };
    }

    private static Block unwrapBlock(Block block)
    {
        return block.getUnderlyingValueBlock();
    }

    private static void validateBlockTypeEncoders(TypeEncoder[] typeEncoders, Block[] blocks)
    {
        verify(typeEncoders.length == blocks.length, "Expected type encoders to have the same length as blocks");
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (int i = 0; i < typeEncoders.length; i++) {
            if (!typeEncoders[i].isSupported(unwrapBlock(blocks[i]))) {
                builder.add("channel %d: %s tried to encode %s".formatted(i, typeEncoders[i], describeBlock(blocks[i])));
            }
        }

        List<String> mismatched = builder.build();
        if (!mismatched.isEmpty()) {
            throw new TrinoException(SERIALIZATION_ERROR, "Encoders %s are not matching block types %s during serialization: %s".formatted(
                    Arrays.toString(typeEncoders),
                    Arrays.stream(blocks)
                            .map(JsonEncodingUtils::describeBlock)
                            .collect(joining(", ")),
                    mismatched));
        }
    }

    public sealed interface TypeEncoder
    {
        void encode(JsonGenerator generator, Block block, int position)
                throws IOException;

        boolean isSupported(Block block);
    }

    private static final class BigintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(BIGINT.getLong(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof LongArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class IntegerEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(INTEGER.getInt(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof IntArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class BooleanEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeBoolean(BOOLEAN.getBoolean(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof ByteArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class SmallintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(SMALLINT.getShort(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof ShortArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class TinyintEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(TINYINT.getByte(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof ByteArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class DoubleEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(DOUBLE.getDouble(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof LongArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class RealEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            generator.writeNumber(REAL.getFloat(block, position));
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof IntArrayBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class VarcharEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            Slice slice = VARCHAR.getSlice(block, position);
            generator.writeUTF8String(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof VariableWidthBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class CharEncoder
            implements TypeEncoder
    {
        private final int length;

        private CharEncoder(int length)
        {
            this.length = length;
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            Slice slice = padSpaces(VARCHAR.getSlice(block, position), length);
            generator.writeString(slice.toStringUtf8());
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof VariableWidthBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class VarbinaryEncoder
            implements TypeEncoder
    {
        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            // Optimization: avoid copying Slice to byte array
            Slice slice = VARBINARY.getSlice(block, position);
            generator.writeBinary(slice.byteArray(), slice.byteArrayOffset(), slice.length());
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof VariableWidthBlock;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static final class ArrayEncoder
            implements TypeEncoder
    {
        private final ArrayType arrayType;
        private final TypeEncoder typeEncoder;

        public ArrayEncoder(ArrayType arrayType, TypeEncoder typeEncoder)
        {
            this.arrayType = requireNonNull(arrayType, "arrayType is null");
            this.typeEncoder = requireNonNull(typeEncoder, "typeEncoder is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            Block arrayBlock = arrayType.getObject(block, position);
            generator.writeStartArray();
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                typeEncoder.encode(generator, arrayBlock, i);
            }
            generator.writeEndArray();
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof ArrayBlock arrayBlock && typeEncoder.isSupported(unwrapBlock(arrayBlock.getElementsBlock()));
        }

        @Override
        public String toString()
        {
            return "ArrayEncoder{elements=%s}".formatted(typeEncoder);
        }
    }

    private static final class MapEncoder
            implements TypeEncoder
    {
        private final MapType mapType;
        private final TypeEncoder valueEncoder;

        public MapEncoder(MapType mapType, TypeEncoder valueEncoder)
        {
            this.mapType = requireNonNull(mapType, "mapType is null");
            this.valueEncoder = requireNonNull(valueEncoder, "valueEncoder is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            SqlMap map = mapType.getObject(block, position);
            int offset = map.getRawOffset();
            Block keyBlock = map.getRawKeyBlock();
            Block valueBlock = map.getRawValueBlock();

            verify(keyBlock.getPositionCount() == valueBlock.getPositionCount(), "Key and value blocks have different number of positions");
            generator.writeStartObject();
            for (int i = 0; i < map.getSize(); i++) {
                // Map keys are always serialized as strings for backward compatibility with existing clients.
                // Map values are always properly encoded using their types.
                // TODO: improve in v2 JSON format
                generator.writeFieldName(mapType.getKeyType().getObjectValue(keyBlock, offset + i).toString());
                valueEncoder.encode(generator, valueBlock, offset + i);
            }
            generator.writeEndObject();
        }

        @Override
        public boolean isSupported(Block block)
        {
            return block instanceof MapBlock mapBlock && valueEncoder.isSupported(unwrapBlock(mapBlock.getValueBlock()));
        }

        @Override
        public String toString()
        {
            return "MapEncoder{keys=%s, values=%s}".formatted(mapType.getKeyType().getDisplayName(), valueEncoder);
        }
    }

    private static final class RowEncoder
            implements TypeEncoder
    {
        private final RowType rowType;
        private final TypeEncoder[] fieldEncoders;

        public RowEncoder(RowType rowType, TypeEncoder[] fieldEncoders)
        {
            this.rowType = requireNonNull(rowType, "rowType is null");
            this.fieldEncoders = requireNonNull(fieldEncoders, "fieldEncoders is null");
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }
            SqlRow row = rowType.getObject(block, position);
            generator.writeStartArray();
            for (int i = 0; i < row.getFieldCount(); i++) {
                fieldEncoders[i].encode(generator, row.getRawFieldBlock(i), row.getRawIndex());
            }
            generator.writeEndArray();
        }

        @Override
        public boolean isSupported(Block block)
        {
            if (!(block instanceof RowBlock rowBlock)) {
                return false;
            }

            for (int i = 0; i < fieldEncoders.length; i++) {
                if (!fieldEncoders[i].isSupported(unwrapBlock(rowBlock.getFieldBlock(i)))) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public String toString()
        {
            String fields = IntStream.range(0, fieldEncoders.length)
                    .mapToObj(i -> "%s: %s".formatted(rowType.getFields().get(i).getName().orElse("_col" + i), fieldEncoders[i]))
                    .collect(joining(", "));

            return "RowEncoder{%s}".formatted(fields);
        }
    }

    private static final class TypeObjectValueEncoder
            implements TypeEncoder
    {
        private final Type type;
        private final boolean supportsParametricDateTime;

        public TypeObjectValueEncoder(Type type, boolean supportsParametricDateTime)
        {
            this.type = requireNonNull(type, "type is null");
            this.supportsParametricDateTime = supportsParametricDateTime;
        }

        @Override
        public void encode(JsonGenerator generator, Block block, int position)
                throws IOException
        {
            if (block.isNull(position)) {
                generator.writeNull();
                return;
            }

            Object value = roundParametricTypes(type.getObjectValue(block, position));

            switch (value) {
                case BigDecimal bigDecimalValue -> generator.writeNumber(bigDecimalValue);
                case SqlDate dateValue -> generator.writeString(dateValue.toString());
                case SqlDecimal decimalValue -> generator.writeString(decimalValue.toString());
                case SqlIntervalDayTime intervalValue -> generator.writeString(intervalValue.toString());
                case SqlIntervalYearMonth intervalValue -> generator.writeString(intervalValue.toString());
                case SqlTime timeValue -> generator.writeString(timeValue.toString());
                case SqlTimeWithTimeZone timeWithTimeZone -> generator.writeString(timeWithTimeZone.toString());
                case SqlTimestamp timestamp -> generator.writeString(timestamp.toString());
                case SqlTimestampWithTimeZone timestampWithTimeZone -> generator.writeString(timestampWithTimeZone.toString());
                case SqlVarbinary sqlVarbinary -> generator.writeBinary(sqlVarbinary.getBytes());
                default -> generator.writePOJO(value);
            }
        }

        @Override
        public boolean isSupported(Block block)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "TypeObjectValueEncoder{type=%s}".formatted(type.getDisplayName());
        }

        private Object roundParametricTypes(Object value)
        {
            if (supportsParametricDateTime) {
                return value;
            }

            return switch (value) {
                case SqlTimestamp sqlTimestamp -> sqlTimestamp.roundTo(3);
                case SqlTimestampWithTimeZone sqlTimestampWithTimeZone -> sqlTimestampWithTimeZone.roundTo(3);
                case SqlTime sqlTime -> sqlTime.roundTo(3);
                case SqlTimeWithTimeZone sqlTimeWithTimeZone -> sqlTimeWithTimeZone.roundTo(3);
                default -> value;
            };
        }
    }
}
