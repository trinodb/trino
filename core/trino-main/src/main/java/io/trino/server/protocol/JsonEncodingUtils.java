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
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
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
import java.util.List;
import java.util.function.Consumer;

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

    public interface TypeEncoder
    {
        void encode(JsonGenerator generator, Block block, int position)
                throws IOException;
    }

    private static class BigintEncoder
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
    }

    private static class IntegerEncoder
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
    }

    private static class BooleanEncoder
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
    }

    private static class SmallintEncoder
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
    }

    private static class TinyintEncoder
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
    }

    private static class DoubleEncoder
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
    }

    private static class RealEncoder
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
    }

    private static class VarcharEncoder
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
    }

    private static class CharEncoder
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
    }

    private static class VarbinaryEncoder
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
    }

    private static class ArrayEncoder
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
    }

    private static class MapEncoder
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
    }

    private static class RowEncoder
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
    }

    private static class TypeObjectValueEncoder
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
