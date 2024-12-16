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
package io.trino.hive.formats.ion;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.google.common.collect.ImmutableList;
import io.trino.hive.formats.line.Column;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;

public class IonEncoderFactory
{
    private IonEncoderFactory() {}

    public static IonEncoder buildEncoder(List<Column> columns)
    {
        return RowEncoder.forFields(columns.stream()
                .map(c -> new RowType.Field(Optional.of(c.name().toLowerCase(Locale.ROOT)), c.type()))
                .toList());
    }

    private interface BlockEncoder
    {
        void encode(IonWriter writer, Block block, int position)
                throws IOException;
    }

    private static BlockEncoder encoderForType(Type type)
    {
        return switch (type) {
            case TinyintType _ -> byteEncoder;
            case SmallintType _ -> shortEncoder;
            case IntegerType _ -> intEncoder;
            case BigintType _ -> longEncoder;
            case BooleanType _ -> boolEncoder;
            case VarbinaryType _ -> binaryEncoder;
            case RealType _ -> realEncoder;
            case DoubleType _ -> doubleEncoder;
            case VarcharType _, CharType _ -> stringEncoder;
            case DecimalType t -> decimalEncoder(t);
            case DateType _ -> dateEncoder;
            case TimestampType t -> timestampEncoder(t);
            case MapType t -> new MapEncoder(t, t.getKeyType(),
                    encoderForType(t.getValueType()));
            case RowType t -> RowEncoder.forFields(t.getFields());
            case ArrayType t -> new ArrayEncoder(wrapEncoder(encoderForType(t.getElementType())));
            default -> throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
        };
    }

    private static BlockEncoder wrapEncoder(BlockEncoder encoder)
    {
        return (writer, block, position) ->
        {
            if (block.isNull(position)) {
                writer.writeNull();
            }
            else {
                encoder.encode(writer, block, position);
            }
        };
    }

    private record RowEncoder(List<String> fieldNames, List<BlockEncoder> fieldEncoders)
            implements BlockEncoder, IonEncoder
    {
        private static RowEncoder forFields(List<RowType.Field> fields)
        {
            ImmutableList.Builder<String> fieldNamesBuilder = ImmutableList.builder();
            ImmutableList.Builder<BlockEncoder> fieldEncodersBuilder = ImmutableList.builder();

            for (RowType.Field field : fields) {
                fieldNamesBuilder.add(field.getName().get().toLowerCase(Locale.ROOT));
                fieldEncodersBuilder.add(wrapEncoder(encoderForType(field.getType())));
            }

            return new RowEncoder(fieldNamesBuilder.build(), fieldEncodersBuilder.build());
        }

        @Override
        public void encode(IonWriter writer, Block block, int position)
                throws IOException
        {
            encodeStruct(writer, ((RowBlock) block)::getFieldBlock, position);
        }

        @Override
        public void encode(IonWriter writer, Page page)
                throws IOException
        {
            for (int i = 0; i < page.getPositionCount(); i++) {
                encodeStruct(writer, page::getBlock, i);
            }
            // todo: it's probably preferable to decouple ion writer flushes
            //       from page sizes, but it's convenient for now
            writer.flush();
        }

        private void encodeStruct(IonWriter writer, IntFunction<Block> blockSelector, int position)
                throws IOException
        {
            writer.stepIn(IonType.STRUCT);
            for (int i = 0; i < fieldEncoders.size(); i++) {
                // Omit the filed when the field is null
                Block block = blockSelector.apply(i);
                if (block.isNull(position)) {
                    continue;
                }
                writer.setFieldName(fieldNames.get(i));
                fieldEncoders.get(i)
                        .encode(writer, block, position);
            }
            writer.stepOut();
        }
    }

    private record MapEncoder(MapType mapType, Type keyType,
                        BlockEncoder encoder)
            implements BlockEncoder
    {
        public MapEncoder(MapType mapType, Type keyType, BlockEncoder encoder)

        {
            this.mapType = mapType;
            if (!(keyType instanceof VarcharType _ || keyType instanceof CharType _)) {
                throw new UnsupportedOperationException("Unsupported map key type: " + keyType);
            }
            this.keyType = keyType;
            this.encoder = encoder;
        }

        @Override
        public void encode(IonWriter writer, Block block, int position)
                throws IOException
        {
            SqlMap sqlMap = mapType.getObject(block, position);
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();

            writer.stepIn(IonType.STRUCT);
            for (int i = 0; i < sqlMap.getSize(); i++) {
                checkArgument(!rawKeyBlock.isNull(rawOffset + i), "map key is null");
                writer.setFieldName(VarcharType.VARCHAR.getSlice(rawKeyBlock, rawOffset + i).toString(StandardCharsets.UTF_8));
                encoder.encode(writer, rawValueBlock, rawOffset + i);
            }
            writer.stepOut();
        }
    }

    private record ArrayEncoder(BlockEncoder elementEncoder)
            implements BlockEncoder
    {
        @Override
        public void encode(IonWriter writer, Block block, int position)
                throws IOException
        {
            writer.stepIn(IonType.LIST);
            Block elementBlock = ((ArrayBlock) block).getArray(position);
            for (int i = 0; i < elementBlock.getPositionCount(); i++) {
                elementEncoder.encode(writer, elementBlock, i);
            }
            writer.stepOut();
        }
    }

    private static BlockEncoder timestampEncoder(TimestampType type)
    {
        if (type.isShort()) {
            return (writer, block, position) -> {
                long epochMicros = type.getLong(block, position);
                BigDecimal decimalMillis = BigDecimal.valueOf(epochMicros)
                        .movePointLeft(3)
                        .setScale(type.getPrecision() - 3, RoundingMode.UNNECESSARY);

                writer.writeTimestamp(Timestamp.forMillis(decimalMillis, 0));
            };
        }
        else {
            return (writer, block, position) -> {
                LongTimestamp longTimestamp = (LongTimestamp) type.getObject(block, position);
                BigDecimal picosOfMicros = BigDecimal.valueOf(longTimestamp.getPicosOfMicro())
                        .movePointLeft(9);
                BigDecimal decimalMillis = BigDecimal.valueOf(longTimestamp.getEpochMicros())
                        .movePointLeft(3)
                        .add(picosOfMicros)
                        .setScale(type.getPrecision() - 3, RoundingMode.UNNECESSARY);

                writer.writeTimestamp(Timestamp.forMillis(decimalMillis, 0));
            };
        }
    }

    private static BlockEncoder decimalEncoder(DecimalType type)
    {
        if (type.isShort()) {
            return (writer, block, position) -> {
                writer.writeDecimal(BigDecimal.valueOf(type.getLong(block, position), type.getScale()));
            };
        }
        else {
            return (writer, block, position) -> {
                writer.writeDecimal(new BigDecimal(((Int128) type.getObject(block, position)).toBigInteger(), type.getScale()));
            };
        }
    }

    private static final BlockEncoder byteEncoder = (writer, block, position) ->
            writer.writeInt(TinyintType.TINYINT.getLong(block, position));

    private static final BlockEncoder shortEncoder = (writer, block, position) ->
            writer.writeInt(SmallintType.SMALLINT.getLong(block, position));

    private static final BlockEncoder intEncoder = (writer, block, position) ->
            writer.writeInt(IntegerType.INTEGER.getInt(block, position));

    private static final BlockEncoder stringEncoder = (writer, block, position) ->
            writer.writeString(VarcharType.VARCHAR.getSlice(block, position).toString(StandardCharsets.UTF_8));

    private static final BlockEncoder boolEncoder = (writer, block, position) ->
            writer.writeBool(BooleanType.BOOLEAN.getBoolean(block, position));

    private static final BlockEncoder binaryEncoder = (writer, block, position) ->
            writer.writeBlob(VarbinaryType.VARBINARY.getSlice(block, position).getBytes());

    private static final BlockEncoder longEncoder = (writer, block, position) ->
            writer.writeInt(BigintType.BIGINT.getLong(block, position));

    private static final BlockEncoder realEncoder = (writer, block, position) ->
            writer.writeFloat(RealType.REAL.getFloat(block, position));

    private static final BlockEncoder doubleEncoder = (writer, block, position) ->
            writer.writeFloat(DoubleType.DOUBLE.getDouble(block, position));

    private static final BlockEncoder dateEncoder = (writer, block, position) ->
            writer.writeTimestamp(
                    Timestamp.forDateZ(
                            Date.from(
                                    LocalDate.ofEpochDay(DateType.DATE.getLong(block, position))
                                            .atStartOfDay(ZoneId.of("UTC"))
                                            .toInstant())));
}
