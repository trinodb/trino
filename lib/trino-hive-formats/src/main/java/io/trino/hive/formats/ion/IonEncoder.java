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
import java.util.Optional;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * An IonEncoder encodes Pages of trino data to an IonWriter.
 */
public class IonEncoder
{
    private final RowEncoder rowEncoder;

    public IonEncoder(List<Column> columns)
    {
        rowEncoder = RowEncoder.forFields(columns.stream()
                .map(c -> new RowType.Field(Optional.of(c.name()), c.type()))
                .toList());
    }

    /**
     * Encode the page of data to the IonWriter.
     * <br>
     * IonWriter.flush() will be called after the page is written.
     */
    public void encode(IonWriter writer, Page page)
            throws IOException
    {
        for (int i = 0; i < page.getPositionCount(); i++) {
            rowEncoder.encodeStruct(writer, page::getBlock, i);
        }
        // todo: consider decoupling ion writer flushes from page sizes.
        writer.flush();
    }

    private interface BlockEncoder
    {
        void encode(IonWriter writer, Block block, int position)
                throws IOException;
    }

    private static BlockEncoder encoderForType(Type type)
    {
        BlockEncoder encoder = switch (type) {
            case TinyintType _ -> BYTE_ENCODER;
            case SmallintType _ -> SHORT_ENCODER;
            case IntegerType _ -> INT_ENCODER;
            case BigintType _ -> LONG_ENCODER;
            case BooleanType _ -> BOOL_ENCODER;
            case VarbinaryType _ -> BINARY_ENCODER;
            case RealType _ -> REAL_ENCODER;
            case DoubleType _ -> DOUBLE_ENCODER;
            case VarcharType _, CharType _ -> STRING_ENCODER;
            case DateType _ -> DATE_ENCODER;
            case DecimalType t -> decimalEncoder(t);
            case TimestampType t -> timestampEncoder(t);
            case RowType t -> RowEncoder.forFields(t.getFields());
            case MapType t -> new MapEncoder(t, t.getKeyType(), encoderForType(t.getValueType()));
            case ArrayType t -> new ArrayEncoder(encoderForType(t.getElementType()));
            default -> throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
        };
        return wrapEncoder(encoder);
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
            implements BlockEncoder
    {
        private static RowEncoder forFields(List<RowType.Field> fields)
        {
            ImmutableList.Builder<String> fieldNamesBuilder = ImmutableList.builder();
            ImmutableList.Builder<BlockEncoder> fieldEncodersBuilder = ImmutableList.builder();

            for (RowType.Field field : fields) {
                fieldNamesBuilder.add(field.getName().get());
                fieldEncodersBuilder.add(encoderForType(field.getType()));
            }

            return new RowEncoder(fieldNamesBuilder.build(), fieldEncodersBuilder.build());
        }

        @Override
        public void encode(IonWriter writer, Block block, int position)
                throws IOException
        {
            encodeStruct(writer, ((RowBlock) block)::getFieldBlock, position);
        }

        // used for encoding 'top-level' rows by the IonEncoder
        private void encodeStruct(IonWriter writer, IntFunction<Block> blockSelector, int position)
                throws IOException
        {
            writer.stepIn(IonType.STRUCT);
            for (int i = 0; i < fieldEncoders.size(); i++) {
                // fields are omitted by default, as was true in the hive serde.
                // there is an unimplemented hive legacy property of `ion.serialize_null`
                // that could be used to specify typed or untyped ion nulls instead.
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

    private record MapEncoder(MapType mapType, Type keyType, BlockEncoder encoder)
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

    private static final BlockEncoder BYTE_ENCODER = (writer, block, position) ->
            writer.writeInt(TinyintType.TINYINT.getLong(block, position));

    private static final BlockEncoder SHORT_ENCODER = (writer, block, position) ->
            writer.writeInt(SmallintType.SMALLINT.getLong(block, position));

    private static final BlockEncoder INT_ENCODER = (writer, block, position) ->
            writer.writeInt(IntegerType.INTEGER.getInt(block, position));

    private static final BlockEncoder STRING_ENCODER = (writer, block, position) ->
            writer.writeString(VarcharType.VARCHAR.getSlice(block, position).toString(StandardCharsets.UTF_8));

    private static final BlockEncoder BOOL_ENCODER = (writer, block, position) ->
            writer.writeBool(BooleanType.BOOLEAN.getBoolean(block, position));

    private static final BlockEncoder BINARY_ENCODER = (writer, block, position) ->
            writer.writeBlob(VarbinaryType.VARBINARY.getSlice(block, position).getBytes());

    private static final BlockEncoder LONG_ENCODER = (writer, block, position) ->
            writer.writeInt(BigintType.BIGINT.getLong(block, position));

    private static final BlockEncoder REAL_ENCODER = (writer, block, position) ->
            writer.writeFloat(RealType.REAL.getFloat(block, position));

    private static final BlockEncoder DOUBLE_ENCODER = (writer, block, position) ->
            writer.writeFloat(DoubleType.DOUBLE.getDouble(block, position));

    private static final BlockEncoder DATE_ENCODER = (writer, block, position) ->
            writer.writeTimestamp(
                    Timestamp.forDateZ(
                            Date.from(
                                    LocalDate.ofEpochDay(DateType.DATE.getLong(block, position))
                                            .atStartOfDay(ZoneId.of("UTC"))
                                            .toInstant())));
}
