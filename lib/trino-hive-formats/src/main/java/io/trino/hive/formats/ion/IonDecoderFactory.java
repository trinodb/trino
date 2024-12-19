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

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.hive.formats.DistinctMapKeys;
import io.trino.hive.formats.line.Column;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Timestamps;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.Varchars;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;

public class IonDecoderFactory
{
    private IonDecoderFactory() {}

    /**
     * Builds a decoder for the given columns.
     * <p>
     * The decoder expects to decode the _current_ Ion Value.
     * It also expects that the calling code will manage the PageBuilder.
     * <p>
     *
     * @param strictPathing controls behavior when encountering mistyped
     * values during path extraction. That is outside (before), the trino
     * type model. The ion-hive-serde used path extraction for navigating
     * the top-level-values even if no path extractions were configured.
     * So, in absence of support for path extraction configurations this
     * still affects the handling of mistyped top-level-values.
     * todo: revisit the above once path extraction config is supported.
     */
    public static IonDecoder buildDecoder(List<Column> columns, boolean strictPathing)
    {
        RowDecoder rowDecoder = RowDecoder.forFields(
                columns.stream()
                        .map(c -> new RowType.Field(Optional.of(c.name()), c.type()))
                        .toList());

        return (ionReader, pageBuilder) -> {
            IonType ionType = ionReader.getType();
            IntFunction<BlockBuilder> blockSelector = pageBuilder::getBlockBuilder;

            if (ionType == IonType.STRUCT && !ionReader.isNullValue()) {
                rowDecoder.decode(ionReader, blockSelector);
            }
            else if (ionType == IonType.STRUCT || ionType == IonType.NULL || !strictPathing) {
                rowDecoder.appendNulls(blockSelector);
            }
            else {
                throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR,
                        "Top-level-value of IonType %s is not valid with strict typing.".formatted(ionType));
            }
        };
    }

    private interface BlockDecoder
    {
        void decode(IonReader reader, BlockBuilder builder);
    }

    private static BlockDecoder decoderForType(Type type)
    {
        return switch (type) {
            case TinyintType t -> wrapDecoder(byteDecoder, t, IonType.INT);
            case SmallintType t -> wrapDecoder(shortDecoder, t, IonType.INT);
            case IntegerType t -> wrapDecoder(intDecoder, t, IonType.INT);
            case BigintType t -> wrapDecoder(longDecoder, t, IonType.INT);
            case RealType t -> wrapDecoder(realDecoder, t, IonType.FLOAT);
            case DoubleType t -> wrapDecoder(floatDecoder, t, IonType.FLOAT);
            case DecimalType t -> wrapDecoder(decimalDecoder(t), t, IonType.DECIMAL, IonType.INT);
            case BooleanType t -> wrapDecoder(boolDecoder, t, IonType.BOOL);
            case DateType t -> wrapDecoder(dateDecoder, t, IonType.TIMESTAMP);
            case TimestampType t -> wrapDecoder(timestampDecoder(t), t, IonType.TIMESTAMP);
            case VarcharType t -> wrapDecoder(varcharDecoder(t), t, IonType.STRING, IonType.SYMBOL);
            case CharType t -> wrapDecoder(charDecoder(t), t, IonType.STRING, IonType.SYMBOL);
            case VarbinaryType t -> wrapDecoder(binaryDecoder, t, IonType.BLOB, IonType.CLOB);
            case RowType t -> wrapDecoder(RowDecoder.forFields(t.getFields()), t, IonType.STRUCT);
            case ArrayType t -> wrapDecoder(new ArrayDecoder(decoderForType(t.getElementType())), t, IonType.LIST, IonType.SEXP);
            case MapType t -> wrapDecoder(new MapDecoder(t, decoderForType(t.getValueType())), t, IonType.STRUCT);
            default -> throw new IllegalArgumentException(String.format("Unsupported type: %s", type));
        };
    }

    /**
     * Wraps decoders for common handling logic.
     * <p>
     * Handles un-typed and correctly typed null values.
     * Throws for mistyped values, whether null or not.
     * Delegates to Decoder for correctly-typed, non-null values.
     * <p>
     * This code treats all values as nullable.
     */
    private static BlockDecoder wrapDecoder(BlockDecoder decoder, Type trinoType, IonType... allowedTypes)
    {
        Set<IonType> allowedWithNull = new HashSet<>(Arrays.asList(allowedTypes));
        allowedWithNull.add(IonType.NULL);

        return (reader, builder) -> {
            final IonType ionType = reader.getType();
            if (!allowedWithNull.contains(ionType)) {
                throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR,
                        "Cannot coerce IonType %s to Trino type %s".formatted(ionType, trinoType));
            }
            if (reader.isNullValue()) {
                builder.appendNull();
            }
            else {
                decoder.decode(reader, builder);
            }
        };
    }

    /**
     * The RowDecoder is used as the BlockDecoder for nested RowTypes and is used for decoding
     * top-level structs into pages.
     */
    private record RowDecoder(Map<String, Integer> fieldPositions, List<BlockDecoder> fieldDecoders)
            implements BlockDecoder
    {
        private static RowDecoder forFields(List<RowType.Field> fields)
        {
            ImmutableList.Builder<BlockDecoder> decoderBuilder = ImmutableList.builder();
            ImmutableMap.Builder<String, Integer> fieldPositionBuilder = ImmutableMap.builder();
            for (int pos = 0; pos < fields.size(); pos++) {
                RowType.Field field = fields.get(pos);
                decoderBuilder.add(decoderForType(field.getType()));
                fieldPositionBuilder.put(field.getName().get().toLowerCase(Locale.ROOT), pos);
            }
            return new RowDecoder(fieldPositionBuilder.buildOrThrow(), decoderBuilder.build());
        }

        @Override
        public void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((RowBlockBuilder) blockBuilder)
                    .buildEntry(fieldBuilders -> decode(ionReader, fieldBuilders::get));
        }

        // assumes that the reader is positioned on a non-null struct value
        private void decode(IonReader ionReader, IntFunction<BlockBuilder> blockSelector)
        {
            boolean[] encountered = new boolean[fieldDecoders.size()];
            ionReader.stepIn();

            while (ionReader.next() != null) {
                final Integer fieldIndex = fieldPositions.get(ionReader.getFieldName().toLowerCase(Locale.ROOT));
                if (fieldIndex == null) {
                    continue;
                }
                final BlockBuilder blockBuilder = blockSelector.apply(fieldIndex);
                if (encountered[fieldIndex]) {
                    blockBuilder.resetTo(blockBuilder.getPositionCount() - 1);
                }
                else {
                    encountered[fieldIndex] = true;
                }
                fieldDecoders.get(fieldIndex).decode(ionReader, blockBuilder);
            }

            for (int i = 0; i < encountered.length; i++) {
                if (!encountered[i]) {
                    blockSelector.apply(i).appendNull();
                }
            }

            ionReader.stepOut();
        }

        private void appendNulls(IntFunction<BlockBuilder> blockSelector)
        {
            for (int i = 0; i < fieldDecoders.size(); i++) {
                blockSelector.apply(i).appendNull();
            }
        }
    }

    private static class MapDecoder
            implements BlockDecoder
    {
        private final BlockDecoder valueDecoder;
        private final Type keyType;
        private final Type valueType;
        private final DistinctMapKeys distinctMapKeys;
        private BlockBuilder keyBlockBuilder;
        private BlockBuilder valueBlockBuilder;

        public MapDecoder(MapType mapType, BlockDecoder valueDecoder)
        {
            this.keyType = mapType.getKeyType();
            if (!(keyType instanceof VarcharType _ || keyType instanceof CharType _)) {
                throw new UnsupportedOperationException("Unsupported map key type: " + keyType);
            }
            this.valueType = mapType.getValueType();
            this.valueDecoder = valueDecoder;
            this.distinctMapKeys = new DistinctMapKeys(mapType, true);
            this.keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, 128);
            this.valueBlockBuilder = mapType.getValueType().createBlockBuilder(null, 128);
        }

        @Override
        public void decode(IonReader ionReader, BlockBuilder builder)
        {
            ionReader.stepIn();
            // buffer the keys and values
            while (ionReader.next() != null) {
                VarcharType.VARCHAR.writeSlice(keyBlockBuilder, Slices.utf8Slice(ionReader.getFieldName()));
                valueDecoder.decode(ionReader, valueBlockBuilder);
            }
            ValueBlock keys = keyBlockBuilder.buildValueBlock();
            ValueBlock values = valueBlockBuilder.buildValueBlock();
            keyBlockBuilder = keyType.createBlockBuilder(null, keys.getPositionCount());
            valueBlockBuilder = valueType.createBlockBuilder(null, values.getPositionCount());

            // copy the distinct key entries to the output
            boolean[] distinctKeys = distinctMapKeys.selectDistinctKeys(keys);

            ((MapBlockBuilder) builder).buildEntry((keyBuilder, valueBuilder) -> {
                for (int index = 0; index < distinctKeys.length; index++) {
                    boolean distinctKey = distinctKeys[index];
                    if (distinctKey) {
                        keyBuilder.append(keys, index);
                        valueBuilder.append(values, index);
                    }
                }
            });
            ionReader.stepOut();
        }
    }

    private record ArrayDecoder(BlockDecoder elementDecoder)
            implements BlockDecoder
    {
        @Override
        public void decode(IonReader ionReader, BlockBuilder blockBuilder)
        {
            ((ArrayBlockBuilder) blockBuilder)
                    .buildEntry(elementBuilder -> {
                        ionReader.stepIn();
                        while (ionReader.next() != null) {
                            elementDecoder.decode(ionReader, elementBuilder);
                        }
                        ionReader.stepOut();
                    });
        }
    }

    private static BlockDecoder timestampDecoder(TimestampType type)
    {
        // Ion supports arbitrarily precise Timestamps.
        // Other Hive formats are using the DecodedTimestamp and TrinoTimestampEncoders in
        // io.trino.plugin.base.type but those don't cover picos.
        // This code uses same pattern of splitting the parsed timestamp into (seconds, fraction)
        // then rounding the fraction using Timestamps.round() ensures consistency with the others
        // while capturing picos if present. Fractional precision beyond picos is ignored.
        return (reader, builder) -> {
            BigDecimal decimalSeconds = reader.timestampValue()
                    .getDecimalMillis()
                    .movePointLeft(3);
            BigDecimal decimalPicos = decimalSeconds.remainder(BigDecimal.ONE)
                    .movePointRight(12);

            long fractionalPicos = Timestamps.round(decimalPicos.longValue(), 12 - type.getPrecision());
            long epochMicros = decimalSeconds.longValue() * Timestamps.MICROSECONDS_PER_SECOND
                    + fractionalPicos / Timestamps.PICOSECONDS_PER_MICROSECOND;

            if (type.isShort()) {
                type.writeLong(builder, epochMicros);
            }
            else {
                type.writeObject(builder,
                        new LongTimestamp(epochMicros, (int) (fractionalPicos % Timestamps.PICOSECONDS_PER_MICROSECOND)));
            }
        };
    }

    private static BlockDecoder decimalDecoder(DecimalType type)
    {
        int precision = type.getPrecision();
        int scale = type.getScale();

        return (reader, builder) -> {
            try {
                BigDecimal decimal = reader.bigDecimalValue();
                BigInteger unscaled = decimal
                        .setScale(scale, RoundingMode.UNNECESSARY)
                        .unscaledValue();

                if (Decimals.overflows(unscaled, precision)) {
                    throw new TrinoException(StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE,
                            "Decimal value %s does not fit %d digits of precision and %d of scale!"
                                    .formatted(decimal, precision, scale));
                }
                if (type.isShort()) {
                    type.writeLong(builder, unscaled.longValue());
                }
                else {
                    type.writeObject(builder, Int128.valueOf(unscaled));
                }
            }
            catch (ArithmeticException e) {
                throw new TrinoException(StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE,
                        "Decimal value %s does not fit %d digits of scale!".formatted(reader.bigDecimalValue(), scale));
            }
        };
    }

    private static BlockDecoder varcharDecoder(VarcharType type)
    {
        return (ionReader, blockBuilder) ->
                type.writeSlice(blockBuilder, Varchars.truncateToLength(Slices.utf8Slice(ionReader.stringValue()), type));
    }

    private static BlockDecoder charDecoder(CharType type)
    {
        return (ionReader, blockBuilder) ->
                type.writeSlice(blockBuilder, Chars.truncateToLengthAndTrimSpaces(Slices.utf8Slice(ionReader.stringValue()), type));
    }

    private static final BlockDecoder byteDecoder = (ionReader, blockBuilder) ->
            TinyintType.TINYINT.writeLong(blockBuilder, readLong(ionReader));

    private static final BlockDecoder shortDecoder = (ionReader, blockBuilder) ->
            SmallintType.SMALLINT.writeLong(blockBuilder, readLong(ionReader));

    private static final BlockDecoder intDecoder = (ionReader, blockBuilder) ->
            IntegerType.INTEGER.writeLong(blockBuilder, readLong(ionReader));

    private static final BlockDecoder longDecoder = (ionReader, blockBuilder) ->
            BigintType.BIGINT.writeLong(blockBuilder, readLong(ionReader));

    private static long readLong(IonReader ionReader)
    {
        try {
            return ionReader.longValue();
        }
        catch (IonException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR, e.getMessage());
        }
    }

    private static final BlockDecoder realDecoder = (ionReader, blockBuilder) -> {
        double readValue = ionReader.doubleValue();
        if (readValue == (float) readValue) {
            RealType.REAL.writeFloat(blockBuilder, (float) readValue);
        }
        else {
            throw new TrinoException(StandardErrorCode.GENERIC_USER_ERROR,
                    "Won't truncate double precise float to real!");
        }
    };

    private static final BlockDecoder floatDecoder = (ionReader, blockBuilder) ->
            DoubleType.DOUBLE.writeDouble(blockBuilder, ionReader.doubleValue());

    private static final BlockDecoder boolDecoder = (ionReader, blockBuilder) ->
            BooleanType.BOOLEAN.writeBoolean(blockBuilder, ionReader.booleanValue());

    private static final BlockDecoder dateDecoder = (ionReader, blockBuilder) -> {
        Timestamp ionTs = ionReader.timestampValue();
        LocalDate localDate = LocalDate.of(ionTs.getZYear(), ionTs.getZMonth(), ionTs.getZDay());
        DateType.DATE.writeLong(blockBuilder, localDate.toEpochDay());
    };

    private static final BlockDecoder binaryDecoder = (ionReader, blockBuilder) ->
            VarbinaryType.VARBINARY.writeSlice(blockBuilder, Slices.wrappedBuffer(ionReader.newBytes()));
}
