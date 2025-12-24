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
package io.trino.util.variant;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.variant.VariantEncoder;
import io.trino.type.UnknownType;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BOOLEAN_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_BYTE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DATE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL16_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DECIMAL8_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_DOUBLE_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_FLOAT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_INT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_LONG_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_NULL_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_SHORT_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIMESTAMP_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_TIME_SIZE;
import static io.trino.spi.variant.VariantEncoder.ENCODED_UUID_SIZE;
import static io.trino.spi.variant.VariantEncoder.encodeBinary;
import static io.trino.spi.variant.VariantEncoder.encodeBoolean;
import static io.trino.spi.variant.VariantEncoder.encodeByte;
import static io.trino.spi.variant.VariantEncoder.encodeDate;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal16;
import static io.trino.spi.variant.VariantEncoder.encodeDecimal8;
import static io.trino.spi.variant.VariantEncoder.encodeDouble;
import static io.trino.spi.variant.VariantEncoder.encodeFloat;
import static io.trino.spi.variant.VariantEncoder.encodeInt;
import static io.trino.spi.variant.VariantEncoder.encodeLong;
import static io.trino.spi.variant.VariantEncoder.encodeNull;
import static io.trino.spi.variant.VariantEncoder.encodeShort;
import static io.trino.spi.variant.VariantEncoder.encodeString;
import static io.trino.spi.variant.VariantEncoder.encodeUuid;
import static io.trino.spi.variant.VariantEncoder.encodedBinarySize;
import static io.trino.spi.variant.VariantEncoder.encodedStringSize;
import static java.lang.Math.multiplyExact;

/// Encodes a primitive value directly from a block into a variant Slice.
/// This does not extend VariantWriter to avoid the required creation of
/// PlannedValue objects that would be required to retain the Block and position.
/// Because of this efficiency advantage, this class should be used in preference
/// to VariantWriter wherever possible.
abstract class PrimitiveVariantEncoder
{
    static Optional<PrimitiveVariantEncoder> create(Type type)
    {
        return Optional.ofNullable(switch (type) {
            case UnknownType _ -> new UnknownVariantEncoder();
            case BooleanType _ -> new BooleanVariantEncoder();
            case TinyintType _ -> new TinyintVariantEncoder();
            case SmallintType _ -> new SmallintVariantEncoder();
            case IntegerType _ -> new IntegerVariantEncoder();
            case BigintType _ -> new BigintVariantEncoder();
            case DecimalType t -> t.isShort() ? new ShortDecimalVariantEncoder(t) : new LongDecimalVariantEncoder(t);
            case RealType _ -> new FloatVariantEncoder();
            case DoubleType _ -> new DoubleVariantEncoder();
            case DateType _ -> new DateVariantEncoder();
            case TimeType _ -> new TimeVariantEncoder();
            case TimestampType t -> t.isShort() ? new ShortTimestampVariantEncoder() : new LongTimestampVariantEncoder(t);
            case TimestampWithTimeZoneType t -> t.isShort() ? new ShortTimestampWithTimezoneVariantEncoder() : new LongTimestampWithTimezoneVariantEncoder(t);
            case UuidType _ -> new UuidVariantEncoder();
            case VarcharType _ -> new VarcharVariantEncoder();
            case VarbinaryType _ -> new VarbinaryVariantEncoder();
            default -> null;
        });
    }

    abstract int size(Block block, int position);

    public final int write(Block block, int position, Slice out, int offset)
    {
        if (block.isNull(position)) {
            return encodeNull(out, offset);
        }
        return writeNonNull(block, position, out, offset);
    }

    abstract int writeNonNull(Block block, int position, Slice out, int offset);

    private static final class UnknownVariantEncoder
            extends PrimitiveVariantEncoder
    {
        @Override
        public int size(Block block, int position)
        {
            return ENCODED_NULL_SIZE;
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeNull(out, offset);
        }
    }

    private abstract static class FixedPrimitiveVariantEncoder
            extends PrimitiveVariantEncoder
    {
        private final int encodedSize;

        FixedPrimitiveVariantEncoder(int encodedSize)
        {
            this.encodedSize = encodedSize;
        }

        @Override
        public final int size(Block block, int position)
        {
            if (block.isNull(position)) {
                return ENCODED_NULL_SIZE;
            }
            return encodedSize;
        }
    }

    private static class BooleanVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private BooleanVariantEncoder()
        {
            super(ENCODED_BOOLEAN_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeBoolean(BOOLEAN.getBoolean(block, position), out, offset);
        }
    }

    private static class TinyintVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private TinyintVariantEncoder()
        {
            super(ENCODED_BYTE_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeByte(TINYINT.getByte(block, position), out, offset);
        }
    }

    private static class SmallintVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private SmallintVariantEncoder()
        {
            super(ENCODED_SHORT_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeShort(SMALLINT.getShort(block, position), out, offset);
        }
    }

    private static class IntegerVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private IntegerVariantEncoder()
        {
            super(ENCODED_INT_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeInt(IntegerType.INTEGER.getInt(block, position), out, offset);
        }
    }

    private static class BigintVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private BigintVariantEncoder()
        {
            super(ENCODED_LONG_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeLong(BIGINT.getLong(block, position), out, offset);
        }
    }

    private static class ShortDecimalVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private final DecimalType type;

        private ShortDecimalVariantEncoder(DecimalType type)
        {
            super(ENCODED_DECIMAL8_SIZE);
            this.type = type;
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeDecimal8(type.getLong(block, position), type.getScale(), out, offset);
        }
    }

    private static class LongDecimalVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private final DecimalType type;

        private LongDecimalVariantEncoder(DecimalType type)
        {
            super(ENCODED_DECIMAL16_SIZE);
            this.type = type;
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeDecimal16((Int128) type.getObject(block, position), type.getScale(), out, offset);
        }
    }

    private static class FloatVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private FloatVariantEncoder()
        {
            super(ENCODED_FLOAT_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeFloat(REAL.getFloat(block, position), out, offset);
        }
    }

    private static class DoubleVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private DoubleVariantEncoder()
        {
            super(ENCODED_DOUBLE_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeDouble(DOUBLE.getDouble(block, position), out, offset);
        }
    }

    private static class DateVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private DateVariantEncoder()
        {
            super(ENCODED_DATE_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeDate(DateType.DATE.getInt(block, position), out, offset);
        }
    }

    private static class TimeVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private TimeVariantEncoder()
        {
            super(ENCODED_TIME_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            long epochPicos = BIGINT.getLong(block, position);
            long micros = epochPicos / 1_000_000L;
            return VariantEncoder.encodeTimeMicrosNtz(micros, out, offset);
        }
    }

    private static class ShortTimestampVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private ShortTimestampVariantEncoder()
        {
            super(ENCODED_TIMESTAMP_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            long epochMicros = BIGINT.getLong(block, position);
            long nanos = multiplyExact(epochMicros, 1_000L);
            return VariantEncoder.encodeTimestampNanosNtz(nanos, out, offset);
        }
    }

    private static class LongTimestampVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private final TimestampType type;

        private LongTimestampVariantEncoder(TimestampType type)
        {
            super(ENCODED_TIMESTAMP_SIZE);
            this.type = type;
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            LongTimestamp timestamp = (LongTimestamp) type.getObject(block, position);
            long nanosFromMicros = multiplyExact(timestamp.getEpochMicros(), 1_000L);
            long extraNanos = timestamp.getPicosOfMicro() / 1_000; // 1000 ps = 1 ns
            long nanos = Math.addExact(nanosFromMicros, extraNanos);
            return VariantEncoder.encodeTimestampNanosNtz(nanos, out, offset);
        }
    }

    private static class ShortTimestampWithTimezoneVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private ShortTimestampWithTimezoneVariantEncoder()
        {
            super(ENCODED_TIMESTAMP_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            long packedEpochMillis = BIGINT.getLong(block, position);
            long epochMillis = unpackMillisUtc(packedEpochMillis);
            long epochMicros = multiplyExact(epochMillis, 1_000L);
            return VariantEncoder.encodeTimestampMicrosUtc(epochMicros, out, offset);
        }
    }

    private static class LongTimestampWithTimezoneVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private final TimestampWithTimeZoneType type;

        private LongTimestampWithTimezoneVariantEncoder(TimestampWithTimeZoneType type)
        {
            super(ENCODED_TIMESTAMP_SIZE);
            this.type = type;
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) type.getObject(block, position);

            if (type.getPrecision() <= 6) {
                long millisFromMillis = multiplyExact(timestamp.getEpochMillis(), 1000L);
                int extraMillis = timestamp.getPicosOfMilli() / 1_000_000;
                long epochMicros = Math.addExact(millisFromMillis, extraMillis);
                return VariantEncoder.encodeTimestampMicrosUtc(epochMicros, out, offset);
            }

            long nanosFromMillis = multiplyExact(timestamp.getEpochMillis(), 1_000_000L);
            int extraNanos = timestamp.getPicosOfMilli() / 1_000;
            long nanos = Math.addExact(nanosFromMillis, extraNanos);
            return VariantEncoder.encodeTimestampNanosUtc(nanos, out, offset);
        }
    }

    private static class UuidVariantEncoder
            extends FixedPrimitiveVariantEncoder
    {
        private UuidVariantEncoder()
        {
            super(ENCODED_UUID_SIZE);
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeUuid(UUID.getSlice(block, position), out, offset);
        }
    }

    private static final class VarcharVariantEncoder
            extends PrimitiveVariantEncoder
    {
        @Override
        public int size(Block block, int position)
        {
            if (block.isNull(position)) {
                return ENCODED_NULL_SIZE;
            }
            return encodedStringSize(getSliceLength(block, position));
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeString(getSlice(block, position), out, offset);
        }
    }

    private static final class VarbinaryVariantEncoder
            extends PrimitiveVariantEncoder
    {
        @Override
        public int size(Block block, int position)
        {
            if (block.isNull(position)) {
                return ENCODED_NULL_SIZE;
            }
            return encodedBinarySize(getSliceLength(block, position));
        }

        @Override
        public int writeNonNull(Block block, int position, Slice out, int offset)
        {
            return encodeBinary(getSlice(block, position), out, offset);
        }
    }

    private static Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    private static int getSliceLength(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSliceLength(valuePosition);
    }
}
