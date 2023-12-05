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

package io.trino.plugin.hive.coercions;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalConversions.doubleToLongDecimal;
import static io.trino.spi.type.DecimalConversions.doubleToShortDecimal;
import static io.trino.spi.type.DecimalConversions.longDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.longDecimalToReal;
import static io.trino.spi.type.DecimalConversions.longToLongCast;
import static io.trino.spi.type.DecimalConversions.longToShortCast;
import static io.trino.spi.type.DecimalConversions.realToLongDecimal;
import static io.trino.spi.type.DecimalConversions.realToShortDecimal;
import static io.trino.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.trino.spi.type.DecimalConversions.shortDecimalToReal;
import static io.trino.spi.type.DecimalConversions.shortToLongCast;
import static io.trino.spi.type.DecimalConversions.shortToShortCast;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.Decimals.overflows;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Math.min;
import static java.lang.String.format;

public final class DecimalCoercers
{
    private DecimalCoercers() {}

    public static TypeCoercer<DecimalType, DecimalType> createDecimalToDecimalCoercer(DecimalType fromType, DecimalType toType)
    {
        if (fromType.isShort()) {
            if (toType.isShort()) {
                return new ShortDecimalToShortDecimalCoercer(fromType, toType);
            }
            return new ShortDecimalToLongDecimalCoercer(fromType, toType);
        }
        if (toType.isShort()) {
            return new LongDecimalToShortDecimalCoercer(fromType, toType);
        }
        return new LongDecimalToLongDecimalCoercer(fromType, toType);
    }

    private static class ShortDecimalToShortDecimalCoercer
            extends TypeCoercer<DecimalType, DecimalType>
    {
        private final long rescale;

        public ShortDecimalToShortDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
            rescale = longTenToNth(Math.abs(toType.getScale() - fromType.getScale()));
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            long returnValue = shortToShortCast(fromType.getLong(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale(),
                    rescale,
                    rescale / 2);
            toType.writeLong(blockBuilder, returnValue);
        }
    }

    private static class ShortDecimalToLongDecimalCoercer
            extends TypeCoercer<DecimalType, DecimalType>
    {
        public ShortDecimalToLongDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            Int128 coercedValue = shortToLongCast(fromType.getLong(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale());
            toType.writeObject(blockBuilder, coercedValue);
        }
    }

    private static class LongDecimalToShortDecimalCoercer
            extends TypeCoercer<DecimalType, DecimalType>
    {
        public LongDecimalToShortDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            long returnValue = longToShortCast((Int128) fromType.getObject(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale());
            toType.writeLong(blockBuilder, returnValue);
        }
    }

    private static class LongDecimalToLongDecimalCoercer
            extends TypeCoercer<DecimalType, DecimalType>
    {
        public LongDecimalToLongDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            Int128 coercedValue = longToLongCast((Int128) fromType.getObject(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale());
            toType.writeObject(blockBuilder, coercedValue);
        }
    }

    public static TypeCoercer<DecimalType, DoubleType> createDecimalToDoubleCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToDoubleCoercer(fromType);
        }
        return new LongDecimalToDoubleCoercer(fromType);
    }

    private static class ShortDecimalToDoubleCoercer
            extends TypeCoercer<DecimalType, DoubleType>
    {
        private final long rescale;

        public ShortDecimalToDoubleCoercer(DecimalType fromType)
        {
            super(fromType, DOUBLE);
            rescale = longTenToNth(fromType.getScale());
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeDouble(blockBuilder,
                    shortDecimalToDouble(fromType.getLong(block, position), rescale));
        }
    }

    private static class LongDecimalToDoubleCoercer
            extends TypeCoercer<DecimalType, DoubleType>
    {
        public LongDecimalToDoubleCoercer(DecimalType fromType)
        {
            super(fromType, DOUBLE);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeDouble(blockBuilder,
                    longDecimalToDouble((Int128) fromType.getObject(block, position), fromType.getScale()));
        }
    }

    public static TypeCoercer<DecimalType, RealType> createDecimalToRealCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToRealCoercer(fromType);
        }
        return new LongDecimalToRealCoercer(fromType);
    }

    private static class ShortDecimalToRealCoercer
            extends TypeCoercer<DecimalType, RealType>
    {
        private final long rescale;

        public ShortDecimalToRealCoercer(DecimalType fromType)
        {
            super(fromType, REAL);
            rescale = longTenToNth(fromType.getScale());
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    shortDecimalToReal(fromType.getLong(block, position), rescale));
        }
    }

    private static class LongDecimalToRealCoercer
            extends TypeCoercer<DecimalType, RealType>
    {
        public LongDecimalToRealCoercer(DecimalType fromType)
        {
            super(fromType, REAL);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    longDecimalToReal((Int128) fromType.getObject(block, position), fromType.getScale()));
        }
    }

    public static TypeCoercer<DecimalType, VarcharType> createDecimalToVarcharCoercer(DecimalType fromType, VarcharType toType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToVarcharCoercer(fromType, toType);
        }
        return new LongDecimalToVarcharCoercer(fromType, toType);
    }

    private static class ShortDecimalToVarcharCoercer
            extends TypeCoercer<DecimalType, VarcharType>
    {
        private final int lengthLimit;

        protected ShortDecimalToVarcharCoercer(DecimalType fromType, VarcharType toType)
        {
            super(fromType, toType);
            this.lengthLimit = toType.getLength().orElse(Integer.MAX_VALUE);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            String stringValue = Decimals.toString(fromType.getLong(block, position), fromType.getScale());
            // Hive truncates digits (also before the decimal point), which can be perceived as a bug
            if (stringValue.length() > lengthLimit) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Decimal value %s representation exceeds varchar(%s) bounds", stringValue, lengthLimit));
            }
            toType.writeString(blockBuilder, stringValue.substring(0, min(lengthLimit, stringValue.length())));
        }
    }

    private static class LongDecimalToVarcharCoercer
            extends TypeCoercer<DecimalType, VarcharType>
    {
        private final int lengthLimit;

        protected LongDecimalToVarcharCoercer(DecimalType fromType, VarcharType toType)
        {
            super(fromType, toType);
            this.lengthLimit = toType.getLength().orElse(Integer.MAX_VALUE);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            String stringValue = Decimals.toString((Int128) fromType.getObject(block, position), fromType.getScale());
            // Hive truncates digits (also before the decimal point), which can be perceived as a bug
            if (stringValue.length() > lengthLimit) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Decimal value %s representation exceeds varchar(%s) bounds", stringValue, lengthLimit));
            }
            toType.writeString(blockBuilder, stringValue.substring(0, min(lengthLimit, stringValue.length())));
        }
    }

    public static <T extends Type> TypeCoercer<DecimalType, T> createDecimalToInteger(DecimalType fromType, T toType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToIntegerCoercer<>(fromType, toType);
        }
        return new LongDecimalToIntegerCoercer<T>(fromType, toType);
    }

    private abstract static class AbstractDecimalToIntegerNumberCoercer<T extends Type>
            extends TypeCoercer<DecimalType, T>
    {
        protected final long minValue;
        protected final long maxValue;

        public AbstractDecimalToIntegerNumberCoercer(DecimalType fromType, T toType)
        {
            super(fromType, toType);

            if (toType.equals(TINYINT)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (toType.equals(SMALLINT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (toType.equals(INTEGER)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (toType.equals(BIGINT)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format("Could not create Coercer from Decimal to %s", toType));
            }
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            String stringValue = getStringValue(block, position);
            int dotPosition = stringValue.indexOf(".");
            long longValue;
            try {
                longValue = Long.parseLong(stringValue.substring(0, dotPosition > 0 ? dotPosition : stringValue.length()));
            }
            catch (NumberFormatException e) {
                blockBuilder.appendNull();
                return;
            }
            // Hive truncates digits (also before the decimal point), which can be perceived as a bug
            if (longValue < minValue || longValue > maxValue) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeLong(blockBuilder, longValue);
            }
        }

        protected abstract String getStringValue(Block block, int position);
    }

    private static class LongDecimalToIntegerCoercer<T extends Type>
            extends AbstractDecimalToIntegerNumberCoercer<T>
    {
        public LongDecimalToIntegerCoercer(DecimalType fromType, T toType)
        {
            super(fromType, toType);
        }

        @Override
        protected String getStringValue(Block block, int position)
        {
            return Decimals.toString((Int128) fromType.getObject(block, position), fromType.getScale());
        }
    }

    private static class ShortDecimalToIntegerCoercer<T extends Type>
            extends AbstractDecimalToIntegerNumberCoercer<T>
    {
        public ShortDecimalToIntegerCoercer(DecimalType fromType, T toType)
        {
            super(fromType, toType);
        }

        @Override
        protected String getStringValue(Block block, int position)
        {
            return Decimals.toString(fromType.getLong(block, position), fromType.getScale());
        }
    }

    public static TypeCoercer<DoubleType, DecimalType> createDoubleToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new DoubleToShortDecimalCoercer(toType);
        }
        return new DoubleToLongDecimalCoercer(toType);
    }

    private static class DoubleToShortDecimalCoercer
            extends TypeCoercer<DoubleType, DecimalType>
    {
        public DoubleToShortDecimalCoercer(DecimalType toType)
        {
            super(DOUBLE, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    doubleToShortDecimal(fromType.getDouble(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    private static class DoubleToLongDecimalCoercer
            extends TypeCoercer<DoubleType, DecimalType>
    {
        public DoubleToLongDecimalCoercer(DecimalType toType)
        {
            super(DOUBLE, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeObject(blockBuilder,
                    doubleToLongDecimal(fromType.getDouble(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    public static TypeCoercer<RealType, DecimalType> createRealToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new RealToShortDecimalCoercer(toType);
        }
        return new RealToLongDecimalCoercer(toType);
    }

    private static class RealToShortDecimalCoercer
            extends TypeCoercer<RealType, DecimalType>
    {
        public RealToShortDecimalCoercer(DecimalType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeLong(blockBuilder,
                    realToShortDecimal(fromType.getFloat(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    private static class RealToLongDecimalCoercer
            extends TypeCoercer<RealType, DecimalType>
    {
        public RealToLongDecimalCoercer(DecimalType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeObject(blockBuilder,
                    realToLongDecimal(fromType.getFloat(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    public static <F extends Type> TypeCoercer<F, DecimalType> createIntegerNumberToDecimalCoercer(F fromType, DecimalType toType)
    {
        if (toType.isShort()) {
            return new IntegerNumberToShortDecimalCoercer<>(fromType, toType);
        }
        return new IntegerNumberToLongDecimalCoercer<>(fromType, toType);
    }

    private static class IntegerNumberToShortDecimalCoercer<F extends Type>
            extends TypeCoercer<F, DecimalType>
    {
        public IntegerNumberToShortDecimalCoercer(F fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            BigDecimal bigDecimal = BigDecimal.valueOf(fromType.getLong(block, position)).setScale(toType.getScale());
            if (overflows(bigDecimal, toType.getPrecision())) {
                blockBuilder.appendNull();
            }
            else {
                writeShortDecimal(blockBuilder, bigDecimal.unscaledValue().longValueExact());
            }
        }
    }

    private static class IntegerNumberToLongDecimalCoercer<F extends Type>
            extends TypeCoercer<F, DecimalType>
    {
        public IntegerNumberToLongDecimalCoercer(F fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            BigDecimal bigDecimal = BigDecimal.valueOf(fromType.getLong(block, position)).setScale(toType.getScale());
            if (overflows(bigDecimal, toType.getPrecision())) {
                blockBuilder.appendNull();
            }
            else {
                writeBigDecimal(toType, blockBuilder, bigDecimal);
            }
        }
    }
}
