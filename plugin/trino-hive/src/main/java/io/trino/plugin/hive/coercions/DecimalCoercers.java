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
import io.trino.spi.type.VarcharType;

import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
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
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Math.min;
import static java.lang.String.format;

public final class DecimalCoercers
{
    private DecimalCoercers() {}

    public static Function<Block, Block> createDecimalToDecimalCoercer(DecimalType fromType, DecimalType toType)
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

    public static Function<Block, Block> createDecimalToDoubleCoercer(DecimalType fromType)
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

    public static Function<Block, Block> createDecimalToRealCoercer(DecimalType fromType)
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

    public static Function<Block, Block> createDecimalToVarcharCoercer(DecimalType fromType, VarcharType toType)
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

    public static Function<Block, Block> createDoubleToDecimalCoercer(DecimalType toType)
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

    public static Function<Block, Block> createRealToDecimalCoercer(DecimalType toType)
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
}
