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

package io.prestosql.plugin.hive.coercions;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;

import java.math.BigInteger;
import java.util.function.Function;

import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.spi.type.Decimals.overflows;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.overflows;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static java.lang.String.format;

public class DecimalCoercers
{
    private DecimalCoercers()
    {
    }

    public static Function<Block, Block> createDecimalToDecimalCoercer(DecimalType fromType, DecimalType toType)
    {
        if (fromType.isShort()) {
            if (toType.isShort()) {
                return new ShortDecimalToShortDecimalCoercer(fromType, toType);
            }
            else {
                return new ShortDecimalToLongDecimalCoercer(fromType, toType);
            }
        }
        else {
            if (toType.isShort()) {
                return new LongDecimalToShortDecimalCoercer(fromType, toType);
            }
            else {
                return new LongDecimalToLongDecimalCoercer(fromType, toType);
            }
        }
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
            Slice coercedValue = shortToLongCast(fromType.getLong(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale());
            toType.writeSlice(blockBuilder, coercedValue);
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
            long returnValue = longToShortCast(fromType.getSlice(block, position),
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
            Slice coercedValue = longToLongCast(fromType.getSlice(block, position),
                    fromType.getPrecision(),
                    fromType.getScale(),
                    toType.getPrecision(),
                    toType.getScale());
            toType.writeSlice(blockBuilder, coercedValue);
        }
    }

    private static long shortToShortCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale,
            long scalingFactor,
            long halfOfScalingFactor)
    {
        long returnValue;
        if (resultScale >= sourceScale) {
            returnValue = value * scalingFactor;
        }
        else {
            returnValue = value / scalingFactor;
            if (value >= 0) {
                if (value % scalingFactor >= halfOfScalingFactor) {
                    returnValue++;
                }
            }
            else {
                if (value % scalingFactor <= -halfOfScalingFactor) {
                    returnValue--;
                }
            }
        }
        if (overflows(returnValue, (int) resultPrecision)) {
            throw throwCastException(value, sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
        return returnValue;
    }

    /**
     * most of the following cast logic is directly copied from io.prestosql.type.DecimalToDecimalCasts
     * TODO: abstract the logic can be reused.
     */
    private static Slice shortToLongCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return longToLongCast(unscaledDecimal(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
    }

    private static long longToShortCast(
            Slice value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return unscaledDecimalToUnscaledLongUnsafe(longToLongCast(value, sourcePrecision, sourceScale, resultPrecision, resultScale));
    }

    private static Slice longToLongCast(
            Slice value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        try {
            Slice result = rescale(value, (int) (resultScale - sourceScale));
            if (overflows(result, (int) resultPrecision)) {
                throw throwCastException(unscaledDecimalToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw throwCastException(unscaledDecimalToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
    }

    private static PrestoException throwCastException(long value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new PrestoException(INVALID_CAST_ARGUMENT,
                format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }

    private static PrestoException throwCastException(BigInteger value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new PrestoException(INVALID_CAST_ARGUMENT,
                format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }
}
