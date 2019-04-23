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
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.spi.type.Decimals.overflows;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.compareAbsolute;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.overflows;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.parseFloat;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;

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

    public static Function<Block, Block> createDecimalToDoubleCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToDoubleCoercer(fromType);
        }
        else {
            return new LongDecimalToDoubleCoercer(fromType);
        }
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
                    longDecimalToDouble(fromType.getSlice(block, position), fromType.getScale()));
        }
    }

    public static Function<Block, Block> createDecimalToRealCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToRealCoercer(fromType);
        }
        else {
            return new LongDecimalToRealCoercer(fromType);
        }
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
                    longDecimalToReal(fromType.getSlice(block, position), fromType.getScale()));
        }
    }

    public static Function<Block, Block> createDoubleToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new DoubleToShortDecimalCoercer(toType);
        }
        else {
            return new DoubleToLongDecimalCoercer(toType);
        }
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
            toType.writeSlice(blockBuilder,
                    doubleToLongDecimal(fromType.getDouble(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    public static Function<Block, Block> createRealToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new RealToShortDecimalCoercer(toType);
        }
        else {
            return new RealToLongDecimalCoercer(toType);
        }
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
                    realToShortDecimal(fromType.getLong(block, position), toType.getPrecision(), toType.getScale()));
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
            toType.writeSlice(blockBuilder,
                    realToLongDecimal(fromType.getLong(block, position), toType.getPrecision(), toType.getScale()));
        }
    }

    /**
     * most of the following cast logic is directly copied from io.prestosql.type.DecimalToDecimalCasts
     * and io.prestosql.type.DecimalCasts.DecimalCasts
     * TODO: abstract the logic can be reused.
     */
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
                format("Cannot cast DECIMAL(%d, %d) '%s' to DECIMAL(%d, %d)",
                        sourcePrecision,
                        sourceScale,
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }

    private static PrestoException throwCastException(BigInteger value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new PrestoException(INVALID_CAST_ARGUMENT,
                format("Cannot cast DECIMAL(%d, %d) '%s' to DECIMAL(%d, %d)",
                        sourcePrecision,
                        sourceScale,
                        Decimals.toString(value, (int) sourceScale),
                        resultPrecision,
                        resultScale));
    }

    /**
     * Powers of 10 which can be represented exactly in double.
     */
    private static final double[] DOUBLE_10_POW = {
            1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
            1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
            1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
            1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
    };

    private static final Slice MAX_EXACT_DOUBLE = unscaledDecimal((1L << 52) - 1);

    /**
     * Powers of 10 which can be represented exactly in float.
     */
    private static final float[] FLOAT_10_POW = {
            1.0e0f, 1.0e1f, 1.0e2f, 1.0e3f, 1.0e4f, 1.0e5f,
            1.0e6f, 1.0e7f, 1.0e8f, 1.0e9f, 1.0e10f
    };
    private static final Slice MAX_EXACT_FLOAT = unscaledDecimal((1L << 22) - 1);

    public static double shortDecimalToDouble(long decimal, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    private static double longDecimalToDouble(Slice decimal, long scale)
    {
        // If both decimal and scale can be represented exactly in double then compute rescaled and rounded result directly in double.
        if (scale < DOUBLE_10_POW.length && compareAbsolute(decimal, MAX_EXACT_DOUBLE) <= 0) {
            return (double) unscaledDecimalToUnscaledLongUnsafe(decimal) / DOUBLE_10_POW[intScale(scale)];
        }

        // TODO: optimize and convert directly to double in similar fashion as in double to decimal casts
        return parseDouble(Decimals.toString(decimal, intScale(scale)));
    }

    private static long shortDecimalToReal(long decimal, long tenToScale)
    {
        return floatToRawIntBits(((float) decimal) / tenToScale);
    }

    private static long longDecimalToReal(Slice decimal, long scale)
    {
        // If both decimal and scale can be represented exactly in float then compute rescaled and rounded result directly in float.
        if (scale < FLOAT_10_POW.length && compareAbsolute(decimal, MAX_EXACT_FLOAT) <= 0) {
            return floatToRawIntBits((float) unscaledDecimalToUnscaledLongUnsafe(decimal) / FLOAT_10_POW[intScale(scale)]);
        }

        // TODO: optimize and convert directly to float in similar fashion as in double to decimal casts
        return floatToRawIntBits(parseFloat(Decimals.toString(decimal, intScale(scale))));
    }

    private static long doubleToShortDecimal(double value, long precision, long scale)
    {
        // TODO: implement specialized version for short decimals
        Slice decimal = doubleToLongDecimal(value, precision, scale);

        long low = UnscaledDecimal128Arithmetic.getLong(decimal, 0);
        long high = UnscaledDecimal128Arithmetic.getLong(decimal, 1);

        checkState(high == 0 && low >= 0, "Unexpected long decimal");

        if (UnscaledDecimal128Arithmetic.isNegative(decimal)) {
            return -low;
        }
        else {
            return low;
        }
    }

    private static Slice doubleToLongDecimal(double value, long precision, long scale)
    {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = BigDecimal.valueOf(value).setScale(intScale(scale), HALF_UP);
            Slice decimal = Decimals.encodeScaledValue(bigDecimal);
            if (UnscaledDecimal128Arithmetic.overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT,
                        format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    private static long realToShortDecimal(long value, long precision, long scale)
    {
        // TODO: implement specialized version for short decimals
        Slice decimal = realToLongDecimal(value, precision, scale);

        long low = UnscaledDecimal128Arithmetic.getLong(decimal, 0);
        long high = UnscaledDecimal128Arithmetic.getLong(decimal, 1);

        checkState(high == 0 && low >= 0, "Unexpected long decimal");

        if (UnscaledDecimal128Arithmetic.isNegative(decimal)) {
            return -low;
        }
        else {
            return low;
        }
    }

    private static Slice realToLongDecimal(long value, long precision, long scale)
    {
        float floatValue = intBitsToFloat(intScale(value));
        if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = new BigDecimal(String.valueOf(floatValue)).setScale(intScale(scale), HALF_UP);
            Slice decimal = Decimals.encodeScaledValue(bigDecimal);
            if (UnscaledDecimal128Arithmetic.overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT,
                        format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT,
                    format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int intScale(long scale)
    {
        return (int) scale;
    }
}
