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
package io.prestosql.type.coercions;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalConversions.longDecimalToDouble;
import static io.prestosql.spi.type.DecimalConversions.longDecimalToReal;
import static io.prestosql.spi.type.DecimalConversions.longToLongCast;
import static io.prestosql.spi.type.DecimalConversions.longToShortCast;
import static io.prestosql.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.prestosql.spi.type.DecimalConversions.shortDecimalToReal;
import static io.prestosql.spi.type.DecimalConversions.shortToLongCast;
import static io.prestosql.spi.type.DecimalConversions.shortToShortCast;
import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.type.coercions.TypeCoercers.isNumber;
import static io.prestosql.type.coercions.TypeCoercers.longCanBeCasted;
import static java.lang.String.format;

public final class DecimalCoercers
{
    private DecimalCoercers() {}

    public static TypeCoercer<DecimalType, ?> createCoercer(DecimalType sourceType, Type targetType)
    {
        if (targetType instanceof DecimalType) {
            return TypeCoercer.create(sourceType, (DecimalType) targetType, DecimalCoercers::coerceDecimalToDecimal);
        }

        if (targetType instanceof DoubleType) {
            return TypeCoercer.create(sourceType, (DoubleType) targetType, DecimalCoercers::coerceDecimalToDouble);
        }

        if (targetType instanceof RealType) {
            return TypeCoercer.create(sourceType, (RealType) targetType, DecimalCoercers::coerceDecimalToReal);
        }

        if (isNumber(targetType)) {
            return TypeCoercer.create(sourceType, targetType, DecimalCoercers::coerceDecimalToNumber);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceDecimalToNumber(DecimalType sourceType, Type targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        long value;
        long rescale = longTenToNth(sourceType.getScale());

        if (sourceType.isShort()) {
            value = shortToShortCast(sourceType.getLong(block, position),
                    sourceType.getPrecision(),
                    sourceType.getScale(),
                    MAX_SHORT_PRECISION,
                    0,
                    rescale,
                    rescale / 2);
        }
        else {
            value = longToShortCast(sourceType.getSlice(block, position),
                    sourceType.getPrecision(),
                    sourceType.getScale(),
                    MAX_SHORT_PRECISION,
                    0);
        }

        if (longCanBeCasted(value, sourceType, targetType)) {
            targetType.writeLong(blockBuilder, value);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
        }
    }

    private static void coerceDecimalToDecimal(DecimalType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (sourceType.isShort()) {
            if (targetType.isShort()) {
                coerceShortDecimalToShortDecimal(sourceType, targetType, blockBuilder, block, position);
            }
            else {
                coerceShortDecimalToLongDecimal(sourceType, targetType, blockBuilder, block, position);
            }
        }
        else if (targetType.isShort()) {
            coerceLongDecimalToShortDecimal(sourceType, targetType, blockBuilder, block, position);
        }
        else {
            coerceLongDecimalToLongDecimal(sourceType, targetType, blockBuilder, block, position);
        }
    }

    private static void coerceLongDecimalToLongDecimal(DecimalType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        Slice coercedValue = longToLongCast(sourceType.getSlice(block, position),
                sourceType.getPrecision(),
                sourceType.getScale(),
                targetType.getPrecision(),
                targetType.getScale());

        targetType.writeSlice(blockBuilder, coercedValue);
    }

    private static void coerceLongDecimalToShortDecimal(DecimalType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        long returnValue = longToShortCast(sourceType.getSlice(block, position),
                sourceType.getPrecision(),
                sourceType.getScale(),
                targetType.getPrecision(),
                targetType.getScale());

        targetType.writeLong(blockBuilder, returnValue);
    }

    private static void coerceShortDecimalToShortDecimal(DecimalType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        long rescale = longTenToNth(Math.abs(targetType.getScale() - sourceType.getScale()));

        long returnValue = shortToShortCast(sourceType.getLong(block, position),
                sourceType.getPrecision(),
                sourceType.getScale(),
                targetType.getPrecision(),
                targetType.getScale(),
                rescale,
                rescale / 2);

        targetType.writeLong(blockBuilder, returnValue);
    }

    private static void coerceShortDecimalToLongDecimal(DecimalType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        Slice coercedValue = shortToLongCast(sourceType.getLong(block, position),
                sourceType.getPrecision(),
                sourceType.getScale(),
                targetType.getPrecision(),
                targetType.getScale());

        targetType.writeSlice(blockBuilder, coercedValue);
    }

    public static void coerceDecimalToDouble(DecimalType sourceType, DoubleType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (sourceType.isShort()) {
            targetType.writeDouble(blockBuilder, shortDecimalToDouble(sourceType.getLong(block, position), longTenToNth(sourceType.getScale())));
        }
        else {
            targetType.writeDouble(blockBuilder, longDecimalToDouble(sourceType.getSlice(block, position), sourceType.getScale()));
        }
    }

    public static void coerceDecimalToReal(DecimalType sourceType, RealType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (sourceType.isShort()) {
            targetType.writeLong(blockBuilder, shortDecimalToReal(sourceType.getLong(block, position), longTenToNth(sourceType.getScale())));
        }
        else {
            targetType.writeLong(blockBuilder, longDecimalToReal(sourceType.getSlice(block, position), sourceType.getScale()));
        }
    }
}
