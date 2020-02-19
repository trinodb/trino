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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalConversions.shortToLongCast;
import static io.prestosql.spi.type.DecimalConversions.shortToShortCast;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.type.coercions.TypeCoercers.isNumber;
import static io.prestosql.type.coercions.TypeCoercers.longCanBeCasted;
import static java.lang.Float.floatToIntBits;
import static java.lang.String.format;

public class NumberCoercers
{
    private NumberCoercers() {}

    public static TypeCoercer<?, ?> createCoercer(Type sourceType, Type targetType)
    {
        if (!isNumber(sourceType)) {
            throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
        }

        if (targetType instanceof CharType) {
            return TypeCoercer.create(sourceType, (CharType) targetType, NumberCoercers::coerceVarcharToChar);
        }

        if (targetType instanceof VarcharType) {
            return TypeCoercer.create(sourceType, (VarcharType) targetType, NumberCoercers::coerceNumberToVarchar);
        }

        if (targetType instanceof BigintType) {
            return TypeCoercer.create(sourceType, (BigintType) targetType, NumberCoercers::upscaleNumberCoercer);
        }

        if (targetType instanceof DecimalType) {
            return TypeCoercer.create(sourceType, (DecimalType) targetType, NumberCoercers::coerceNumberToDecimal);
        }

        if (targetType instanceof DoubleType) {
            return TypeCoercer.create(sourceType, (DoubleType) targetType, NumberCoercers::coerceNumberToDouble);
        }

        if (targetType instanceof RealType) {
            return TypeCoercer.create(sourceType, (RealType) targetType, NumberCoercers::coerceNumberToReal);
        }

        if (sourceType instanceof BigintType && targetType instanceof TimestampType) {
            return TypeCoercer.create((BigintType) sourceType, (TimestampType) targetType, NumberCoercers::coerceBigIntToTimestamp);
        }

        if (isNumber(targetType)) {
            return TypeCoercer.create(sourceType, targetType, NumberCoercers::downscaleNumberCoercer);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceVarcharToChar(Type sourceType, CharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeSlice(blockBuilder, utf8Slice(String.valueOf(sourceType.getLong(block, position))));
    }

    private static void upscaleNumberCoercer(Type sourceType, BigintType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeLong(blockBuilder, sourceType.getLong(block, position));
    }

    private static void downscaleNumberCoercer(Type sourceType, Type targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        long value = sourceType.getLong(block, position);

        if (longCanBeCasted(value, sourceType, targetType)) {
            targetType.writeLong(blockBuilder, value);
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
        }
    }

    private static void coerceNumberToVarchar(Type sourceType, VarcharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeSlice(blockBuilder, utf8Slice(String.valueOf(sourceType.getLong(block, position))));
    }

    private static void coerceNumberToDouble(Type sourceType, DoubleType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeDouble(blockBuilder, (double) sourceType.getLong(block, position));
    }

    private static void coerceNumberToReal(Type sourceType, RealType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeLong(blockBuilder, floatToIntBits((float) sourceType.getLong(block, position)));
    }

    private static void coerceBigIntToTimestamp(BigintType sourceType, TimestampType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeLong(blockBuilder, sourceType.getLong(block, position));
    }

    private static void coerceNumberToDecimal(Type sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (targetType.isShort()) {
            long rescale = longTenToNth(Math.abs(targetType.getScale()));

            targetType.writeLong(blockBuilder, shortToShortCast(
                    sourceType.getLong(block, position),
                    Decimals.MAX_SHORT_PRECISION,
                    0,
                    targetType.getPrecision(),
                    targetType.getScale(),
                    rescale,
                    rescale / 2));
        }
        else {
            targetType.writeSlice(blockBuilder, shortToLongCast(
                    sourceType.getLong(block, position),
                    Decimals.MAX_SHORT_PRECISION,
                    0,
                    targetType.getPrecision(),
                    targetType.getScale()));
        }
    }
}
