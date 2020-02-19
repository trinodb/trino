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
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalConversions.realToLongDecimal;
import static io.prestosql.spi.type.DecimalConversions.realToShortDecimal;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;

public class RealCoercers
{
    private RealCoercers() {}

    public static TypeCoercer<RealType, ?> createCoercer(RealType sourceType, Type targetType)
    {
        if (targetType instanceof DoubleType) {
            return TypeCoercer.create(sourceType, (DoubleType) targetType, RealCoercers::realToDoubleCoercer);
        }

        if (targetType instanceof CharType) {
            return TypeCoercer.create(sourceType, (CharType) targetType, RealCoercers::realToCharCoercer);
        }

        if (targetType instanceof VarcharType) {
            return TypeCoercer.create(sourceType, (VarcharType) targetType, RealCoercers::realToVarcharCoercer);
        }

        if (targetType instanceof DecimalType) {
            return TypeCoercer.create(sourceType, (DecimalType) targetType, RealCoercers::realToDecimalCoercer);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void realToDoubleCoercer(RealType sourceType, DoubleType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeDouble(blockBuilder, intBitsToFloat((int) sourceType.getLong(block, position)));
    }

    private static void realToCharCoercer(RealType sourceType, CharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeSlice(blockBuilder, utf8Slice(String.valueOf(intBitsToFloat((int) sourceType.getLong(block, position)))));
    }

    private static void realToVarcharCoercer(RealType sourceType, VarcharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeSlice(blockBuilder, utf8Slice(String.valueOf(intBitsToFloat((int) sourceType.getLong(block, position)))));
    }

    private static void realToDecimalCoercer(RealType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (targetType.isShort()) {
            targetType.writeLong(blockBuilder, realToShortDecimal(sourceType.getLong(block, position), targetType.getPrecision(), targetType.getScale()));
        }
        else {
            targetType.writeSlice(blockBuilder, realToLongDecimal(sourceType.getLong(block, position), targetType.getPrecision(), targetType.getScale()));
        }
    }
}
