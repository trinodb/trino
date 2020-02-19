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

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DecimalConversions.doubleToLongDecimal;
import static io.prestosql.spi.type.DecimalConversions.doubleToShortDecimal;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;

public class DoubleCoercers
{
    private DoubleCoercers() {}

    public static TypeCoercer<DoubleType, ?> createCoercer(DoubleType sourceType, Type targetType)
    {
        if (targetType instanceof RealType) {
            return TypeCoercer.create(sourceType, (RealType) targetType, DoubleCoercers::coerceDoubleToReal);
        }

        if (targetType instanceof CharType) {
            return TypeCoercer.create(sourceType, (CharType) targetType, DoubleCoercers::coerceDoubleToChar);
        }

        if (targetType instanceof DecimalType) {
            return TypeCoercer.create(sourceType, (DecimalType) targetType, DoubleCoercers::coerceDoubleToDecimal);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceDoubleToReal(DoubleType sourceType, RealType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeLong(blockBuilder, floatToRawIntBits((float) sourceType.getDouble(block, position)));
    }

    private static void coerceDoubleToChar(DoubleType sourceType, CharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        targetType.writeSlice(blockBuilder, utf8Slice(String.valueOf(sourceType.getDouble(block, position))));
    }

    private static void coerceDoubleToDecimal(DoubleType sourceType, DecimalType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        if (targetType.isShort()) {
            targetType.writeLong(blockBuilder, doubleToShortDecimal(sourceType.getDouble(block, position), targetType.getPrecision(), targetType.getScale()));
        }
        else {
            targetType.writeSlice(blockBuilder, doubleToLongDecimal(sourceType.getDouble(block, position), targetType.getPrecision(), targetType.getScale()));
        }
    }
}
