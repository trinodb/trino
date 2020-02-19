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
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static java.lang.String.format;

public class VarbinaryCoercers
{
    private VarbinaryCoercers() {}

    public static TypeCoercer<?, ?> createCoercer(VarbinaryType sourceType, Type targetType)
    {
        if (targetType instanceof CharType) {
            return TypeCoercer.create(sourceType, (CharType) targetType, VarbinaryCoercers::coerceVarbinaryToChar);
        }

        if (targetType instanceof VarcharType) {
            return TypeCoercer.create(sourceType, (VarcharType) targetType, VarbinaryCoercers::coerceVarbinaryToVarchar);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s", sourceType, targetType));
    }

    private static void coerceVarbinaryToChar(VarbinaryType sourceType, CharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        sourceType.writeSlice(blockBuilder, truncateToLengthAndTrimSpaces(sourceType.getSlice(block, position), targetType));
    }

    private static void coerceVarbinaryToVarchar(VarbinaryType sourceType, VarcharType targetType, BlockBuilder blockBuilder, Block block, int position)
    {
        sourceType.writeSlice(blockBuilder, truncateToLength(sourceType.getSlice(block, position), targetType));
    }
}
