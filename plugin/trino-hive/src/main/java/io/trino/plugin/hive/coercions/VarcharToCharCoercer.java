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

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;

public class VarcharToCharCoercer
        extends TypeCoercer<VarcharType, CharType>
{
    public VarcharToCharCoercer(VarcharType fromType, CharType toType)
    {
        super(fromType, toType);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        Slice value = fromType.getSlice(block, position);
        toType.writeSlice(blockBuilder, truncateToLengthAndTrimSpaces(value, toType));
    }
}
