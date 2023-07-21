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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.Varchars.truncateToLength;

public class IntegerNumberToVarcharCoercer<F extends Type>
        extends TypeCoercer<F, VarcharType>
{
    public IntegerNumberToVarcharCoercer(F fromType, VarcharType toType)
    {
        super(fromType, toType);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        long value = fromType.getLong(block, position);
        Slice converted = utf8Slice(String.valueOf(value));
        // Truncate the Slice to match the length bound of the target type
        Slice slice = truncateToLength(converted, toType);
        toType.writeSlice(blockBuilder, slice);
    }
}
