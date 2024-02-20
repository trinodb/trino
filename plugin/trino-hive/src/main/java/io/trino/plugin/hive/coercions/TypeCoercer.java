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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public abstract class TypeCoercer<F extends Type, T extends Type>
        implements Function<Block, Block>
{
    protected final F fromType;
    protected final T toType;

    protected TypeCoercer(F fromType, T toType)
    {
        this.fromType = requireNonNull(fromType);
        this.toType = requireNonNull(toType);
    }

    @Override
    public Block apply(Block block)
    {
        BlockBuilder blockBuilder = toType.createBlockBuilder(null, block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            applyCoercedValue(blockBuilder, block, i);
        }
        return blockBuilder.build();
    }

    protected abstract void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position);

    public Type getFromType()
    {
        return fromType;
    }

    public Type getToType()
    {
        return toType;
    }
}
