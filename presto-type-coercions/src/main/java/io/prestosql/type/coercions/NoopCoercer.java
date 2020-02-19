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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import static java.lang.String.format;

public class NoopCoercer
        extends TypeCoercer
{
    public NoopCoercer(Type fromType, Type toType)
    {
        super(fromType, toType);

        if (!fromType.equals(toType)) {
            throw new IllegalArgumentException(format("Type %s does not match %s type", fromType, toType));
        }
    }

    @Override
    public Block apply(Block block)
    {
        return block;
    }

    @Override
    public void coerceValue(BlockBuilder blockBuilder, Block block, int position)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return format("NoopCoercer<%s>", sourceType);
    }
}
