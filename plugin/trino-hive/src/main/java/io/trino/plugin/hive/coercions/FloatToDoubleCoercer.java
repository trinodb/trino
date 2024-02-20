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
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;

public class FloatToDoubleCoercer
        extends TypeCoercer<RealType, DoubleType>
{
    public FloatToDoubleCoercer()
    {
        super(REAL, DOUBLE);
    }

    @Override
    public Block apply(Block block)
    {
        // data may have already been coerced by the Avro reader
        if (block instanceof LongArrayBlock) {
            return block;
        }
        return super.apply(block);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        DOUBLE.writeDouble(blockBuilder, REAL.getFloat(block, position));
    }
}
