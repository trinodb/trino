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
package io.trino.operator;

import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class ChannelSet
{
    private final FlatSet set;

    private ChannelSet(FlatSet set)
    {
        this.set = set;
    }

    public long getEstimatedSizeInBytes()
    {
        return set.getEstimatedSize();
    }

    public int size()
    {
        return set.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public boolean containsNull()
    {
        return set.containsNull();
    }

    public boolean contains(Block valueBlock, int position)
    {
        return set.contains(valueBlock, position);
    }

    public boolean contains(Block valueBlock, int position, long rawHash)
    {
        return set.contains(valueBlock, position, rawHash);
    }

    public static class ChannelSetBuilder
    {
        private final LocalMemoryContext memoryContext;
        private final FlatSet set;

        public ChannelSetBuilder(Type type, TypeOperators typeOperators, LocalMemoryContext memoryContext)
        {
            set = new FlatSet(
                    type,
                    typeOperators.getReadValueOperator(type, simpleConvention(FLAT_RETURN, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, FLAT)),
                    typeOperators.getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, FLAT, BLOCK_POSITION_NOT_NULL)),
                    typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION_NOT_NULL)));
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.memoryContext.setBytes(set.getEstimatedSize());
        }

        public ChannelSet build()
        {
            return new ChannelSet(set);
        }

        public void addAll(Block valueBlock, Block hashBlock)
        {
            if (valueBlock.getPositionCount() == 0) {
                return;
            }

            if (valueBlock instanceof RunLengthEncodedBlock rleBlock) {
                if (hashBlock != null) {
                    set.add(rleBlock.getValue(), 0, BIGINT.getLong(hashBlock, 0));
                }
                else {
                    set.add(rleBlock.getValue(), 0);
                }
            }
            else if (hashBlock != null) {
                for (int position = 0; position < valueBlock.getPositionCount(); position++) {
                    set.add(valueBlock, position, BIGINT.getLong(hashBlock, position));
                }
            }
            else {
                for (int position = 0; position < valueBlock.getPositionCount(); position++) {
                    set.add(valueBlock, position);
                }
            }

            memoryContext.setBytes(set.getEstimatedSize());
        }
    }
}
