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

package io.trino.operator.aggregation.histogram;

import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class SingleHistogramState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = instanceSize(SingleHistogramState.class);

    private final Type keyType;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;
    private SingleTypedHistogram typedHistogram;

    public SingleHistogramState(Type keyType, BlockPositionEqual equalOperator, BlockPositionHashCode hashCodeOperator, int expectedEntriesCount)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.equalOperator = requireNonNull(equalOperator, "equalOperator is null");
        this.hashCodeOperator = requireNonNull(hashCodeOperator, "hashCodeOperator is null");
        typedHistogram = new SingleTypedHistogram(keyType, equalOperator, hashCodeOperator, expectedEntriesCount);
    }

    @Override
    public TypedHistogram get()
    {
        return typedHistogram;
    }

    @Override
    public void deserialize(Block block, int expectedSize)
    {
        typedHistogram = new SingleTypedHistogram(block, keyType, equalOperator, hashCodeOperator, expectedSize);
    }

    @Override
    public void addMemoryUsage(long memory)
    {
    }

    @Override
    public long getEstimatedSize()
    {
        long estimatedSize = INSTANCE_SIZE;

        if (typedHistogram != null) {
            estimatedSize += typedHistogram.getEstimatedSize();
        }
        return estimatedSize;
    }
}
