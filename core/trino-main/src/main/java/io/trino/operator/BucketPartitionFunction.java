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

import io.trino.spi.Page;
import io.trino.spi.connector.BucketFunction;

import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BucketPartitionFunction
        implements PartitionFunction
{
    private final BucketFunction bucketFunction;
    private final int[] bucketToPartition;
    private final int partitionCount;

    public BucketPartitionFunction(BucketFunction bucketFunction, int[] bucketToPartition)
    {
        this.bucketFunction = requireNonNull(bucketFunction, "bucketFunction is null");
        this.bucketToPartition = bucketToPartition.clone();
        partitionCount = IntStream.of(bucketToPartition).max().getAsInt() + 1;
    }

    @Override
    public int getPartitionCount()
    {
        return partitionCount;
    }

    @Override
    public int getPartition(Page functionArguments, int position)
    {
        int bucket = bucketFunction.getBucket(functionArguments, position);
        return bucketToPartition[bucket];
    }

    @Override
    public void getPartitions(Page functionArguments, int positionOffset, int length, int[] partitions)
    {
        checkArgument(positionOffset >= 0, "Invalid positionOffset: %s", positionOffset);
        checkArgument(length >= 0, "Invalid length: %s", length);
        checkArgument(positionOffset + length <= functionArguments.getPositionCount(), "End position exceeds page position count: %s > %s", positionOffset + length, functionArguments.getPositionCount());
        checkArgument(length <= partitions.length, "Length exceeds partitions length: %s > %s", length, partitions.length);

        bucketFunction.getBuckets(functionArguments, positionOffset, length, partitions);
        for (int i = 0; i < length; i++) {
            int bucket = partitions[i];
            partitions[i] = bucketToPartition[bucket];
        }
    }

    @Override
    public void getPartitions(Page functionArguments, int positionOffset, int length, boolean[] mask, int[] partitions)
    {
        checkArgument(positionOffset >= 0, "Invalid positionOffset: %s", positionOffset);
        checkArgument(length >= 0, "Invalid length: %s", length);
        checkArgument(positionOffset + length <= functionArguments.getPositionCount(), "End position exceeds page position count: %s > %s", positionOffset + length, functionArguments.getPositionCount());
        checkArgument(length <= mask.length, "Length exceeds mask length: %s > %s", length, mask.length);
        checkArgument(length <= partitions.length, "Length exceeds partitions length: %s > %s", length, partitions.length);

        bucketFunction.getBuckets(functionArguments, positionOffset, length, mask, partitions);
        for (int i = 0; i < length; i++) {
            if (mask[i]) {
                int bucket = partitions[i];
                partitions[i] = bucketToPartition[bucket];
            }
        }
    }
}
