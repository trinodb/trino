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
package io.trino.sql.planner;

import io.trino.operator.HashGenerator;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;

import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class HashPartitionFunction
        implements PartitionFunction
{
    private final HashGenerator generator;
    private final int bucketCount;
    private final int[] bucketToPartition;
    private final int partitionCount;

    public HashPartitionFunction(HashGenerator generator, int bucketCount, int[] bucketToPartition)
    {
        checkArgument(bucketCount > 0, "partitionCount must be at least 1");
        this.generator = generator;
        this.bucketCount = bucketCount;
        this.bucketToPartition = bucketToPartition.clone();
        partitionCount = IntStream.of(bucketToPartition).max().getAsInt() + 1;
    }

    @Override
    public int partitionCount()
    {
        return partitionCount;
    }

    @Override
    public int getPartition(Page functionArguments, int position)
    {
        int bucket = generator.getPartition(bucketCount, position, functionArguments);
        return bucketToPartition[bucket];
    }

    @Override
    public void getPartitions(Page page, int[] partitions, long[] rawHashes, int offset, int length)
    {
        generator.hashBlocksBatched(page, rawHashes, offset, length);
        for (int i = 0; i < length; i++) {
            long rawHash = rawHashes[i];
            // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
            // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
            int bucket = (int) ((Integer.toUnsignedLong(Long.hashCode(rawHash)) * bucketToPartition.length) >>> 32);
            partitions[i] = bucketToPartition[bucket];
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("generator", generator)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
