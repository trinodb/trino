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
import io.trino.sql.planner.HashBucketFunction;

import java.util.stream.IntStream;

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
    public int partitionCount()
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
    public void getPartitions(Page page, int[] partitions, long[] rawHashes, int offset, int length)
    {
        boolean isBatchedHashCalculated = false;
        if (bucketFunction instanceof HashBucketFunction) {
            isBatchedHashCalculated = ((HashBucketFunction) bucketFunction).calculateBatchedHashes(page, rawHashes, offset, length);
        }
        if (isBatchedHashCalculated) {
            for (int i = 0; i < length; i++) {
                long rawHash = rawHashes[i];
                // This function reduces the 64 bit rawHash to [0, partitionCount) uniformly. It first reduces the rawHash to 32 bit
                // integer x then normalize it to x / 2^32 * partitionCount to reduce the range of x from [0, 2^32) to [0, partitionCount)
                int bucket = (int) ((Integer.toUnsignedLong(Long.hashCode(rawHash)) * bucketToPartition.length) >>> 32);
                partitions[i] = bucketToPartition[bucket];
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                partitions[i] = getPartition(page, offset + i);
            }
        }
    }
}
