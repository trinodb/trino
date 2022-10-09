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

import com.google.common.primitives.Ints;
import io.airlift.slice.XxHash64;
import io.trino.Session;
import io.trino.operator.exchange.LocalPartitionGenerator;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.sql.planner.MergePartitioningHandle;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.ScaleWriterPartitioningHandle;
import io.trino.sql.planner.SystemPartitioningHandle;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PartitionFunctionFactory
{
    private final NodePartitioningManager nodePartitioningManager;
    private final BlockTypeOperators blockTypeOperators;

    public PartitionFunctionFactory(NodePartitioningManager nodePartitioningManager, BlockTypeOperators blockTypeOperators)
    {
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    public PartitionFunction create(
            Session session,
            PartitioningHandle partitioning,
            List<Integer> partitionChannels,
            List<Type> partitionChannelTypes,
            Optional<Integer> partitionHashChannel,
            int partitionCount)
    {
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");
        partitioning = unwrapScalingPartitioningHandle(partitioning);

        if (partitioning.getConnectorHandle() instanceof SystemPartitioningHandle) {
            HashGenerator hashGenerator;
            if (partitionHashChannel.isPresent()) {
                hashGenerator = new PrecomputedHashGenerator(partitionHashChannel.get());
            }
            else {
                hashGenerator = new InterpretedHashGenerator(partitionChannelTypes, Ints.toArray(partitionChannels), blockTypeOperators);
            }
            return new LocalPartitionGenerator(hashGenerator, partitionCount);
        }

        // Distribute buckets assigned to this node among threads.
        // The same bucket function (with the same bucket count) as for node
        // partitioning must be used. This way rows within a single bucket
        // will be being processed by single thread.
        int bucketCount = nodePartitioningManager.getBucketCount(session, partitioning);
        int[] bucketToPartition = new int[bucketCount];

        for (int bucket = 0; bucket < bucketCount; bucket++) {
            // mix the bucket bits so we don't use the same bucket number used to distribute between stages
            int hashedBucket = (int) XxHash64.hash(Long.reverse(bucket));
            bucketToPartition[bucket] = hashedBucket & (partitionCount - 1);
        }

        if (partitioning.getConnectorHandle() instanceof MergePartitioningHandle handle) {
            return handle.getPartitionFunction(
                    (scheme, types) -> nodePartitioningManager.getPartitionFunction(session, scheme, types, bucketToPartition),
                    partitionChannelTypes,
                    bucketToPartition);
        }

        return new BucketPartitionFunction(
                nodePartitioningManager.getBucketFunction(session, partitioning, partitionChannelTypes, bucketCount),
                bucketToPartition);
    }

    public Function<Page, Page> createPartitionPagePreparer(PartitioningHandle partitioning, List<Integer> partitionChannels)
    {
        partitioning = unwrapScalingPartitioningHandle(partitioning);
        Function<Page, Page> partitionPagePreparer;
        if (partitioning.getConnectorHandle() instanceof SystemPartitioningHandle) {
            partitionPagePreparer = identity();
        }
        else {
            int[] partitionChannelsArray = Ints.toArray(partitionChannels);
            partitionPagePreparer = page -> page.getColumns(partitionChannelsArray);
        }
        return partitionPagePreparer;
    }

    private PartitioningHandle unwrapScalingPartitioningHandle(PartitioningHandle partitioning)
    {
        if (partitioning.getConnectorHandle() instanceof ScaleWriterPartitioningHandle handle) {
            return new PartitioningHandle(
                    partitioning.getCatalogHandle(),
                    partitioning.getTransactionHandle(),
                    handle.getPartitioningHandle());
        }
        return partitioning;
    }
}
