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
package io.trino.execution.scheduler;

import com.google.common.primitives.ImmutableLongArray;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OutputDataSizeEstimate
{
    private final ImmutableLongArray partitionDataSizes;

    public OutputDataSizeEstimate(ImmutableLongArray partitionDataSizes)
    {
        this.partitionDataSizes = requireNonNull(partitionDataSizes, "partitionDataSizes is null");
    }

    public long getPartitionSizeInBytes(int partitionId)
    {
        return partitionDataSizes.get(partitionId);
    }

    public static OutputDataSizeEstimate merge(Collection<OutputDataSizeEstimate> estimates)
    {
        int partitionCount = getPartitionCount(estimates);
        long[] merged = new long[partitionCount];
        for (OutputDataSizeEstimate estimate : estimates) {
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                merged[partitionId] += estimate.getPartitionSizeInBytes(partitionId);
            }
        }
        return new OutputDataSizeEstimate(ImmutableLongArray.copyOf(merged));
    }

    private static int getPartitionCount(Collection<OutputDataSizeEstimate> estimates)
    {
        int[] partitionCounts = estimates.stream()
                .mapToInt(estimate -> estimate.partitionDataSizes.length())
                .distinct()
                .toArray();
        checkArgument(partitionCounts.length <= 1, "partition count is expected to match");
        if (partitionCounts.length == 0) {
            return 0;
        }
        return partitionCounts[0];
    }
}
