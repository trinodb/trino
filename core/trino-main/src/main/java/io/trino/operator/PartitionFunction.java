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

import static com.google.common.base.Preconditions.checkArgument;

public interface PartitionFunction
{
    int getPartitionCount();

    /**
     * @param page the arguments to bucketing function in order (no extra columns)
     * @deprecated Use {@link #getPartitions}.
     */
    @Deprecated
    int getPartition(Page page, int position);

    /**
     * @param page the arguments to bucketing function in order (no extra columns)
     * @param partitions the array of length at least {@code length} to hold the partitioning result.
     * Value at index {@code i} should hold the partition for position {@code positionOffset + i}.
     */
    default void getPartitions(Page page, int positionOffset, int length, int[] partitions)
    {
        checkArgument(positionOffset >= 0, "Invalid positionOffset: %s", positionOffset);
        checkArgument(length >= 0, "Invalid length: %s", length);
        checkArgument(positionOffset + length <= page.getPositionCount(), "End position exceeds page position count: %s > %s", positionOffset + length, page.getPositionCount());
        checkArgument(length <= partitions.length, "Length exceeds partitions length: %s > %s", length, partitions.length);

        for (int i = 0; i < length; i++) {
            partitions[i] = getPartition(page, positionOffset + i);
        }
    }

    /**
     * @param page the arguments to bucketing function in order (no extra columns)
     * @param mask defines which positions should have partition calculated for. Partition should be calculated for position {@code positionOffset + i}
     * if value at index {@code i} is true.
     * @param partitions the array of length at least {@code length} to hold the partitioning result.
     * Value at index {@code i} should hold the partition for position {@code positionOffset + i}.
     */
    default void getPartitions(Page page, int positionOffset, int length, boolean[] mask, int[] partitions)
    {
        checkArgument(positionOffset >= 0, "Invalid positionOffset: %s", positionOffset);
        checkArgument(length >= 0, "Invalid length: %s", length);
        checkArgument(positionOffset + length <= page.getPositionCount(), "End position exceeds page position count: %s > %s", positionOffset + length, page.getPositionCount());
        checkArgument(length <= mask.length, "Length exceeds mask length: %s > %s", length, mask.length);
        checkArgument(length <= partitions.length, "Length exceeds partitions length: %s > %s", length, partitions.length);

        for (int i = 0; i < length; i++) {
            if (mask[i]) {
                partitions[i] = getPartition(page, positionOffset + i);
            }
        }
    }
}
