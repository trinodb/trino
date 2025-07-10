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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static java.lang.Math.clamp;

public class SpoolingPagePartitioner
{
    static final double LOWER_BOUND = 0.05; // 5% of the target size
    static final double UPPER_BOUND = 0.1; // 10% of the target size

    private SpoolingPagePartitioner() {}

    public static List<List<Page>> partition(List<Page> pages, long targetSize)
    {
        Deque<Page> queue = new ArrayDeque<>(pages);
        List<Page> currentPartition = new ArrayList<>();
        ImmutableList.Builder<List<Page>> partitions = ImmutableList.builder();

        while (!queue.isEmpty()) {
            Page currentPage = queue.removeFirst();

            long remainingSize = targetSize - size(currentPartition);
            verify(remainingSize >= 0, "Current partition size %s is larger than target size %s", size(currentPartition), targetSize);

            if (currentPage.getSizeInBytes() < remainingSize) {
                currentPartition.add(currentPage);

                if (withinThreshold(size(currentPartition), targetSize)) {
                    partitions.add(ImmutableList.copyOf(currentPartition));
                    currentPartition.clear();
                }

                continue;
            }

            List<Page> currentPartitioned = new ArrayList<>(takeFromHead(currentPage, remainingSize, targetSize));
            currentPartition.add(currentPartitioned.removeFirst());

            // Add the remaining split pages back to the queue in the original order
            currentPartitioned.reversed().forEach(queue::addFirst);

            if (withinThreshold(size(currentPartition), targetSize)) {
                partitions.add(ImmutableList.copyOf(currentPartition));
                currentPartition.clear();
            }
        }

        // If there are any remaining pages in the current partition, add them as a final partition
        if (!currentPartition.isEmpty()) {
            partitions.add(ImmutableList.copyOf(currentPartition));
        }

        return partitions.build();
    }

    private static boolean withinThreshold(long page, long targetSize)
    {
        return page >= targetSize * (1 - LOWER_BOUND) && page <= targetSize * (1 + UPPER_BOUND);
    }

    private static List<Page> takeFromHead(Page page, long targetHeadSize, long tailSplitSize)
    {
        verify(page.getSizeInBytes() >= targetHeadSize, "Page size %s must be greater than head size %s", page.getSizeInBytes(), targetHeadSize);
        ImmutableList.Builder<Page> builder = ImmutableList.builder();

        int positions = positionsWithBytes(page, targetHeadSize);
        builder.add(page.getRegion(0, positions));

        if (positions == page.getPositionCount()) {
            return builder.build();
        }
        builder.addAll(splitPage(page.getRegion(positions, page.getPositionCount() - positions), tailSplitSize));
        return builder.build();
    }

    private static long averageSizePerPosition(Page page)
    {
        return clamp(page.getSizeInBytes() / (long) page.getPositionCount(), 1, Integer.MAX_VALUE);
    }

    private static int positionsWithBytes(Page page, long bytes)
    {
        long positions = bytes / averageSizePerPosition(page);
        return clamp(positions, 1, page.getPositionCount());
    }

    private static long size(List<Page> pages)
    {
        return pages.stream().mapToLong(Page::getSizeInBytes).sum();
    }
}
