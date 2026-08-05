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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.ToLongFunction;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

class TestSpoolingPagePartitioner
{
    @Test
    void testPartitionPagesWithMaxSize()
    {
        testPartitionPages(100, 750);
        testPartitionPages(512, 158);
        testPartitionPages(1_000, 82);
        testPartitionPages(2_000, 41);
        testPartitionPages(3_000, 28);
        testPartitionPages(5_000, 17);
        testPartitionPages(10_000, 9);
        testPartitionPages(25_000, 4);
        testPartitionPages(40_000, 3);
        testPartitionPages(81_000, 1);
    }

    void testPartitionPages(int maxPartitionSize, int expectedPartitions)
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);
        List<Page> pages = ImmutableList.<Page>builder()
                .add(createSequencePage(types, 500, 0, 0, 0))
                .add(createSequencePage(types, 500, 500, 500, 500))
                .add(createSequencePage(types, 2000, 1000, 1000, 1000))
                .build();

        List<List<Page>> partitions = SpoolingPagePartitioner.partition(pages, maxPartitionSize);

        // Partitioning does not change size in bytes
        assertThat(size(partitions))
                .isEqualTo(reduce(pages, Page::getSizeInBytes))
                .isEqualTo(81000);

        // Partitioning does not change position count
        assertThat(positions(partitions))
                .isEqualTo(reduce(pages, Page::getPositionCount))
                .isEqualTo(3000);

        assertPartitionSizes(partitions, maxPartitionSize, expectedPartitions);
    }

    @Test
    void testPartitionPagesWithVariableLengthRows()
    {
        // First row is very large (1000 chars), remaining 9 rows are tiny (1 char).
        // averageSizePerPosition ≈ 100 bytes, so when remainingSize=120, takeFromHead
        // picks 1 position — but that position is 1000 bytes, overshooting the target
        // by far more than the 10% upper bound. Without the fix this causes
        // verify(remainingSize >= 0) to fail on the next iteration.
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);
        VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("a".repeat(1000)));
        for (int i = 0; i < 9; i++) {
            VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice("a"));
        }
        Page page = new Page(blockBuilder.build());

        List<List<Page>> partitions = SpoolingPagePartitioner.partition(ImmutableList.of(page), 120);

        assertThat(size(partitions)).isEqualTo(page.getSizeInBytes());
        assertThat(positions(partitions)).isEqualTo(page.getPositionCount());
    }

    private void assertPartitionSizes(List<List<Page>> partitions, long maxPartitionSize, int expectedPartitions)
    {
        assertThat(partitions).hasSize(expectedPartitions);

        // Last partition can be smaller than maxPartitionSize
        for (int i = 0; i < partitions.size() - 1; i++) {
            List<Page> partition = partitions.get(i);
            long partitionSize = reduce(partition, Page::getSizeInBytes);
            assertThat(partitionSize)
                    .isBetween(
                            (long) (maxPartitionSize * (1 - SpoolingPagePartitioner.LOWER_BOUND)),
                            (long) (maxPartitionSize * (1 + SpoolingPagePartitioner.UPPER_BOUND)));
        }

        List<Page> pages = flatten(partitions);

        // Verify that the partitioned pages contain the expected values in expected order
        long currentPosition = 0;
        for (Page page : pages) {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    assertThat(BIGINT.getObjectValue(page.getBlock(channel), position)).isEqualTo(currentPosition);
                }
                currentPosition++;
            }
        }
    }

    private static long size(List<List<Page>> partitions)
    {
        return calculate(partitions, Page::getSizeInBytes);
    }

    private static long positions(List<List<Page>> partitions)
    {
        return calculate(partitions, Page::getPositionCount);
    }

    private static long calculate(List<List<Page>> partitions, ToLongFunction<Page> pageFunction)
    {
        return partitions.stream()
                .mapToLong(pages -> reduce(pages, pageFunction))
                .sum();
    }

    private static long reduce(List<Page> pages, ToLongFunction<Page> pageFunction)
    {
        return pages.stream().mapToLong(pageFunction).sum();
    }

    private static List<Page> flatten(List<List<Page>> partitions)
    {
        return partitions.stream()
                .flatMap(List::stream)
                .collect(toImmutableList());
    }
}
