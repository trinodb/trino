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
package io.trino.operator.output;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.SequencePageBuilder;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.operator.output.SkewedPartitionRebalancer.createSkewedPartitionRebalancer;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSkewedPartitionRebalancer
{
    private static final long MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD = DataSize.of(1, MEGABYTE).toBytes();
    private static final long MIN_DATA_PROCESSED_REBALANCE_THRESHOLD = DataSize.of(50, MEGABYTE).toBytes();

    @Test
    public void testRebalanceWithSkewness()
    {
        int partitionCount = 3;
        SkewedPartitionRebalancer rebalancer = createSkewedPartitionRebalancer(partitionCount, 3, 6, MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD, MIN_DATA_PROCESSED_REBALANCE_THRESHOLD);
        SkewedPartitionFunction function = new SkewedPartitionFunction(new TestPartitionFunction(partitionCount), rebalancer);

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 1000);
        rebalancer.addPartitionRowCount(2, 1000);
        rebalancer.addDataProcessed(DataSize.of(40, MEGABYTE).toBytes());
        // No rebalancing will happen since data processed is less than 50MB limit
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 17))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 3, 6, 9, 12, 15)),
                        new IntArrayList(ImmutableList.of(1, 4, 7, 10, 13, 16)),
                        new IntArrayList(ImmutableList.of(2, 5, 8, 11, 14)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0), ImmutableList.of(1), ImmutableList.of(2));

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 1000);
        rebalancer.addPartitionRowCount(2, 1000);
        rebalancer.addDataProcessed(DataSize.of(20, MEGABYTE).toBytes());
        // Rebalancing will happen since we crossed the data processed limit.
        // Part0 -> Task1 (Bucket1), Part1 -> Task0 (Bucket1), Part2 -> Task0 (Bucket2)
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 17))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 2, 4, 6, 8, 10, 12, 14, 16)),
                        new IntArrayList(ImmutableList.of(1, 3, 7, 9, 13, 15)),
                        new IntArrayList(ImmutableList.of(5, 11)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0, 1), ImmutableList.of(1, 0), ImmutableList.of(2, 0));

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 1000);
        rebalancer.addPartitionRowCount(2, 1000);
        rebalancer.addDataProcessed(DataSize.of(200, MEGABYTE).toBytes());
        // Rebalancing will happen
        // Part0 -> Task2 (Bucket1), Part1 -> Task2 (Bucket2), Part2 -> Task1 (Bucket2)
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 17))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 2, 4, 9, 11, 13)),
                        new IntArrayList(ImmutableList.of(1, 3, 5, 10, 12, 14)),
                        new IntArrayList(ImmutableList.of(6, 7, 8, 15, 16)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0, 1, 2), ImmutableList.of(1, 0, 2), ImmutableList.of(2, 0, 1));
    }

    @Test
    public void testRebalanceWithoutSkewness()
    {
        int partitionCount = 6;
        SkewedPartitionRebalancer rebalancer = createSkewedPartitionRebalancer(partitionCount, 3, 4, MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD, MIN_DATA_PROCESSED_REBALANCE_THRESHOLD);
        SkewedPartitionFunction function = new SkewedPartitionFunction(new TestPartitionFunction(partitionCount), rebalancer);

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 700);
        rebalancer.addPartitionRowCount(2, 600);
        rebalancer.addPartitionRowCount(3, 1000);
        rebalancer.addPartitionRowCount(4, 700);
        rebalancer.addPartitionRowCount(5, 600);
        rebalancer.addDataProcessed(DataSize.of(500, MEGABYTE).toBytes());
        // No rebalancing will happen since there is no skewness across task buckets
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 6))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 3)),
                        new IntArrayList(ImmutableList.of(1, 4)),
                        new IntArrayList(ImmutableList.of(2, 5)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0), ImmutableList.of(1), ImmutableList.of(2), ImmutableList.of(0), ImmutableList.of(1), ImmutableList.of(2));
    }

    @Test
    public void testNoRebalanceWhenDataWrittenIsLessThanTheRebalanceLimit()
    {
        int partitionCount = 3;
        SkewedPartitionRebalancer rebalancer = createSkewedPartitionRebalancer(partitionCount, 3, 6, MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD, MIN_DATA_PROCESSED_REBALANCE_THRESHOLD);
        SkewedPartitionFunction function = new SkewedPartitionFunction(new TestPartitionFunction(partitionCount), rebalancer);

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 0);
        rebalancer.addPartitionRowCount(2, 0);
        rebalancer.addDataProcessed(DataSize.of(40, MEGABYTE).toBytes());
        // No rebalancing will happen since we do not cross the max data processed limit of 50MB
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 6))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 3)),
                        new IntArrayList(ImmutableList.of(1, 4)),
                        new IntArrayList(ImmutableList.of(2, 5)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0), ImmutableList.of(1), ImmutableList.of(2));
    }

    @Test
    public void testNoRebalanceWhenDataWrittenByThePartitionIsLessThanWriterScalingMinDataProcessed()
    {
        int partitionCount = 3;
        long minPartitionDataProcessedRebalanceThreshold = DataSize.of(50, MEGABYTE).toBytes();
        SkewedPartitionRebalancer rebalancer = createSkewedPartitionRebalancer(partitionCount, 3, 6, minPartitionDataProcessedRebalanceThreshold, MIN_DATA_PROCESSED_REBALANCE_THRESHOLD);
        SkewedPartitionFunction function = new SkewedPartitionFunction(new TestPartitionFunction(partitionCount), rebalancer);

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 600);
        rebalancer.addPartitionRowCount(2, 0);
        rebalancer.addDataProcessed(DataSize.of(60, MEGABYTE).toBytes());
        // No rebalancing will happen since no partition has crossed the writerScalingMinDataProcessed limit of 50MB
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 6))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 3)),
                        new IntArrayList(ImmutableList.of(1, 4)),
                        new IntArrayList(ImmutableList.of(2, 5)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0), ImmutableList.of(1), ImmutableList.of(2));
    }

    @Test
    public void testRebalancePartitionToSingleTaskInARebalancingLoop()
    {
        int partitionCount = 3;
        SkewedPartitionRebalancer rebalancer = createSkewedPartitionRebalancer(partitionCount, 3, 6, MIN_PARTITION_DATA_PROCESSED_REBALANCE_THRESHOLD, MIN_DATA_PROCESSED_REBALANCE_THRESHOLD);
        SkewedPartitionFunction function = new SkewedPartitionFunction(new TestPartitionFunction(partitionCount), rebalancer);

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 0);
        rebalancer.addPartitionRowCount(2, 0);

        rebalancer.addDataProcessed(DataSize.of(60, MEGABYTE).toBytes());
        // rebalancing will only happen to single task even though two tasks are available
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 17))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 6, 12)),
                        new IntArrayList(ImmutableList.of(1, 3, 4, 7, 9, 10, 13, 15, 16)),
                        new IntArrayList(ImmutableList.of(2, 5, 8, 11, 14)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0, 1), ImmutableList.of(1), ImmutableList.of(2));

        rebalancer.addPartitionRowCount(0, 1000);
        rebalancer.addPartitionRowCount(1, 0);
        rebalancer.addPartitionRowCount(2, 0);

        rebalancer.addDataProcessed(DataSize.of(60, MEGABYTE).toBytes());
        rebalancer.rebalance();

        assertThat(getPartitionPositions(function, 17))
                .containsExactly(
                        new IntArrayList(ImmutableList.of(0, 9)),
                        new IntArrayList(ImmutableList.of(1, 3, 4, 7, 10, 12, 13, 16)),
                        new IntArrayList(ImmutableList.of(2, 5, 6, 8, 11, 14, 15)));
        assertThat(rebalancer.getPartitionAssignments())
                .containsExactly(ImmutableList.of(0, 1, 2), ImmutableList.of(1), ImmutableList.of(2));
    }

    private List<List<Integer>> getPartitionPositions(PartitionFunction function, int maxPosition)
    {
        List<List<Integer>> partitionPositions = new ArrayList<>();
        for (int partition = 0; partition < function.getPartitionCount(); partition++) {
            partitionPositions.add(new ArrayList<>());
        }

        for (int position = 0; position < maxPosition; position++) {
            int partition = function.getPartition(dummyPage(), position);
            partitionPositions.get(partition).add(position);
        }

        return partitionPositions;
    }

    private static Page dummyPage()
    {
        return SequencePageBuilder.createSequencePage(ImmutableList.of(BIGINT), 100, 0);
    }

    private static class TestPartitionFunction
            implements PartitionFunction
    {
        private final int partitionCount;

        private TestPartitionFunction(int partitionCount)
        {
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return position % partitionCount;
        }
    }
}
