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

package io.trino.operator.exchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.operator.exchange.UniformPartitionRebalancer.WriterPartitionId;
import static io.trino.operator.exchange.UniformPartitionRebalancer.WriterPartitionId.serialize;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUniformPartitionRebalancer
{
    @Test
    public void testRebalanceWithWriterSkewness()
    {
        AtomicLong physicalWrittenBytesForWriter0 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter1 = new AtomicLong(0);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                4,
                2,
                DataSize.of(4, MEGABYTE).toBytes());

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 2L,
                new WriterPartitionId(1, 1), 20000L,
                new WriterPartitionId(0, 2), 2L,
                new WriterPartitionId(1, 3), 20000L)));

        physicalWrittenBytesForWriter1.set(DataSize.of(200, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1, 0));

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 3), 10000L,
                new WriterPartitionId(1, 3), 10000L,
                new WriterPartitionId(1, 1), 40000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(50, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(500, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1, 0),
                        ImmutableList.of(0),
                        ImmutableList.of(1, 0));

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 1), 10000L,
                new WriterPartitionId(1, 1), 10000L,
                new WriterPartitionId(0, 3), 10000L,
                new WriterPartitionId(1, 3), 20000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(100, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1, 0),
                        ImmutableList.of(0),
                        ImmutableList.of(1, 0));
    }

    @Test
    public void testComputeRebalanceThroughputWithAllWritersOfTheSamePartition()
    {
        AtomicLong physicalWrittenBytesForWriter0 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter1 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter2 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter3 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter4 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter5 = new AtomicLong(0);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get,
                physicalWrittenBytesForWriter2::get,
                physicalWrittenBytesForWriter3::get,
                physicalWrittenBytesForWriter4::get,
                physicalWrittenBytesForWriter5::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                2,
                6,
                DataSize.of(4, MEGABYTE).toBytes());

        // init 6 writers and 2 partitions, so partition0 -> writer0 and partition1 -> writer1
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 2))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1));

        // new data, partition0 -> writer0 has 100M, and partition1 -> writer1 has 1M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 10000L,
                new WriterPartitionId(1, 1), 100L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(100, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(1, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 rebalanced, partition0 -> writer[0, 2]
        // partition1's data is less than threshold.
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 2))
                .containsExactly(
                        ImmutableList.of(0, 2),
                        ImmutableList.of(1));

        // new data, partition0 -> writer[0, 2] each has 100M, and partition1 -> writer1 has 1M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 20000L,
                new WriterPartitionId(1, 1), 200L,
                new WriterPartitionId(2, 0), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(200, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(2, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter2.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 rebalanced, partition0 -> writer[0, 2, 3]
        // partition1's data is less than threshold.
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 2))
                .containsExactly(
                        ImmutableList.of(0, 2, 3),
                        ImmutableList.of(1));

        // new data, partition0 -> writer[0, 2, 3] each has 100M, and partition1 -> writer1 has 1M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 30000L,
                new WriterPartitionId(1, 1), 300L,
                new WriterPartitionId(2, 0), 20000L,
                new WriterPartitionId(3, 0), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(300, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(3, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter2.set(DataSize.of(200, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter3.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 rebalanced, partition0 -> writer[0, 2, 3, 4]
        // partition1's data is less than threshold.
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 2))
                .containsExactly(
                        ImmutableList.of(0, 2, 3, 4),
                        ImmutableList.of(1));

        // new data, partition0 -> writer[0, 2, 3, 4] each has 100M, and partition1 -> writer1 has 90M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 40000L,
                new WriterPartitionId(1, 1), 9300L,
                new WriterPartitionId(2, 0), 30000L,
                new WriterPartitionId(3, 0), 20000L,
                new WriterPartitionId(4, 0), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(400, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(93, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter2.set(DataSize.of(300, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter3.set(DataSize.of(200, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter4.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 be rebalanced, partition0 -> writer[0, 2, 3, 4, 5]
        // only partition0 rebalanced, because after rebalanced partition0
        // we estimate 6 writers' throughput are [80, 90, 80, 80, 80, 80],
        // and data skew is less than threshold.
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 2))
                .containsExactly(
                        ImmutableList.of(0, 2, 3, 4, 5),
                        ImmutableList.of(1));
    }

    @Test
    public void testRebalanceAffectAllWritersOfTheSamePartition()
    {
        AtomicLong physicalWrittenBytesForWriter0 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter1 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter2 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter3 = new AtomicLong(0);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get,
                physicalWrittenBytesForWriter2::get,
                physicalWrittenBytesForWriter3::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                3,
                4,
                DataSize.of(4, MEGABYTE).toBytes());

        // init 4 writers and 3 partitions, so partition0 -> writer0, partition1 -> writer1 and
        // partition2 -> writer2
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 3))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(2));

        // new data, partition0 -> writer0 has 100M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 rebalanced, partition0 -> writer[0, 1]
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 3))
                .containsExactly(
                        ImmutableList.of(0, 1),
                        ImmutableList.of(1),
                        ImmutableList.of(2));

        // new data, partition1 -> writer1 has 100M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 10000L,
                new WriterPartitionId(1, 1), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(100, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition1 rebalanced, partition0 -> writer[0, 1], partition1 -> writer[1, 0]
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 3))
                .containsExactly(
                        ImmutableList.of(0, 1),
                        ImmutableList.of(1, 0),
                        ImmutableList.of(2));

        // new data, partition0 -> wrter0 31M, partition0 -> writer1 30M
        // partition1 -> writer0 10M, partition1 -> writer1 10M
        // partition2 -> writer2 10M
        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 13000L,
                new WriterPartitionId(0, 1), 3000L,
                new WriterPartitionId(1, 0), 1000L,
                new WriterPartitionId(1, 1), 11000L,
                new WriterPartitionId(2, 2), 1000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(141, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(140, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter2.set(DataSize.of(10, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        // check partition0 rebalanced, partition0 -> writer[0, 1, 3]
        // this affect the writer1 and writer3's throughput,
        // now all writers' throughput is [30, 30, 10, 20] and the skew is less than threshold,
        // no more rebalance needed.
        assertThat(getWriterIdsForPartitions(partitionRebalancer, 3))
                .containsExactly(
                        ImmutableList.of(0, 1, 3),
                        ImmutableList.of(1, 0),
                        ImmutableList.of(2));
    }

    @Test
    public void testNoRebalanceWhenDataWrittenIsLessThanTheRebalanceLimit()
    {
        AtomicLong physicalWrittenBytesForWriter0 = new AtomicLong(0);
        AtomicLong physicalWrittenBytesForWriter1 = new AtomicLong(0);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                4,
                2,
                DataSize.of(4, MEGABYTE).toBytes());

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 2L,
                new WriterPartitionId(1, 1), 20000L,
                new WriterPartitionId(0, 2), 2L,
                new WriterPartitionId(1, 3), 20000L)));

        physicalWrittenBytesForWriter1.set(DataSize.of(30, MEGABYTE).toBytes());

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1));

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1));
    }

    @Test
    public void testNoRebalanceWithoutWriterSkewness()
    {
        AtomicReference<Long> physicalWrittenBytesForWriter0 = new AtomicReference<>(0L);
        AtomicReference<Long> physicalWrittenBytesForWriter1 = new AtomicReference<>(0L);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                4,
                2,
                DataSize.of(4, MEGABYTE).toBytes());

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 20000L,
                new WriterPartitionId(1, 1), 20000L,
                new WriterPartitionId(0, 2), 20000L,
                new WriterPartitionId(1, 3), 20000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(50, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter1.set(DataSize.of(100, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1));

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1));
    }

    @Test
    public void testNoRebalanceWhenDataWrittenByThePartitionIsLessThanWriterMinSize()
    {
        AtomicReference<Long> physicalWrittenBytesForWriter0 = new AtomicReference<>(0L);
        AtomicReference<Long> physicalWrittenBytesForWriter1 = new AtomicReference<>(0L);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                4,
                2,
                DataSize.of(500, MEGABYTE).toBytes());

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 2L,
                new WriterPartitionId(1, 1), 20000L,
                new WriterPartitionId(0, 2), 2L,
                new WriterPartitionId(1, 3), 20000L)));

        physicalWrittenBytesForWriter1.set(DataSize.of(200, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 4))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(0),
                        ImmutableList.of(1));
    }

    @Test
    public void testPartitionShouldNotScaledTwiceInTheSameRebalanceCall()
    {
        AtomicReference<Long> physicalWrittenBytesForWriter0 = new AtomicReference<>(0L);
        AtomicReference<Long> physicalWrittenBytesForWriter1 = new AtomicReference<>(0L);
        AtomicReference<Long> physicalWrittenBytesForWriter2 = new AtomicReference<>(0L);
        List<Supplier<Long>> writerPhysicalWrittenBytes = ImmutableList.of(
                physicalWrittenBytesForWriter0::get,
                physicalWrittenBytesForWriter1::get,
                physicalWrittenBytesForWriter2::get);
        AtomicReference<Long2LongMap> partitionRowCounts = new AtomicReference<>(new Long2LongOpenHashMap());

        UniformPartitionRebalancer partitionRebalancer = new UniformPartitionRebalancer(
                writerPhysicalWrittenBytes,
                partitionRowCounts::get,
                6,
                3,
                DataSize.of(32, MEGABYTE).toBytes());

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 0), 2L,
                new WriterPartitionId(1, 1), 2L,
                new WriterPartitionId(2, 2), 2L,
                new WriterPartitionId(0, 3), 2L,
                new WriterPartitionId(1, 4), 2L,
                new WriterPartitionId(2, 5), 20000L)));

        physicalWrittenBytesForWriter2.set(DataSize.of(200, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 6))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(2),
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(2, 0));

        partitionRowCounts.set(serializeToLong2LongMap(ImmutableMap.of(
                new WriterPartitionId(0, 5), 10000L,
                new WriterPartitionId(2, 5), 10000L)));

        physicalWrittenBytesForWriter0.set(DataSize.of(100, MEGABYTE).toBytes());
        physicalWrittenBytesForWriter2.set(DataSize.of(300, MEGABYTE).toBytes());

        partitionRebalancer.rebalancePartitions();

        assertThat(getWriterIdsForPartitions(partitionRebalancer, 6))
                .containsExactly(
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(2),
                        ImmutableList.of(0),
                        ImmutableList.of(1),
                        ImmutableList.of(2, 0, 1));
    }

    private Long2LongMap serializeToLong2LongMap(Map<WriterPartitionId, Long> input)
    {
        return new Long2LongOpenHashMap(
                input.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> serialize(entry.getKey()),
                                Map.Entry::getValue)));
    }

    private List<List<Integer>> getWriterIdsForPartitions(UniformPartitionRebalancer partitionRebalancer, int partitionCount)
    {
        return IntStream.range(0, partitionCount)
                .mapToObj(partitionRebalancer::getWriterIds)
                .collect(toImmutableList());
    }
}
