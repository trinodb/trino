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
package io.trino.plugin.hive;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.createLongRepeatBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V2;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.testng.Assert.assertEquals;

public class TestHivePartitionedBucketFunction
{
    @DataProvider(name = "hiveBucketingVersion")
    public static Object[][] hiveBucketingVersion()
    {
        return new Object[][] {{BUCKETING_V1}, {BUCKETING_V2}};
    }

    @Test(dataProvider = "hiveBucketingVersion")
    public void testSinglePartition(BucketingVersion hiveBucketingVersion)
    {
        int numValues = 1024;
        int numBuckets = 10;
        Block bucketColumn = createLongSequenceBlockWithNull(numValues);
        Page bucketedColumnPage = new Page(bucketColumn);
        Block partitionColumn = createLongRepeatBlock(78758, numValues);
        Page page = new Page(bucketColumn, partitionColumn);
        BucketFunction hiveBucketFunction = bucketFunction(hiveBucketingVersion, numBuckets, ImmutableList.of(HIVE_LONG));
        Multimap<Integer, Integer> bucketPositions = HashMultimap.create();

        for (int i = 0; i < numValues; i++) {
            int hiveBucket = hiveBucketFunction.getBucket(bucketedColumnPage, i);
            // record list of positions for each hive bucket
            bucketPositions.put(hiveBucket, i);
        }

        BucketFunction hivePartitionedBucketFunction = partitionedBucketFunction(hiveBucketingVersion, numBuckets, ImmutableList.of(HIVE_LONG), ImmutableList.of(BIGINT), 100);
        // All positions of a bucket should hash to the same partitioned bucket
        for (Map.Entry<Integer, Collection<Integer>> entry : bucketPositions.asMap().entrySet()) {
            assertBucketCount(hivePartitionedBucketFunction, page, entry.getValue(), 1);
        }

        assertBucketCount(
                hivePartitionedBucketFunction,
                page,
                IntStream.range(0, numValues).boxed().collect(toImmutableList()),
                numBuckets);
    }

    @Test(dataProvider = "hiveBucketingVersion")
    public void testMultiplePartitions(BucketingVersion hiveBucketingVersion)
    {
        int numValues = 1024;
        int numBuckets = 10;
        Block bucketColumn = createLongSequenceBlockWithNull(numValues);
        Page bucketedColumnPage = new Page(bucketColumn);
        BucketFunction hiveBucketFunction = bucketFunction(hiveBucketingVersion, numBuckets, ImmutableList.of(HIVE_LONG));

        int numPartitions = 8;
        List<Long> partitionValues = new ArrayList<>();
        for (int i = 0; i < numPartitions - 1; i++) {
            partitionValues.addAll(Collections.nCopies(numValues / numPartitions, i * 348349L));
        }
        partitionValues.addAll(Collections.nCopies(numValues / numPartitions, null));
        Block partitionColumn = createLongsBlock(partitionValues);
        Page page = new Page(bucketColumn, partitionColumn);
        Map<Long, HashMultimap<Integer, Integer>> partitionedBucketPositions = new HashMap<>();

        for (int i = 0; i < numValues; i++) {
            int hiveBucket = hiveBucketFunction.getBucket(bucketedColumnPage, i);
            Long hivePartition = partitionValues.get(i);
            // record list of positions for each combination of hive partition and bucket
            partitionedBucketPositions.computeIfAbsent(hivePartition, ignored -> HashMultimap.create())
                    .put(hiveBucket, i);
        }

        BucketFunction hivePartitionedBucketFunction = partitionedBucketFunction(hiveBucketingVersion, numBuckets, ImmutableList.of(HIVE_LONG), ImmutableList.of(BIGINT), 4000);
        // All positions of a hive partition and bucket should hash to the same partitioned bucket
        for (Map.Entry<Long, HashMultimap<Integer, Integer>> partitionEntry : partitionedBucketPositions.entrySet()) {
            for (Map.Entry<Integer, Collection<Integer>> entry : partitionEntry.getValue().asMap().entrySet()) {
                assertBucketCount(hivePartitionedBucketFunction, page, entry.getValue(), 1);
            }
        }

        assertBucketCount(
                hivePartitionedBucketFunction,
                page,
                IntStream.range(0, numValues).boxed().collect(toImmutableList()),
                numBuckets * numPartitions);
    }

    /**
     * Make sure that hashes for single partitions are consecutive. This makes
     * sure that single partition insert will be distributed across all worker nodes
     * (when number of workers is less or equal to number of partition buckets) because
     * workers are assigned to consecutive buckets in sequence.
     */
    @Test(dataProvider = "hiveBucketingVersion")
    public void testConsecutiveBucketsWithinPartition(BucketingVersion hiveBucketingVersion)
    {
        BlockBuilder bucketColumn = BIGINT.createFixedSizeBlockBuilder(10);
        BlockBuilder partitionColumn = BIGINT.createFixedSizeBlockBuilder(10);
        for (int i = 0; i < 100; ++i) {
            BIGINT.writeLong(bucketColumn, i);
            BIGINT.writeLong(partitionColumn, 42);
        }
        Page page = new Page(bucketColumn, partitionColumn);

        BucketFunction hivePartitionedBucketFunction = partitionedBucketFunction(hiveBucketingVersion, 10, ImmutableList.of(HIVE_LONG), ImmutableList.of(BIGINT), 4000);
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            positions.add(hivePartitionedBucketFunction.getBucket(page, i));
        }

        int minPosition = min(positions);
        int maxPosition = max(positions);

        // assert that every bucket number was generated
        assertEquals(maxPosition - minPosition + 1, 10);
    }

    private static void assertBucketCount(BucketFunction bucketFunction, Page page, Collection<Integer> positions, int bucketCount)
    {
        assertEquals(
                positions.stream()
                        .map(position -> bucketFunction.getBucket(page, position))
                        .distinct()
                        .count(),
                bucketCount);
    }

    private static Block createLongSequenceBlockWithNull(int numValues)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(numValues);
        int start = 923402935;
        int end = start + numValues - 1;
        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }
        builder.appendNull();
        return builder.build();
    }

    private static BucketFunction partitionedBucketFunction(BucketingVersion hiveBucketingVersion, int hiveBucketCount, List<HiveType> hiveBucketTypes, List<Type> partitionColumnsTypes, int bucketCount)
    {
        return new HivePartitionedBucketFunction(
                hiveBucketingVersion,
                hiveBucketCount,
                hiveBucketTypes,
                partitionColumnsTypes,
                new TypeOperators(),
                bucketCount);
    }

    private static BucketFunction bucketFunction(BucketingVersion hiveBucketingVersion, int hiveBucketCount, List<HiveType> hiveBucketTypes)
    {
        return new HiveBucketFunction(hiveBucketingVersion, hiveBucketCount, hiveBucketTypes);
    }
}
