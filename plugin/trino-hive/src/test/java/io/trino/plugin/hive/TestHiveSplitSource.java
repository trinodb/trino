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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveSplitSource
{
    @Test
    public void testOutstandingSplitCount()
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10,
                DataSize.of(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                false);

        // add 10 splits
        for (int i = 0; i < 10; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove 1 split
        assertEquals(getSplits(hiveSplitSource, 1).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 9);

        // remove 4 splits
        assertEquals(getSplits(hiveSplitSource, 4).size(), 4);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 5);

        // try to remove 20 splits, and verify we only got 5
        assertEquals(getSplits(hiveSplitSource, 20).size(), 5);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 0);
    }

    @Test
    public void testDynamicPartitionPruning()
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10,
                DataSize.of(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                false);

        // add two splits, one of the splits is dynamically pruned
        hiveSplitSource.addToQueue(new TestSplit(0, () -> false));
        hiveSplitSource.addToQueue(new TestSplit(1, () -> true));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 2);

        // try to remove 2 splits, only one should be returned
        assertEquals(getSplits(hiveSplitSource, 2).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 0);
    }

    @Test
    public void testEvenlySizedSplitRemainder()
    {
        DataSize initialSplitSize = getMaxInitialSplitSize(SESSION);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10,
                DataSize.of(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newSingleThreadExecutor(),
                new CounterStat(),
                false);

        // One byte larger than the initial split max size
        DataSize fileSize = DataSize.ofBytes(initialSplitSize.toBytes() + 1);
        long halfOfSize = fileSize.toBytes() / 2;
        hiveSplitSource.addToQueue(new TestSplit(1, OptionalInt.empty(), fileSize));

        HiveSplit first = (HiveSplit) getSplits(hiveSplitSource, 1).get(0);
        assertEquals(first.getLength(), halfOfSize);

        HiveSplit second = (HiveSplit) getSplits(hiveSplitSource, 1).get(0);
        assertEquals(second.getLength(), fileSize.toBytes() - halfOfSize);
    }

    @Test
    public void testFail()
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10,
                DataSize.of(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                false);

        // add some splits
        for (int i = 0; i < 5; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        // remove a split and verify
        assertEquals(getSplits(hiveSplitSource, 1).size(), 1);
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // fail source
        hiveSplitSource.fail(new RuntimeException("test"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4);

        // try to remove a split and verify we got the expected exception
        assertThatThrownBy(() -> getSplits(hiveSplitSource, 1))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("test");
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // attempt to add another split and verify it does not work
        hiveSplitSource.addToQueue(new TestSplit(99));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // fail source again
        hiveSplitSource.fail(new RuntimeException("another failure"));
        assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), 4); // 3 splits + poison

        // try to remove a split and verify we got the first exception
        assertThatThrownBy(() -> getSplits(hiveSplitSource, 1))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("test");
    }

    @Test
    public void testReaderWaitsForSplits()
            throws Exception
    {
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10,
                DataSize.of(1, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                false);

        SettableFuture<ConnectorSplit> splits = SettableFuture.create();

        // create a thread that will get a split
        CountDownLatch started = new CountDownLatch(1);
        Thread getterThread = new Thread(() -> {
            try {
                started.countDown();
                List<ConnectorSplit> batch = getSplits(hiveSplitSource, 1);
                assertEquals(batch.size(), 1);
                splits.set(batch.get(0));
            }
            catch (Throwable e) {
                splits.setException(e);
            }
        });
        getterThread.start();

        try {
            // wait for the thread to be started
            assertTrue(started.await(1, TimeUnit.SECONDS));

            // sleep for a bit, and assure the thread is blocked
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(!splits.isDone());

            // add a split
            hiveSplitSource.addToQueue(new TestSplit(33));

            // wait for thread to get the split
            ConnectorSplit split = splits.get(800, TimeUnit.MILLISECONDS);
            assertEquals(((HiveSplit) split).getSchema().getProperty("id"), "33");
        }
        finally {
            // make sure the thread exits
            getterThread.interrupt();
        }
    }

    @Test
    public void testOutstandingSplitSize()
    {
        DataSize maxOutstandingSplitsSize = DataSize.of(1, MEGABYTE);
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                SESSION,
                "database",
                "table",
                10,
                10000,
                maxOutstandingSplitsSize,
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                false);
        int testSplitSizeInBytes = new TestSplit(0).getEstimatedSizeInBytes();

        int maxSplitCount = toIntExact(maxOutstandingSplitsSize.toBytes()) / testSplitSizeInBytes;
        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }

        assertEquals(getSplits(hiveSplitSource, maxSplitCount).size(), maxSplitCount);

        for (int i = 0; i < maxSplitCount; i++) {
            hiveSplitSource.addToQueue(new TestSplit(i));
            assertEquals(hiveSplitSource.getBufferedInternalSplitCount(), i + 1);
        }
        assertTrinoExceptionThrownBy(() -> hiveSplitSource.addToQueue(new TestSplit(0)))
                .hasErrorCode(HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT)
                .hasMessageContaining("Split buffering for database.table exceeded memory limit");
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getFutureValue(source.getNextBatch(maxSize)).getSplits();
    }

    private static class TestingHiveSplitLoader
            implements HiveSplitLoader
    {
        @Override
        public void start(HiveSplitSource splitSource)
        {
        }

        @Override
        public void stop()
        {
        }
    }

    private static class TestSplit
            extends InternalHiveSplit
    {
        private TestSplit(int id)
        {
            this(id, OptionalInt.empty());
        }

        private TestSplit(int id, BooleanSupplier partitionMatchSupplier)
        {
            this(id, OptionalInt.empty(), DataSize.ofBytes(100), partitionMatchSupplier);
        }

        private TestSplit(int id, OptionalInt bucketNumber)
        {
            this(id, bucketNumber, DataSize.ofBytes(100));
        }

        private TestSplit(int id, OptionalInt bucketNumber, DataSize fileSize)
        {
            this(id, bucketNumber, fileSize, () -> true);
        }

        private TestSplit(int id, OptionalInt bucketNumber, DataSize fileSize, BooleanSupplier partitionMatchSupplier)
        {
            super(
                    "partition-name",
                    "path",
                    0,
                    fileSize.toBytes(),
                    fileSize.toBytes(),
                    Instant.now().toEpochMilli(),
                    properties("id", String.valueOf(id)),
                    ImmutableList.of(),
                    ImmutableList.of(new InternalHiveBlock(0, fileSize.toBytes(), ImmutableList.of())),
                    bucketNumber,
                    bucketNumber,
                    true,
                    false,
                    TableToPartitionMapping.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    partitionMatchSupplier);
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.setProperty(key, value);
            return properties;
        }
    }
}
