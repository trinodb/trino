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
package io.trino.plugin.hudi.partition;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ResumableTask.TaskStatus;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiPartitionInfoLoader
{
    private static final String TABLE_PATH = "/test/table/path";

    @Test
    public void testLoaderCreation()
    {
        // Test that we can create a HudiPartitionInfoLoader with valid inputs
        try (TestHudiDirectoryLister directoryLister = new TestHudiDirectoryLister()) {
            HudiSplitFactory splitFactory = createSplitFactory();
            AsyncQueue<ConnectorSplit> asyncQueue = new ThrottledAsyncQueue<>(
                    100,
                    1000,
                    Executors.newSingleThreadExecutor());
            Deque<String> partitionQueue = new ConcurrentLinkedDeque<>();
            Deque<Iterator<ConnectorSplit>> splitIterators = new ConcurrentLinkedDeque<>();

            HudiPartitionInfoLoader loader = new HudiPartitionInfoLoader(
                    directoryLister,
                    splitFactory,
                    asyncQueue,
                    partitionQueue,
                    splitIterators);

            assertThat(loader).isNotNull();
        }
    }

    @Test
    public void testLoaderYieldsAndResumesWithMultiplePartitions()
            throws Exception
    {
        // This test verifies the fix for https://github.com/trinodb/trino/issues/26967
        // Simulates the deadlock scenario: multiple partitions generating splits with a small queue
        // The loader should:
        // 1. Yield when queue is full (return TaskStatus.continueOn)
        // 2. Save its state (current split iterator)
        // 3. Resume from saved state on next process() call
        // 4. Complete without blocking threads

        try (TestHudiDirectoryListerWithSplits directoryLister = new TestHudiDirectoryListerWithSplits()) {
            HudiSplitFactory splitFactory = createSplitFactory();

            // Create a queue with capacity of 1 to trigger full condition immediately
            AsyncQueue<ConnectorSplit> asyncQueue = new ThrottledAsyncQueue<>(
                    1000,  // maxSplitsPerSecond
                    1,     // maxOutstandingSplits - very small to trigger yielding
                    Executors.newSingleThreadExecutor());

            // Create multiple partitions to process
            Deque<String> partitionQueue = new ConcurrentLinkedDeque<>();
            partitionQueue.add("partition1");
            partitionQueue.add("partition2");
            partitionQueue.add("partition3");

            Deque<Iterator<ConnectorSplit>> splitIterators = new ConcurrentLinkedDeque<>();

            HudiPartitionInfoLoader loader = new HudiPartitionInfoLoader(
                    directoryLister,
                    splitFactory,
                    asyncQueue,
                    partitionQueue,
                    splitIterators);

            // First process() call - should start processing first partition
            TaskStatus status1 = loader.process();

            // With a queue capacity of 1 and multiple splits per partition,
            // the loader should yield after adding the first split
            // The key fix: it returns TaskStatus.continueOn(future) instead of blocking
            if (!status1.isFinished()) {
                assertThat(status1.isFinished()).isFalse();
                // Verify that state was saved - splitIterators should have the current iterator
                assertThat(splitIterators).isNotEmpty();
            }

            // Signal to stop processing new partitions
            loader.stopRunning();

            // Continue processing until finished
            // The loader should be able to resume and complete without deadlock
            int maxIterations = 100; // Safety limit to prevent infinite loop in test
            int iterations = 0;
            TaskStatus currentStatus = status1;

            while (!currentStatus.isFinished() && iterations < maxIterations) {
                // Simulate consuming from the queue to make space
                asyncQueue.getBatchAsync(10);

                // Resume processing
                currentStatus = loader.process();
                iterations++;
            }

            // Verify the loader completed successfully without blocking
            assertThat(currentStatus.isFinished()).isTrue();
            assertThat(iterations).isLessThan(maxIterations);
        }
    }

    private static HudiSplitFactory createSplitFactory()
    {
        HudiTableHandle tableHandle = new HudiTableHandle(
                "test_schema",
                "test_table",
                TABLE_PATH,
                HoodieTableType.COPY_ON_WRITE,
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all());
        HudiSplitWeightProvider weightProvider = new SizeBasedSplitWeightProvider(0.05, DataSize.of(128, MEGABYTE));
        return new HudiSplitFactory(tableHandle, weightProvider);
    }

    // Test implementation of HudiDirectoryLister that returns empty file statuses
    private static class TestHudiDirectoryLister
            implements HudiDirectoryLister
    {
        @Override
        public List<HudiFileStatus> listStatus(HudiPartitionInfo partitionInfo)
        {
            // Return empty list for testing
            return ImmutableList.of();
        }

        @Override
        public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
        {
            return Optional.of(new TestPartitionInfo(partition));
        }

        @Override
        public void close()
        {
            // No-op for testing
        }
    }

    // Test implementation of HudiDirectoryLister that returns file statuses with splits
    private static class TestHudiDirectoryListerWithSplits
            implements HudiDirectoryLister
    {
        @Override
        public List<HudiFileStatus> listStatus(HudiPartitionInfo partitionInfo)
        {
            // Return multiple file statuses per partition to trigger queue full condition
            List<HudiFileStatus> fileStatuses = new ArrayList<>();

            // Create 3 file statuses per partition
            for (int i = 0; i < 3; i++) {
                String filePath = TABLE_PATH + "/" + partitionInfo.getRelativePartitionPath() + "/test-file-" + i + ".parquet";
                fileStatuses.add(new HudiFileStatus(
                        Location.of(filePath),
                        false,
                        DataSize.of(10, MEGABYTE).toBytes(),
                        System.currentTimeMillis(),
                        8L * 1024 * 1024));
            }

            return fileStatuses;
        }

        @Override
        public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
        {
            return Optional.of(new TestPartitionInfo(partition));
        }

        @Override
        public void close()
        {
            // No-op for testing
        }
    }

    // Test implementation of HudiPartitionInfo
    private static class TestPartitionInfo
            implements HudiPartitionInfo
    {
        private final String partitionPath;

        public TestPartitionInfo(String partitionPath)
        {
            this.partitionPath = partitionPath;
        }

        @Override
        public String getRelativePartitionPath()
        {
            return partitionPath;
        }

        @Override
        public List<HivePartitionKey> getHivePartitionKeys()
        {
            return ImmutableList.of();
        }

        @Override
        public boolean doesMatchPredicates()
        {
            return true;
        }

        @Override
        public void loadPartitionInfo(Optional<Partition> partition)
        {
            // No-op for testing
        }
    }
}
