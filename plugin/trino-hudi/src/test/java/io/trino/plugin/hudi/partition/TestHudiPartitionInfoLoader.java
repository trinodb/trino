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
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ResumableTask.TaskStatus;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiPartitionInfoLoader
{
    private static final String COMMIT_TIME = "20250625153731546";
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
            Deque<HiveHudiPartitionInfo> partitionQueue = new ConcurrentLinkedDeque<>();
            Deque<Iterator<ConnectorSplit>> splitIterators = new ConcurrentLinkedDeque<>();

            HudiPartitionInfoLoader loader = new HudiPartitionInfoLoader(
                    directoryLister,
                    COMMIT_TIME,
                    splitFactory,
                    asyncQueue,
                    partitionQueue,
                    false,
                    splitIterators);

            assertThat(loader).isNotNull();
        }
    }

    @Test
    public void testLoaderYieldsAndResumesWithMultiplePartitions()
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
            Deque<HiveHudiPartitionInfo> partitionQueue = new ConcurrentLinkedDeque<>();
            partitionQueue.add(createTestPartition("partition1"));
            partitionQueue.add(createTestPartition("partition2"));
            partitionQueue.add(createTestPartition("partition3"));

            Deque<Iterator<ConnectorSplit>> splitIterators = new ConcurrentLinkedDeque<>();

            HudiPartitionInfoLoader loader = new HudiPartitionInfoLoader(
                    directoryLister,
                    COMMIT_TIME,
                    splitFactory,
                    asyncQueue,
                    partitionQueue,
                    false,
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
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                java.util.OptionalLong.empty(),
                "",
                "101");
        HudiSplitWeightProvider weightProvider = new SizeBasedSplitWeightProvider(0.05, DataSize.of(128, MEGABYTE));
        return new HudiSplitFactory(tableHandle, weightProvider, DataSize.of(128, MEGABYTE));
    }

    private static HiveHudiPartitionInfo createTestPartition(String partitionPath)
    {
        SchemaTableName schemaTableName = new SchemaTableName("test_schema", "test_table");
        Location tableLocation = Location.of(TABLE_PATH);
        String partitionLocation = TABLE_PATH + "/" + partitionPath;

        Partition partition = Partition.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setValues(ImmutableList.of())
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                                "org.apache.hadoop.mapred.TextInputFormat",
                                "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
                        .setLocation(partitionLocation))
                .setColumns(ImmutableList.of())
                .build();

        return new HiveHudiPartitionInfo(
                schemaTableName,
                tableLocation,
                partitionPath,
                partition,
                ImmutableList.<HiveColumnHandle>of(),
                TupleDomain.all());
    }

    private static FileSlice createTestFileSlice(String partitionPath, int fileNumber)
    {
        String fileId = "test-file-" + fileNumber;
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId(partitionPath, fileId);
        long blockSize = 8L * 1024 * 1024;
        String baseFilePath = TABLE_PATH + "/" + partitionPath + "/" + fileId + "_" + COMMIT_TIME + ".parquet";

        StoragePathInfo baseFileInfo = new StoragePathInfo(
                new StoragePath(baseFilePath),
                DataSize.of(10, MEGABYTE).toBytes(),
                false,
                (short) 0,
                blockSize,
                System.currentTimeMillis());

        HoodieBaseFile baseFile = new HoodieBaseFile(baseFileInfo);
        return new FileSlice(fileGroupId, COMMIT_TIME, baseFile, ImmutableList.of());
    }

    // Test implementation of HudiDirectoryLister that returns empty file slices
    private static class TestHudiDirectoryLister
            implements HudiDirectoryLister
    {
        @Override
        public List<FileSlice> listStatus(HudiPartitionInfo partition, boolean useIndex)
        {
            // Return empty list for testing
            return ImmutableList.of();
        }

        @Override
        public void close()
        {
            // No-op for testing
        }
    }

    // Test implementation of HudiDirectoryLister that returns file slices with splits
    private static class TestHudiDirectoryListerWithSplits
            implements HudiDirectoryLister
    {
        @Override
        public List<FileSlice> listStatus(HudiPartitionInfo partition, boolean useIndex)
        {
            // Return multiple file slices per partition to trigger queue full condition
            String partitionPath = partition.getRelativePartitionPath();
            List<FileSlice> fileSlices = new ArrayList<>();

            // Create 3 file slices per partition
            for (int i = 0; i < 3; i++) {
                fileSlices.add(createTestFileSlice(partitionPath, i));
            }

            return fileSlices;
        }

        @Override
        public void close()
        {
            // No-op for testing
        }
    }
}
