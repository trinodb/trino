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
package io.trino.plugin.iceberg.system.partitions;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public final class PartitionsTableSplitSource
        implements ConnectorSplitSource
{
    private static final int TARGET_TASKS_PER_SPLIT = 100; // Balance between parallelism and overhead

    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final String schemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Optional<Type> partitionColumnType;
    private final Optional<Type> dataColumnType;
    private final Map<String, String> fileIoProperties;
    private final ExecutorService executor;
    private boolean finished;

    public PartitionsTableSplitSource(
            Table icebergTable,
            Optional<Long> snapshotId,
            Optional<Type> partitionColumnType,
            Optional<Type> dataColumnType,
            Map<String, String> fileIoProperties,
            ExecutorService executor)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.schemaJson = SchemaParser.toJson(icebergTable.schema());
        this.partitionSpecsByIdJson = icebergTable.specs().entrySet().stream().collect(toImmutableMap(
                Map.Entry::getKey,
                partitionSpec -> PartitionSpecParser.toJson(partitionSpec.getValue())));
        this.partitionColumnType = requireNonNull(partitionColumnType, "partitionColumnType is null");
        this.dataColumnType = requireNonNull(dataColumnType, "dataColumnType is null");
        this.fileIoProperties = requireNonNull(fileIoProperties, "fileIoProperties is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.finished = false;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (finished) {
            return completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
        }

        if (snapshotId.isEmpty()) {
            finished = true;
            return completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
        }

        try {
            TableScan tableScan = icebergTable.newScan()
                    .useSnapshot(snapshotId.get())
                    .includeColumnStats()
                    .planWith(executor);

            List<ConnectorSplit> splits = createSplitsFromFileScanTasks(tableScan);
            finished = true;
            return completedFuture(new ConnectorSplitBatch(splits, true));
        }
        catch (Exception e) {
            finished = true;
            throw new RuntimeException("Failed to create partitions table splits", e);
        }
    }

    private List<ConnectorSplit> createSplitsFromFileScanTasks(TableScan tableScan)
    {
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            List<PartitionsTableSplit.FileScanTaskData> allTasks = new ArrayList<>();
            
            for (FileScanTask fileScanTask : fileScanTasks) {
                allTasks.add(PartitionsTableSplit.FileScanTaskData.from(fileScanTask));
            }

            // Group tasks into splits to balance parallelism and overhead
            List<ConnectorSplit> splits = new ArrayList<>();
            List<PartitionsTableSplit.FileScanTaskData> currentBatch = new ArrayList<>();
            
            for (PartitionsTableSplit.FileScanTaskData task : allTasks) {
                currentBatch.add(task);
                
                if (currentBatch.size() >= TARGET_TASKS_PER_SPLIT) {
                    splits.add(createSplit(new ArrayList<>(currentBatch)));
                    currentBatch.clear();
                }
            }
            
            // Add remaining tasks
            if (!currentBatch.isEmpty()) {
                splits.add(createSplit(new ArrayList<>(currentBatch)));
            }
            
            return splits;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to process file scan tasks", e);
        }
    }

    private PartitionsTableSplit createSplit(List<PartitionsTableSplit.FileScanTaskData> tasks)
    {
        return new PartitionsTableSplit(
                tasks,
                schemaJson,
                partitionSpecsByIdJson,
                partitionColumnType,
                dataColumnType,
                fileIoProperties);
    }

    @Override
    public void close()
    {
        // nothing to close
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}