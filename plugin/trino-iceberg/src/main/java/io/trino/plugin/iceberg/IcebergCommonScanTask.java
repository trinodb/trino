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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.spi.TrinoException;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

// Wrapper class to hide specific ScanTask type information from IcebergSplitSource
abstract class IcebergCommonScanTask
        implements SplittableScanTask<IcebergCommonScanTask>, ContentScanTask<DataFile>
{
    protected final ContentScanTask<DataFile> scanTask;

    protected IcebergCommonScanTask(ContentScanTask<DataFile> scanTask)
    {
        this.scanTask = scanTask;
    }

    public Map<Integer, String> getExtraConstantColumnValues()
    {
        return ImmutableMap.of();
    }

    // ScanTask implementation
    @Override
    public long sizeBytes()
    {
        return scanTask.sizeBytes();
    }

    @Override
    public DataFile file()
    {
        return scanTask.file();
    }

    @Override
    public StructLike partition()
    {
        return scanTask.partition();
    }

    @Override
    public long start()
    {
        return scanTask.start();
    }

    @Override
    public long length()
    {
        return scanTask.length();
    }

    @Override
    public Expression residual()
    {
        return scanTask.residual();
    }

    @Override
    public long estimatedRowsCount()
    {
        return scanTask.estimatedRowsCount();
    }

    @Override
    public int filesCount()
    {
        return scanTask.filesCount();
    }

    @Override
    public boolean isFileScanTask()
    {
        return scanTask.isFileScanTask();
    }

    @Override
    public FileScanTask asFileScanTask()
    {
        return scanTask.asFileScanTask();
    }

    @Override
    public boolean isDataTask()
    {
        return scanTask.isDataTask();
    }

    @Override
    public DataTask asDataTask()
    {
        return scanTask.asDataTask();
    }

    @Override
    public CombinedScanTask asCombinedScanTask()
    {
        return scanTask.asCombinedScanTask();
    }

    @Override
    public PartitionSpec spec()
    {
        return scanTask.spec();
    }

    // SplittableScanTask implementation
    @Override
    public Iterable<IcebergCommonScanTask> split(long l)
    {
        // Same logic as in TableScanUtil.planTaskGroups
        if (scanTask instanceof SplittableScanTask<?>) {
            return Iterables.transform(((SplittableScanTask<ContentScanTask<DataFile>>) scanTask).split(l), IcebergCommonScanTask::createCommonScanTask);
        }
        else {
            return ImmutableList.of(createCommonScanTask(scanTask));
        }
    }

    // Abstract methods
    public abstract List<DeleteFile> deletes();

    public static <T extends ScanTask> IcebergCommonScanTask createCommonScanTask(T scanTask)
    {
        if (scanTask instanceof FileScanTask) {
            return new FileScanCommonScanTask((FileScanTask) scanTask);
        }
        else if (scanTask instanceof AddedRowsScanTask) {
            return new AddedRowsCommonScanTask((AddedRowsScanTask) scanTask);
        }
        else if (scanTask instanceof DeletedDataFileScanTask) {
            return new DeletedDataFileCommonScanTask((DeletedDataFileScanTask) scanTask);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, String.format("ScanTask type not supported: %s", scanTask.getClass().getName()));
        }
    }

    // Wrapper on FileScanTask
    private static class FileScanCommonScanTask
            extends IcebergCommonScanTask
    {
        public FileScanCommonScanTask(FileScanTask scanTask)
        {
            super(scanTask);
        }

        @Override
        public List<DeleteFile> deletes()
        {
            return ((FileScanTask) scanTask).deletes();
        }
    }

    // Wrapper on AddedRowsScanTask
    private static class AddedRowsCommonScanTask
            extends IcebergCommonScanTask
    {
        public AddedRowsCommonScanTask(AddedRowsScanTask scanTask)
        {
            super(scanTask);
        }

        @Override
        public List<DeleteFile> deletes()
        {
            return ((AddedRowsScanTask) scanTask).deletes();
        }

        @Override
        public Map<Integer, String> getExtraConstantColumnValues()
        {
            return CommonChangelogScanUtils.getChangelogColumnValues((AddedRowsScanTask) scanTask);
        }
    }

    // Wrapper on DeletedDataFileScanTask
    private static class DeletedDataFileCommonScanTask
            extends IcebergCommonScanTask
    {
        public DeletedDataFileCommonScanTask(DeletedDataFileScanTask scanTask)
        {
            super(scanTask);
        }

        @Override
        public List<DeleteFile> deletes()
        {
            return ((DeletedDataFileScanTask) scanTask).existingDeletes();
        }

        @Override
        public Map<Integer, String> getExtraConstantColumnValues()
        {
            return CommonChangelogScanUtils.getChangelogColumnValues((DeletedDataFileScanTask) scanTask);
        }
    }

    // Changelog Utils class
    private static class CommonChangelogScanUtils
    {
        public static Map<Integer, String> getChangelogColumnValues(ChangelogScanTask scanTask)
        {
            Map<Integer, String> columnValues = new HashMap();
            columnValues.put(MetadataColumns.CHANGE_TYPE.fieldId(), scanTask.operation().toString());
            columnValues.put(MetadataColumns.CHANGE_ORDINAL.fieldId(), Integer.toString(scanTask.changeOrdinal()));
            columnValues.put(MetadataColumns.COMMIT_SNAPSHOT_ID.fieldId(), Long.toString(scanTask.commitSnapshotId()));
            return ImmutableMap.copyOf(columnValues);
        }
    }
}
