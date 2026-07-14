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
import org.apache.iceberg.FileContent;
import org.apache.iceberg.SortOrder;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record CommitTaskData(
        String path,
        IcebergFileFormat fileFormat,
        long fileSizeInBytes,
        MetricsWrapper metrics,
        String partitionSpecJson,
        Optional<String> partitionDataJson,
        FileContent content,
        Optional<String> referencedDataFile,
        Optional<List<Long>> fileSplitOffsets,
        int sortOrderId,
        Optional<byte[]> serializedDeletionVector,
        Optional<RewriteInfo> rewriteInfo)
{
    public CommitTaskData
    {
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(metrics, "metrics is null");
        requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        requireNonNull(partitionDataJson, "partitionDataJson is null");
        requireNonNull(content, "content is null");
        requireNonNull(referencedDataFile, "referencedDataFile is null");
        requireNonNull(fileSplitOffsets, "fileSplitOffsets is null");
        checkArgument(content == FileContent.DATA || sortOrderId == SortOrder.unsorted().orderId(), "Sorted order id can be present only for data files");
        requireNonNull(serializedDeletionVector, "serializedDeletionVector is null");
        requireNonNull(rewriteInfo, "rewriteInfo is null");
        checkArgument(rewriteInfo.isEmpty() || content == FileContent.DATA, "rewriteInfo can only be present for DATA content, got %s", content);
    }

    public CommitTaskData(
            String path,
            IcebergFileFormat fileFormat,
            long fileSizeInBytes,
            MetricsWrapper metrics,
            String partitionSpecJson,
            Optional<String> partitionDataJson,
            FileContent content,
            Optional<String> referencedDataFile,
            Optional<List<Long>> fileSplitOffsets,
            int sortOrderId,
            Optional<byte[]> serializedDeletionVector)
    {
        this(path,
                fileFormat,
                fileSizeInBytes,
                metrics,
                partitionSpecJson,
                partitionDataJson,
                content,
                referencedDataFile,
                fileSplitOffsets,
                sortOrderId,
                serializedDeletionVector,
                Optional.empty());
    }

    public record RewriteInfo(
            String oldFilePath,
            long oldFileSizeInBytes,
            long oldRecordCount,
            IcebergFileFormat oldFileFormat,
            List<DanglingDeleteFile> danglingDeleteFiles)
    {
        public RewriteInfo
        {
            requireNonNull(oldFilePath, "oldFilePath is null");
            requireNonNull(oldFileFormat, "oldFileFormat is null");
            checkArgument(oldFileSizeInBytes >= 0, "oldFileSizeInBytes is negative");
            checkArgument(oldRecordCount >= 0, "oldRecordCount is negative");
            danglingDeleteFiles = ImmutableList.copyOf(requireNonNull(danglingDeleteFiles, "danglingDeleteFiles is null"));
        }

        public RewriteInfo(String oldFilePath, long oldFileSizeInBytes, long oldRecordCount, IcebergFileFormat oldFileFormat)
        {
            this(oldFilePath, oldFileSizeInBytes, oldRecordCount, oldFileFormat, ImmutableList.of());
        }
    }

    /**
     * Metadata about a delete file that becomes dangling once the data file it references is
     * rewritten (CoW). Sent worker-to-coordinator inside {@link RewriteInfo} via JSON.
     *
     * <p>Wire-shape choice: {@code contentOffset} and {@code contentSizeInBytes} are nullable
     * boxed types (not {@link java.util.OptionalLong} / {@link Optional}{@code <Integer>})
     * because this record is serialised over the JSON fragment channel and we deliberately
     * keep the on-the-wire shape stable across versions. Validation is enforced in the compact
     * constructor instead.
     *
     * <p>Cross-field invariant: a v3 deletion-vector entry has both {@code contentOffset} and
     * {@code contentSizeInBytes} set; a v2 file-scoped position-delete file has both null. They
     * must not be mixed.
     */
    public record DanglingDeleteFile(
            String path,
            long fileSizeInBytes,
            long recordCount,
            String partitionSpecJson,
            Optional<String> partitionDataJson,
            Long contentOffset,
            Long contentSizeInBytes,
            String referencedDataFile)
    {
        public DanglingDeleteFile
        {
            requireNonNull(path, "path is null");
            requireNonNull(partitionSpecJson, "partitionSpecJson is null");
            requireNonNull(partitionDataJson, "partitionDataJson is null");
            requireNonNull(referencedDataFile, "referencedDataFile is null");
            checkArgument(fileSizeInBytes >= 0, "fileSizeInBytes is negative: %s", fileSizeInBytes);
            checkArgument(recordCount >= 0, "recordCount is negative: %s", recordCount);
            checkArgument(
                    (contentOffset == null) == (contentSizeInBytes == null),
                    "contentOffset and contentSizeInBytes must both be present (deletion vector) or both absent (position-delete file)");
            if (contentOffset != null) {
                checkArgument(contentOffset >= 0, "contentOffset is negative: %s", contentOffset);
                checkArgument(contentSizeInBytes >= 0, "contentSizeInBytes is negative: %s", contentSizeInBytes);
            }
        }
    }
}
