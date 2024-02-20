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
package io.trino.plugin.exchange.filesystem;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final int INSTANCE_SIZE = instanceSize(FileSystemExchangeSourceHandle.class);

    private final ExchangeId exchangeId;
    private final int partitionId;
    private final List<SourceFile> files;

    @JsonCreator
    public FileSystemExchangeSourceHandle(
            @JsonProperty("exchangeId") ExchangeId exchangeId,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("files") List<SourceFile> files)
    {
        this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
        this.partitionId = partitionId;
        this.files = ImmutableList.copyOf(requireNonNull(files, "files is null"));
    }

    @JsonProperty
    public ExchangeId getExchangeId()
    {
        return exchangeId;
    }

    @Override
    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @Override
    public long getDataSizeInBytes()
    {
        return files.stream()
                .mapToLong(SourceFile::getFileSize)
                .sum();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(files, SourceFile::getRetainedSizeInBytes);
    }

    @JsonProperty
    public List<SourceFile> getFiles()
    {
        return files;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exchangeId", exchangeId)
                .add("partitionId", partitionId)
                .add("files", files)
                .toString();
    }

    public static class SourceFile
    {
        private static final int INSTANCE_SIZE = instanceSize(SourceFile.class);

        private final String filePath;
        private final long fileSize;
        private final int sourceTaskPartitionId;
        private final int sourceTaskAttemptId;

        @JsonCreator
        public SourceFile(
                @JsonProperty("filePath") String filePath,
                @JsonProperty("fileSize") long fileSize,
                @JsonProperty("sourceTaskPartitionId") int sourceTaskPartitionId,
                @JsonProperty("sourceTaskAttemptId") int sourceTaskAttemptId)
        {
            this.filePath = requireNonNull(filePath, "filePath is null");
            this.fileSize = fileSize;
            this.sourceTaskPartitionId = sourceTaskPartitionId;
            this.sourceTaskAttemptId = sourceTaskAttemptId;
        }

        @JsonProperty
        public String getFilePath()
        {
            return filePath;
        }

        @JsonProperty
        public long getFileSize()
        {
            return fileSize;
        }

        @JsonProperty
        public int getSourceTaskPartitionId()
        {
            return sourceTaskPartitionId;
        }

        @JsonProperty
        public int getSourceTaskAttemptId()
        {
            return sourceTaskAttemptId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SourceFile that = (SourceFile) o;
            return fileSize == that.fileSize && sourceTaskPartitionId == that.sourceTaskPartitionId && sourceTaskAttemptId == that.sourceTaskAttemptId && Objects.equals(filePath, that.filePath);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filePath, fileSize, sourceTaskPartitionId, sourceTaskAttemptId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filePath", filePath)
                    .add("fileSize", fileSize)
                    .add("sourceTaskPartitionId", sourceTaskPartitionId)
                    .add("sourceTaskAttemptId", sourceTaskAttemptId)
                    .toString();
        }

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(filePath);
        }
    }
}
