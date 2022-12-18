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

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.List;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.exchange.filesystem.FileSystemExchangeManager.PATH_SEPARATOR;

@DefunctConfig("exchange.encryption-enabled")
public class FileSystemExchangeConfig
{
    private List<URI> baseDirectories = ImmutableList.of();
    // For S3, we make read requests aligned with part boundaries. Incomplete slice at the end of the buffer is
    // possible and will be copied to the beginning of the new buffer, and we need to make room for that.
    // Therefore, it's recommended to set `maxPageStorageSize` to be slightly larger than a multiple of part size.
    private DataSize maxPageStorageSize = DataSize.of(16, MEGABYTE);
    private int exchangeSinkBufferPoolMinSize = 10;
    private int exchangeSinkBuffersPerPartition = 2;
    private DataSize exchangeSinkMaxFileSize = DataSize.of(1, GIGABYTE);
    private int exchangeSourceConcurrentReaders = 4;
    private int exchangeSourceMaxFilesPerReader = 25;
    private int maxOutputPartitionCount = 50;
    private int exchangeFileListingParallelism = 50;
    private DataSize exchangeSourceHandleTargetDataSize = DataSize.of(256, MEGABYTE);

    @NotNull
    @NotEmpty(message = "At least one base directory needs to be configured")
    public List<URI> getBaseDirectories()
    {
        return baseDirectories;
    }

    @Config("exchange.base-directories")
    @LegacyConfig("exchange.base-directory")
    @ConfigDescription("List of base directories separated by commas")
    public FileSystemExchangeConfig setBaseDirectories(String baseDirectories)
    {
        if (baseDirectories != null) {
            ImmutableList.Builder<URI> builder = ImmutableList.builder();
            for (String baseDirectory : baseDirectories.split(",")) {
                if (!baseDirectory.endsWith(PATH_SEPARATOR)) {
                    // This is needed as URI's resolve method expects directories to end with '/'
                    baseDirectory += PATH_SEPARATOR;
                }
                builder.add(URI.create(baseDirectory));
            }
            this.baseDirectories = builder.build();
        }
        return this;
    }

    @NotNull
    public DataSize getMaxPageStorageSize()
    {
        return maxPageStorageSize;
    }

    @Config("exchange.max-page-storage-size")
    @ConfigDescription("Max storage size of a page written to a sink, including the page itself and its size represented as an int")
    public FileSystemExchangeConfig setMaxPageStorageSize(DataSize maxPageStorageSize)
    {
        this.maxPageStorageSize = maxPageStorageSize;
        return this;
    }

    @Min(0)
    public int getExchangeSinkBufferPoolMinSize()
    {
        return exchangeSinkBufferPoolMinSize;
    }

    @Config("exchange.sink-buffer-pool-min-size")
    public FileSystemExchangeConfig setExchangeSinkBufferPoolMinSize(int exchangeSinkBufferPoolMinSize)
    {
        this.exchangeSinkBufferPoolMinSize = exchangeSinkBufferPoolMinSize;
        return this;
    }

    @Min(2)
    public int getExchangeSinkBuffersPerPartition()
    {
        return exchangeSinkBuffersPerPartition;
    }

    @Config("exchange.sink-buffers-per-partition")
    public FileSystemExchangeConfig setExchangeSinkBuffersPerPartition(int exchangeSinkBuffersPerPartition)
    {
        this.exchangeSinkBuffersPerPartition = exchangeSinkBuffersPerPartition;
        return this;
    }

    @NotNull
    public DataSize getExchangeSinkMaxFileSize()
    {
        return exchangeSinkMaxFileSize;
    }

    @Config("exchange.sink-max-file-size")
    @ConfigDescription("Max size of files written by exchange sinks")
    public FileSystemExchangeConfig setExchangeSinkMaxFileSize(DataSize exchangeSinkMaxFileSize)
    {
        this.exchangeSinkMaxFileSize = exchangeSinkMaxFileSize;
        return this;
    }

    @Min(1)
    public int getExchangeSourceConcurrentReaders()
    {
        return exchangeSourceConcurrentReaders;
    }

    @Config("exchange.source-concurrent-readers")
    public FileSystemExchangeConfig setExchangeSourceConcurrentReaders(int exchangeSourceConcurrentReaders)
    {
        this.exchangeSourceConcurrentReaders = exchangeSourceConcurrentReaders;
        return this;
    }

    @Min(1)
    public int getExchangeSourceMaxFilesPerReader()
    {
        return exchangeSourceMaxFilesPerReader;
    }

    @Config("exchange.source-max-files-per-reader")
    public FileSystemExchangeConfig setExchangeSourceMaxFilesPerReader(int exchangeSourceMaxFilesPerReader)
    {
        this.exchangeSourceMaxFilesPerReader = exchangeSourceMaxFilesPerReader;
        return this;
    }

    @Min(1)
    public int getMaxOutputPartitionCount()
    {
        return maxOutputPartitionCount;
    }

    @Config("exchange.max-output-partition-count")
    public FileSystemExchangeConfig setMaxOutputPartitionCount(int maxOutputPartitionCount)
    {
        this.maxOutputPartitionCount = maxOutputPartitionCount;
        return this;
    }

    @Min(1)
    public int getExchangeFileListingParallelism()
    {
        return exchangeFileListingParallelism;
    }

    @Config("exchange.file-listing-parallelism")
    @ConfigDescription("Max parallelism of file listing calls when enumerating spooling files. The actual parallelism will depend on implementation")
    public FileSystemExchangeConfig setExchangeFileListingParallelism(int exchangeFileListingParallelism)
    {
        this.exchangeFileListingParallelism = exchangeFileListingParallelism;
        return this;
    }

    @NotNull
    public DataSize getExchangeSourceHandleTargetDataSize()
    {
        return exchangeSourceHandleTargetDataSize;
    }

    @Config("exchange.source-handle-target-data-size")
    @ConfigDescription("Target size of the data referenced by a single source handle")
    public FileSystemExchangeConfig setExchangeSourceHandleTargetDataSize(DataSize exchangeSourceHandleTargetDataSize)
    {
        this.exchangeSourceHandleTargetDataSize = exchangeSourceHandleTargetDataSize;
        return this;
    }
}
