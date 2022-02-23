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
package io.trino.plugin.exchange;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class FileSystemExchangeConfig
{
    private String baseDirectory;
    private boolean exchangeEncryptionEnabled;
    // For S3, we make read requests aligned with part boundaries. Incomplete slice at the end of the buffer is
    // possible and will be copied to the beginning of the new buffer, and we need to make room for that.
    // Therefore, it's recommended to set `maxPageStorageSize` to be slightly larger than a multiple of part size.
    private DataSize maxPageStorageSize = DataSize.of(16, MEGABYTE);
    private int exchangeSinkBufferPoolMinSize = 10;
    private int exchangeSourceConcurrentReaders = 4;

    @NotNull
    public String getBaseDirectory()
    {
        return baseDirectory;
    }

    @Config("exchange.base-directory")
    public FileSystemExchangeConfig setBaseDirectory(String baseDirectory)
    {
        this.baseDirectory = baseDirectory;
        return this;
    }

    public boolean isExchangeEncryptionEnabled()
    {
        return exchangeEncryptionEnabled;
    }

    @Config("exchange.encryption-enabled")
    public FileSystemExchangeConfig setExchangeEncryptionEnabled(boolean exchangeEncryptionEnabled)
    {
        this.exchangeEncryptionEnabled = exchangeEncryptionEnabled;
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
}
