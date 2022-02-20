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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class FileSystemExchangeConfig
{
    private String baseDirectory;
    private boolean exchangeEncryptionEnabled;
    private int exchangeSinkBufferPoolMinSize;

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
}
