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
package io.trino.plugin.exchange.filesystem.azure;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeAzureConfig
{
    private Optional<String> azureStorageConnectionString = Optional.empty();
    private DataSize azureStorageBlockSize = DataSize.of(4, MEGABYTE);
    private int maxErrorRetries = 10;

    public Optional<String> getAzureStorageConnectionString()
    {
        return azureStorageConnectionString;
    }

    @Config("exchange.azure.connection-string")
    @ConfigSecuritySensitive
    public ExchangeAzureConfig setAzureStorageConnectionString(String azureStorageConnectionString)
    {
        this.azureStorageConnectionString = Optional.ofNullable(azureStorageConnectionString);
        return this;
    }

    @NotNull
    @MinDataSize("4MB")
    @MaxDataSize("256MB")
    public DataSize getAzureStorageBlockSize()
    {
        return azureStorageBlockSize;
    }

    @Config("exchange.azure.block-size")
    @ConfigDescription("Block size for Azure High-Throughput Block Blob")
    public ExchangeAzureConfig setAzureStorageBlockSize(DataSize azureStorageBlockSize)
    {
        this.azureStorageBlockSize = azureStorageBlockSize;
        return this;
    }

    @Min(0)
    public int getMaxErrorRetries()
    {
        return maxErrorRetries;
    }

    @Config("exchange.azure.max-error-retries")
    public ExchangeAzureConfig setMaxErrorRetries(int maxErrorRetries)
    {
        this.maxErrorRetries = maxErrorRetries;
        return this;
    }
}
