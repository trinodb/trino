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
package io.trino.plugin.exchange.filesystem.alluxio;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeAlluxioConfig
{
    private DataSize alluxioStorageBlockSize = DataSize.of(4, MEGABYTE);
    private Optional<String> alluxioSiteConfPath = Optional.empty();

    @NotNull
    @MinDataSize("4MB")
    @MaxDataSize("256MB")
    public DataSize getAlluxioStorageBlockSize()
    {
        return alluxioStorageBlockSize;
    }

    @Config("exchange.alluxio.block-size")
    @ConfigDescription("Block size for Alluxio storage")
    public ExchangeAlluxioConfig setAlluxioStorageBlockSize(DataSize alluxioStorageBlockSize)
    {
        this.alluxioStorageBlockSize = alluxioStorageBlockSize;
        return this;
    }

    public Optional<@FileExists String> getAlluxioSiteConfPath()
    {
        return alluxioSiteConfPath;
    }

    @Config("exchange.alluxio.site-file-path")
    @ConfigDescription("Path to the alluxio site file that contains your custom configuration")
    public ExchangeAlluxioConfig setAlluxioSiteConfPath(String path)
    {
        this.alluxioSiteConfPath = Optional.ofNullable(path);
        return this;
    }
}
