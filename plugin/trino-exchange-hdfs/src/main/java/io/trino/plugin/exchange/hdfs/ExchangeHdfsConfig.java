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
package io.trino.plugin.exchange.hdfs;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import javax.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeHdfsConfig
{
    private DataSize hdfsStorageBlockSize = DataSize.of(4, MEGABYTE);
    private Optional<String> hdfsProxyUser = Optional.empty();

    @NotNull
    @MinDataSize("4MB")
    @MaxDataSize("256MB")
    public DataSize getHdfsStorageBlockSize()
    {
        return hdfsStorageBlockSize;
    }

    @Config("exchange.hdfs.block-size")
    @ConfigDescription("Block size for HDFS storage")
    public ExchangeHdfsConfig setHdfsStorageBlockSize(DataSize hdfsStorageBlockSize)
    {
        this.hdfsStorageBlockSize = hdfsStorageBlockSize;
        return this;
    }

    public Optional<String> getHdfsProxyUser()
    {
        return hdfsProxyUser;
    }

    @Config("exchange.hdfs.proxy-user")
    @ConfigDescription("Proxy user for HDFS storage")
    public ExchangeHdfsConfig setHdfsProxyUser(String hdfsProxyUser)
    {
        this.hdfsProxyUser = Optional.ofNullable(hdfsProxyUser);
        return this;
    }
}
