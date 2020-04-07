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
package io.prestosql.plugin.hive.rubix;

import com.qubole.rubix.spi.CacheConfig;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class RubixConfig
{
    private boolean parallelWarmupEnabled = true;
    private String cacheLocation;
    private int bookKeeperServerPort = CacheConfig.DEFAULT_BOOKKEEPER_SERVER_PORT;
    private int dataTransferServerPort = CacheConfig.DEFAULT_DATA_TRANSFER_SERVER_PORT;

    public boolean isParallelWarmupEnabled()
    {
        return parallelWarmupEnabled;
    }

    @Config("hive.cache.parallel-warmup-enabled")
    public RubixConfig setParallelWarmupEnabled(boolean value)
    {
        this.parallelWarmupEnabled = value;
        return this;
    }

    @NotNull
    public String getCacheLocation()
    {
        return cacheLocation;
    }

    @Config("hive.cache.location")
    public RubixConfig setCacheLocation(String location)
    {
        this.cacheLocation = location;
        return this;
    }

    public int getBookKeeperServerPort()
    {
        return bookKeeperServerPort;
    }

    @Config("hive.cache.bookkeeper-port")
    public RubixConfig setBookKeeperServerPort(int port)
    {
        this.bookKeeperServerPort = port;
        return this;
    }

    public int getDataTransferServerPort()
    {
        return dataTransferServerPort;
    }

    @Config("hive.cache.data-transfer-port")
    public RubixConfig setDataTransferServerPort(int port)
    {
        this.dataTransferServerPort = port;
        return this;
    }
}
