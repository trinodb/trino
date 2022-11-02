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
package io.trino.plugin.hive.rubix;

import com.qubole.rubix.spi.CacheConfig;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;

public class RubixConfig
{
    public enum ReadMode
    {
        READ_THROUGH(false),
        ASYNC(true);

        private final boolean parallelWarmupEnabled;

        ReadMode(boolean parallelWarmupEnabled)
        {
            this.parallelWarmupEnabled = parallelWarmupEnabled;
        }

        public boolean isParallelWarmupEnabled()
        {
            return parallelWarmupEnabled;
        }

        public static ReadMode fromString(String value)
        {
            switch (value.toLowerCase(ENGLISH)) {
                case "async":
                    return ASYNC;
                case "read-through":
                    return READ_THROUGH;
            }

            throw new IllegalArgumentException(format("Unrecognized value: '%s'", value));
        }
    }

    private ReadMode readMode = ReadMode.ASYNC;
    private Optional<String> cacheLocation = Optional.empty();
    private Duration cacheTtl = new Duration(7, DAYS);
    private int diskUsagePercentage = CacheConfig.DEFAULT_DATA_CACHE_FULLNESS;
    private int bookKeeperServerPort = CacheConfig.DEFAULT_BOOKKEEPER_SERVER_PORT;
    private int dataTransferServerPort = CacheConfig.DEFAULT_DATA_TRANSFER_SERVER_PORT;
    private boolean startServerOnCoordinator;

    @NotNull
    public ReadMode getReadMode()
    {
        return readMode;
    }

    @Config("hive.cache.read-mode")
    public RubixConfig setReadMode(ReadMode readMode)
    {
        this.readMode = readMode;
        return this;
    }

    @NotNull
    public Optional<String> getCacheLocation()
    {
        return cacheLocation;
    }

    @Config("hive.cache.location")
    public RubixConfig setCacheLocation(String location)
    {
        this.cacheLocation = Optional.ofNullable(location);
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("hive.cache.ttl")
    @ConfigDescription("Time files will be kept in cache prior to eviction")
    public RubixConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @Min(0)
    @Max(100)
    public int getDiskUsagePercentage()
    {
        return diskUsagePercentage;
    }

    @Config("hive.cache.disk-usage-percentage")
    @ConfigDescription("Percentage of disk space used for cached data")
    public RubixConfig setDiskUsagePercentage(int diskUsagePercentage)
    {
        this.diskUsagePercentage = diskUsagePercentage;
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

    public boolean isStartServerOnCoordinator()
    {
        return startServerOnCoordinator;
    }

    @Config("hive.cache.start-server-on-coordinator")
    public RubixConfig setStartServerOnCoordinator(boolean startServerOnCoordinator)
    {
        this.startServerOnCoordinator = startServerOnCoordinator;
        return this;
    }
}
