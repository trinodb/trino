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
package io.trino.exchange;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import java.io.File;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.HOURS;

public class ExchangeManagerConfig
{
    private Optional<File> exchangeManagerConfigFile = Optional.empty();
    private boolean orphanCleanupEnabled;
    private Duration orphanCleanupInterval = new Duration(4, HOURS);
    private Duration orphanCleanupRetention = new Duration(12, HOURS);

    public Optional<@FileExists File> getExchangeManagerConfigFile()
    {
        return exchangeManagerConfigFile;
    }

    @Config("exchange-manager.config-file")
    public ExchangeManagerConfig setExchangeManagerConfigFile(File exchangeManagerConfigFile)
    {
        this.exchangeManagerConfigFile = Optional.ofNullable(exchangeManagerConfigFile);
        return this;
    }

    public boolean isOrphanCleanupEnabled()
    {
        return orphanCleanupEnabled;
    }

    @Config("exchange-manager.orphan-cleanup.enabled")
    @ConfigDescription("Enables the orphan cleanup job")
    public ExchangeManagerConfig setOrphanCleanupEnabled(boolean orphanCleanupEnabled)
    {
        this.orphanCleanupEnabled = orphanCleanupEnabled;
        return this;
    }

    @MinDuration("1s")
    public Duration getOrphanCleanupInterval()
    {
        return orphanCleanupInterval;
    }

    @Config("exchange-manager.orphan-cleanup.interval")
    @ConfigDescription("Time to wait between successive cleanup runs. Must be at least `1s`")
    public ExchangeManagerConfig setOrphanCleanupInterval(Duration orphanCleanupInterval)
    {
        this.orphanCleanupInterval = orphanCleanupInterval;
        return this;
    }

    @MinDuration("1s")
    public Duration getOrphanCleanupRetention()
    {
        return orphanCleanupRetention;
    }

    @Config("exchange-manager.orphan-cleanup.retention")
    @ConfigDescription("Minimum age of an exchange directory before it becomes eligible for deletion. Must be at least `1s`.")
    public ExchangeManagerConfig setOrphanCleanupRetention(Duration orphanCleanupRetention)
    {
        this.orphanCleanupRetention = orphanCleanupRetention;
        return this;
    }
}
