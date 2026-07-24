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
package io.trino.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class AutoAnalyzeConfig
{
    private boolean enabled;
    private Duration checkInterval = new Duration(10, TimeUnit.MINUTES);
    private long smallTableThreshold = 100_000;
    private Duration smallTableInterval = new Duration(1, TimeUnit.HOURS);
    private Duration largeTableInterval = new Duration(24, TimeUnit.HOURS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("auto-analyze.enabled")
    @ConfigDescription("Enable automatic background ANALYZE for all tables")
    public AutoAnalyzeConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Duration getCheckInterval()
    {
        return checkInterval;
    }

    @Config("auto-analyze.check-interval")
    @ConfigDescription("How often to scan tables and submit ANALYZE for stale ones")
    public AutoAnalyzeConfig setCheckInterval(Duration checkInterval)
    {
        this.checkInterval = checkInterval;
        return this;
    }

    @Min(0)
    public long getSmallTableThreshold()
    {
        return smallTableThreshold;
    }

    @Config("auto-analyze.small-table-threshold")
    @ConfigDescription("Tables with fewer rows than this threshold are considered small tables")
    public AutoAnalyzeConfig setSmallTableThreshold(long smallTableThreshold)
    {
        this.smallTableThreshold = smallTableThreshold;
        return this;
    }

    @NotNull
    public Duration getSmallTableInterval()
    {
        return smallTableInterval;
    }

    @Config("auto-analyze.small-table-interval")
    @ConfigDescription("Minimum time between ANALYZE runs for small tables (fewer than small-table-threshold rows)")
    public AutoAnalyzeConfig setSmallTableInterval(Duration smallTableInterval)
    {
        this.smallTableInterval = smallTableInterval;
        return this;
    }

    @NotNull
    public Duration getLargeTableInterval()
    {
        return largeTableInterval;
    }

    @Config("auto-analyze.large-table-interval")
    @ConfigDescription("Minimum time between ANALYZE runs for large tables (at least small-table-threshold rows)")
    public AutoAnalyzeConfig setLargeTableInterval(Duration largeTableInterval)
    {
        this.largeTableInterval = largeTableInterval;
        return this;
    }
}
