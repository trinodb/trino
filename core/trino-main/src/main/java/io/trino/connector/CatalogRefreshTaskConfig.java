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
package io.trino.connector;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class CatalogRefreshTaskConfig
{
    private boolean enabled = true;
    private Duration refreshInterval = Duration.succinctDuration(60, TimeUnit.SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("catalog.refresh.enabled")
    public CatalogRefreshTaskConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @MinDuration("60s")
    @NotNull
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("catalog.refresh.refresh-interval")
    public CatalogRefreshTaskConfig setRefreshInterval(Duration interval)
    {
        this.refreshInterval = interval;
        return this;
    }
}
