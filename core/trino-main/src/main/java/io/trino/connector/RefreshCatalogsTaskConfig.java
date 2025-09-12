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

import static java.util.concurrent.TimeUnit.MINUTES;

public class RefreshCatalogsTaskConfig
{
    private boolean enabled; // Disabled by default
    private Duration refreshInterval = new Duration(5, MINUTES);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("catalog.refresh.enabled")
    public RefreshCatalogsTaskConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("catalog.refresh.interval")
    public RefreshCatalogsTaskConfig setRefreshInterval(Duration interval)
    {
        this.refreshInterval = interval;
        return this;
    }
}
