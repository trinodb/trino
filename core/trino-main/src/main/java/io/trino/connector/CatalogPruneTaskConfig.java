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

import static java.util.concurrent.TimeUnit.SECONDS;

public class CatalogPruneTaskConfig
{
    private boolean enabled = true;
    private Duration updateInterval = new Duration(5, SECONDS);

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("catalog.prune.enabled")
    public CatalogPruneTaskConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @MinDuration("1s")
    @NotNull
    public Duration getUpdateInterval()
    {
        return updateInterval;
    }

    @Config("catalog.prune.update-interval")
    public CatalogPruneTaskConfig setUpdateInterval(Duration interval)
    {
        this.updateInterval = interval;
        return this;
    }
}
