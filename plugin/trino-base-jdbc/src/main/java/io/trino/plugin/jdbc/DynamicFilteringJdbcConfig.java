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

package io.trino.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class DynamicFilteringJdbcConfig
{
    private boolean enableDynamicFiltering;
    private Duration dynamicFilteringWaitTimeout = new Duration(0, TimeUnit.MINUTES);

    public boolean isEnableDynamicFiltering()
    {
        return enableDynamicFiltering;
    }

    @Config("dynamic-filtering.enabled")
    public DynamicFilteringJdbcConfig setEnableDynamicFiltering(boolean enableDynamicFiltering)
    {
        this.enableDynamicFiltering = enableDynamicFiltering;
        return this;
    }

    @NotNull
    @MinDuration("0ms")
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public DynamicFilteringJdbcConfig setDynamicFilteringWaitTimeout(Duration timeout)
    {
        this.dynamicFilteringWaitTimeout = timeout;
        return this;
    }
}
