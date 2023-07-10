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
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JdbcDynamicFilteringConfig
{
    private boolean dynamicFilteringEnabled = true;
    // 20s allows DF collection from dimensional tables as well as
    // some larger build side subqueries
    private Duration dynamicFilteringWaitTimeout = new Duration(20, SECONDS);

    public boolean isDynamicFilteringEnabled()
    {
        return dynamicFilteringEnabled;
    }

    @Config("dynamic-filtering.enabled")
    @ConfigDescription("Wait for dynamic filters before starting JDBC query")
    public JdbcDynamicFilteringConfig setDynamicFilteringEnabled(boolean dynamicFilteringEnabled)
    {
        this.dynamicFilteringEnabled = dynamicFilteringEnabled;
        return this;
    }

    @NotNull
    public Duration getDynamicFilteringWaitTimeout()
    {
        return dynamicFilteringWaitTimeout;
    }

    @Config("dynamic-filtering.wait-timeout")
    @ConfigDescription("Duration to wait for completion of dynamic filters")
    public JdbcDynamicFilteringConfig setDynamicFilteringWaitTimeout(Duration dynamicFilteringWaitTimeout)
    {
        this.dynamicFilteringWaitTimeout = dynamicFilteringWaitTimeout;
        return this;
    }
}
