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
package io.trino.plugin.resourcegroups.db;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.AssertTrue;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DbResourceGroupConfig
{
    private String configUrl;
    private String user;
    private String password;
    private boolean exactMatchSelectorEnabled;
    private Duration maxRefreshInterval = new Duration(1, HOURS);
    private Duration refreshInterval = new Duration(1, SECONDS);

    public String getConfigDbUrl()
    {
        return configUrl;
    }

    @Config("resource-groups.config-db-url")
    public DbResourceGroupConfig setConfigDbUrl(String configUrl)
    {
        this.configUrl = configUrl;
        return this;
    }

    public String getConfigDbUser()
    {
        return user;
    }

    @Config("resource-groups.config-db-user")
    @ConfigDescription("Database user name")
    public DbResourceGroupConfig setConfigDbUser(String configUser)
    {
        this.user = configUser;
        return this;
    }

    public String getConfigDbPassword()
    {
        return password;
    }

    @ConfigSecuritySensitive
    @Config("resource-groups.config-db-password")
    @ConfigDescription("Database password")
    public DbResourceGroupConfig setConfigDbPassword(String configPassword)
    {
        this.password = configPassword;
        return this;
    }

    @MinDuration("10s")
    public Duration getMaxRefreshInterval()
    {
        return maxRefreshInterval;
    }

    @Config("resource-groups.max-refresh-interval")
    @ConfigDescription("Time period for which the cluster will continue to accept queries after refresh failures cause configuration to become stale")
    public DbResourceGroupConfig setMaxRefreshInterval(Duration maxRefreshInterval)
    {
        this.maxRefreshInterval = maxRefreshInterval;
        return this;
    }

    @MinDuration("1s")
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("resource-groups.refresh-interval")
    @ConfigDescription("How often the cluster reloads from the database")
    public DbResourceGroupConfig setRefreshInterval(Duration refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    public boolean getExactMatchSelectorEnabled()
    {
        return exactMatchSelectorEnabled;
    }

    @Config("resource-groups.exact-match-selector-enabled")
    public DbResourceGroupConfig setExactMatchSelectorEnabled(boolean exactMatchSelectorEnabled)
    {
        this.exactMatchSelectorEnabled = exactMatchSelectorEnabled;
        return this;
    }

    @AssertTrue(message = "maxRefreshInterval must be greater than refreshInterval")
    public boolean isRefreshIntervalValid()
    {
        return maxRefreshInterval.compareTo(refreshInterval) > 0;
    }
}
