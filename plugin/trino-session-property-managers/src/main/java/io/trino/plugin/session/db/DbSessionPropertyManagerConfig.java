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
package io.trino.plugin.session.db;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DbSessionPropertyManagerConfig
{
    private String configDbUrl;
    private String username;
    private String password;
    private Duration maxRefreshInterval = new Duration(1, HOURS);
    private Duration refreshInterval = new Duration(1, SECONDS);
    private boolean runMigrationsEnabled = true;

    public String getConfigDbUrl()
    {
        return configDbUrl;
    }

    @Config("session-property-manager.config-db-url")
    public DbSessionPropertyManagerConfig setConfigDbUrl(String configDbUrl)
    {
        this.configDbUrl = configDbUrl;
        return this;
    }

    public String getConfigDbUser()
    {
        return username;
    }

    @Config("session-property-manager.config-db-user")
    @ConfigDescription("Database user name")
    public DbSessionPropertyManagerConfig setConfigDbUser(String username)
    {
        this.username = username;
        return this;
    }

    public String getConfigDbPassword()
    {
        return password;
    }

    @Config("session-property-manager.config-db-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Database password")
    public DbSessionPropertyManagerConfig setConfigDbPassword(String password)
    {
        this.password = password;
        return this;
    }

    @MinDuration("10s")
    public Duration getMaxRefreshInterval()
    {
        return maxRefreshInterval;
    }

    @Config("session-property-manager.max-refresh-interval")
    @ConfigDescription("Time period for which the cluster will continue to serve session properties after refresh failures cause configuration to become stale")
    public DbSessionPropertyManagerConfig setMaxRefreshInterval(Duration maxRefreshInterval)
    {
        this.maxRefreshInterval = maxRefreshInterval;
        return this;
    }

    @MinDuration("1s")
    public Duration getRefreshInterval()
    {
        return refreshInterval;
    }

    @Config("session-property-manager.refresh-interval")
    @ConfigDescription("How often the cluster reloads from the database")
    public DbSessionPropertyManagerConfig setRefreshInterval(Duration refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    public boolean isRunMigrationsEnabled()
    {
        return runMigrationsEnabled;
    }

    @Config("session-property-manager.db-migrations-enabled")
    @ConfigDescription("Whether to run migrations on startup")
    public DbSessionPropertyManagerConfig setRunMigrationsEnabled(boolean runMigrationsEnabled)
    {
        this.runMigrationsEnabled = runMigrationsEnabled;
        return this;
    }

    @AssertTrue(message = "maxRefreshInterval must be greater than refreshInterval")
    public boolean isRefreshIntervalValid()
    {
        return maxRefreshInterval.compareTo(refreshInterval) > 0;
    }
}
