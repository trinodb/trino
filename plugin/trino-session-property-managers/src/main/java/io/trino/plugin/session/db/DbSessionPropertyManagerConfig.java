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
import io.airlift.configuration.LegacyConfig;
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
    private Duration connectTimeout = new Duration(10, SECONDS);
    private Duration socketTimeout = new Duration(30, SECONDS);
    private boolean runMigrationsEnabled = true;

    public String getConfigDbUrl()
    {
        return configDbUrl;
    }

    @Config("session-property-manager.config-db-url")
    @ConfigSecuritySensitive
    public DbSessionPropertyManagerConfig setConfigDbUrl(String configDbUrl)
    {
        this.configDbUrl = configDbUrl;
        return this;
    }

    @Deprecated
    @LegacyConfig(value = "session-property-manager.db.url", replacedBy = "session-property-manager.config-db-url")
    public DbSessionPropertyManagerConfig setDbUrl(String configDbUrl)
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

    @Deprecated
    @LegacyConfig(value = "session-property-manager.db.username", replacedBy = "session-property-manager.config-db-user")
    public DbSessionPropertyManagerConfig setDbUsername(String username)
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

    @Deprecated
    @LegacyConfig(value = "session-property-manager.db.password", replacedBy = "session-property-manager.config-db-password")
    public DbSessionPropertyManagerConfig setDbPassword(String password)
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

    @Deprecated
    @LegacyConfig(value = "session-property-manager.db.refresh-period", replacedBy = "session-property-manager.refresh-interval")
    public DbSessionPropertyManagerConfig setDbRefreshPeriod(Duration refreshInterval)
    {
        this.refreshInterval = refreshInterval;
        return this;
    }

    @MinDuration("1s")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("session-property-manager.config-db-connect-timeout")
    @ConfigDescription("Timeout for establishing a connection to the database")
    public DbSessionPropertyManagerConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getSocketTimeout()
    {
        return socketTimeout;
    }

    @Config("session-property-manager.config-db-socket-timeout")
    @ConfigDescription("Timeout for reading from the database connection; a refresh that exceeds it fails and is retried on the next refresh")
    public DbSessionPropertyManagerConfig setSocketTimeout(Duration socketTimeout)
    {
        this.socketTimeout = socketTimeout;
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
