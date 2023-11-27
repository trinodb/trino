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
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

public class QueryConfig
{
    private boolean reuseConnection = true;
    private boolean keepAlive;
    private Duration keepAliveInterval = Duration.valueOf("30s");

    public boolean isReuseConnection()
    {
        return reuseConnection;
    }

    @Config("query.reuse-connection")
    @ConfigDescription("Enables reusing JDBC connection for metadata queries to data source within a single Trino query")
    public QueryConfig setReuseConnection(boolean reuseConnection)
    {
        this.reuseConnection = reuseConnection;
        return this;
    }

    public boolean isKeepAlive()
    {
        return keepAlive;
    }

    @Config("query.keep-alive-connection")
    @ConfigDescription("Enables JDBC connection watchdog to ensure that connection won't be closed by the database when idle")
    public QueryConfig setKeepAlive(boolean keepAlive)
    {
        this.keepAlive = keepAlive;
        return this;
    }

    @MinDuration("5s")
    @MaxDuration("1h")
    public Duration getKeepAliveInterval()
    {
        return keepAliveInterval;
    }

    @Config("query.keep-alive-interval")
    @ConfigDescription("Interval to check for JDBC connection validity")
    public QueryConfig setKeepAliveInterval(Duration keepAliveInterval)
    {
        this.keepAliveInterval = keepAliveInterval;
        return this;
    }
}
