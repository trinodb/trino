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
package com.starburstdata.presto.plugin.oracle;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class OracleConnectionPoolingConfig
{
    private int maxPoolSize = 30;
    private int minPoolSize = 1;
    private Duration inactiveConnectionTimeout = new Duration(20, MINUTES);

    @Min(1)
    public int getMaxPoolSize()
    {
        return maxPoolSize;
    }

    @Config("oracle.connection-pool.max-size")
    @ConfigDescription("Maximum size of JDBC connection poll")
    public OracleConnectionPoolingConfig setMaxPoolSize(int maxPoolSize)
    {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    @Min(0)
    public int getMinPoolSize()
    {
        return minPoolSize;
    }

    @Config("oracle.connection-pool.min-size")
    @ConfigDescription("Minimum size of JDBC connection poll")
    public OracleConnectionPoolingConfig setMinPoolSize(int minPoolSize)
    {
        this.minPoolSize = minPoolSize;
        return this;
    }

    @NotNull
    public Duration getInactiveConnectionTimeout()
    {
        return inactiveConnectionTimeout;
    }

    @Config("oracle.connection-pool.inactive-timeout")
    @ConfigDescription("How long a connection in the pool can remain idle before it is closed")
    public OracleConnectionPoolingConfig setInactiveConnectionTimeout(Duration inactiveConnectionTimeout)
    {
        this.inactiveConnectionTimeout = inactiveConnectionTimeout;
        return this;
    }

    @AssertTrue(message = "Max pool size must be greater or equal than min size")
    public boolean isPoolSizedProperly()
    {
        return getMaxPoolSize() >= getMinPoolSize();
    }
}
