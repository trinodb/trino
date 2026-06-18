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
package io.trino.plugin.doris;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import jakarta.validation.constraints.Min;

import java.util.concurrent.TimeUnit;

public class DorisConfig
{
    private boolean autoReconnect = true;
    private int maxReconnects = 3;
    private boolean forceAggregationPushdown = true;
    private boolean forceTopNPushdown = true;
    private int datetimeColumnSize = 23;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    // Using `useInformationSchema=true` prevents race condition inside MySQL driver's java.sql.DatabaseMetaData.getColumns
    // implementation, which throw SQL exception when a table disappears during listing.
    // Using `useInformationSchema=false` may provide more diagnostic information (see https://github.com/trinodb/trino/issues/1597)
    private boolean driverUseInformationSchema = true;

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("doris.auto-reconnect")
    public DorisConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @Min(1)
    public int getMaxReconnects()
    {
        return maxReconnects;
    }

    @Config("doris.max-reconnects")
    public DorisConfig setMaxReconnects(int maxReconnects)
    {
        this.maxReconnects = maxReconnects;
        return this;
    }

    public int getDatetimeColumnSize()
    {
        return this.datetimeColumnSize;
    }

    @Config("doris.datetime-column-size")
    @ConfigDescription("Value of datetime columnSize for special db like doris")
    public DorisConfig setDatetimeColumnSize(int datetimeColumnSize)
    {
        this.datetimeColumnSize = datetimeColumnSize;
        return this;
    }

    public boolean isForceAggregationPushdown()
    {
        return this.forceAggregationPushdown;
    }

    @Config("doris.force-aggregation-pushdown")
    @ConfigDescription("Determines if missing information will be cached")
    public DorisConfig setForceAggregationPushdown(boolean forceAggregationPushdown)
    {
        this.forceAggregationPushdown = forceAggregationPushdown;
        return this;
    }

    public boolean isForceTopNPushdown()
    {
        return this.forceTopNPushdown;
    }

    @Config("doris.force-topn-pushdown")
    public DorisConfig setForceTopNPushdown(boolean forceTopNPushdown)
    {
        this.forceTopNPushdown = forceTopNPushdown;
        return this;
    }

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("doris.connection-timeout")
    public DorisConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }

    @Config("doris.jdbc.use-information-schema")
    @ConfigDescription("Value of useInformationSchema MySQL JDBC driver connection property")
    public DorisConfig setDriverUseInformationSchema(boolean driverUseInformationSchema)
    {
        this.driverUseInformationSchema = driverUseInformationSchema;
        return this;
    }
}
