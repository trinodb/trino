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
package io.prestosql.plugin.clickhouse;

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import java.util.concurrent.TimeUnit;

public class ClickHouseConfig
{
    private String connectionurl;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    // Using `useInformationSchema=true` prevents race condition inside clickhouse driver's java.sql.DatabaseMetaData.getColumns
    // implementation, which throw SQL exception when a table disappears during listing.
    // Using `useInformationSchema=false` may provide more diagnostic information (see https://github.com/prestosql/presto/issues/1597)
    private boolean driverUseInformationSchema = true;

    private boolean includeSystemTables;

    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connection-timeout")
    public ClickHouseConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isDriverUseInformationSchema()
    {
        return driverUseInformationSchema;
    }
}
