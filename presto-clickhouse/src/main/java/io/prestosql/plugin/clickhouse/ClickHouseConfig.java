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
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class ClickHouseConfig
{
    private static final Logger log = Logger.get(ClickHouseConfig.class);

    private String connectionurl;
    private String user;
    private String password;
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    // Using `useInformationSchema=true` prevents race condition inside clickhouse driver's java.sql.DatabaseMetaData.getColumns
    // implementation, which throw SQL exception when a table disappears during listing.
    // Using `useInformationSchema=false` may provide more diagnostic information (see https://github.com/prestosql/presto/issues/1597)
    private boolean driverUseInformationSchema = true;

    @NotNull
    public String getConnectionUrl()
    {
        return connectionurl;
    }

    @Config("connection-url")
    public ClickHouseConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionurl = connectionUrl;
        return this;
    }

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

    @NotNull
    public String getConnectionuser()
    {
        return user;
    }

    @Config("connection-user")
    public ClickHouseConfig setConnectionuser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public String getConnectionpassword()
    {
        return password;
    }

    @Config("connection-password")
    public ClickHouseConfig setConnectionpassword(String password)
    {
        this.password = password;
        return this;
    }
}
