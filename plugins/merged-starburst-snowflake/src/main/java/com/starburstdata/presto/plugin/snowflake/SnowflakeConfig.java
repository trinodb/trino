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
package com.starburstdata.presto.plugin.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.prestosql.spi.NonObfuscable;

import java.util.Optional;

@NonObfuscable
public class SnowflakeConfig
{
    private SnowflakeImpersonationType impersonationType = SnowflakeImpersonationType.NONE;
    private String warehouse;
    private String database;
    private String role;

    public SnowflakeImpersonationType getImpersonationType()
    {
        return impersonationType;
    }

    @Config("snowflake.impersonation-type")
    @ConfigDescription("User impersonation method in Snowflake")
    public SnowflakeConfig setImpersonationType(SnowflakeImpersonationType impersonationType)
    {
        this.impersonationType = impersonationType;
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return Optional.ofNullable(warehouse);
    }

    @Config("snowflake.warehouse")
    @ConfigDescription("Name of Snowflake warehouse to use")
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }

    public Optional<String> getDatabase()
    {
        return Optional.ofNullable(database);
    }

    @Config("snowflake.database")
    @ConfigDescription("Name of Snowflake database to use")
    public SnowflakeConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }

    public Optional<String> getRole()
    {
        return Optional.ofNullable(role);
    }

    @Config("snowflake.role")
    @ConfigDescription("Name of Snowflake role to use")
    public SnowflakeConfig setRole(String role)
    {
        this.role = role;
        return this;
    }
}
