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
package io.trino.plugin.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class SnowflakeConfig
{
    private Optional<String> database = Optional.empty();
    private Optional<String> role = Optional.empty();
    private Optional<String> warehouse = Optional.empty();

    public Optional<String> getDatabase()
    {
        return database;
    }

    @Config("snowflake.database")
    @ConfigDescription("Name of Snowflake database to use")
    public SnowflakeConfig setDatabase(String database)
    {
        this.database = Optional.ofNullable(database);
        return this;
    }

    public Optional<String> getRole()
    {
        return role;
    }

    @Config("snowflake.role")
    @ConfigDescription("Name of Snowflake role to use")
    public SnowflakeConfig setRole(String role)
    {
        this.role = Optional.ofNullable(role);
        return this;
    }

    public Optional<String> getWarehouse()
    {
        return this.warehouse;
    }

    @Config("snowflake.warehouse")
    @ConfigDescription("Name of Snowflake warehouse to use")
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = Optional.ofNullable(warehouse);
        return this;
    }
}
