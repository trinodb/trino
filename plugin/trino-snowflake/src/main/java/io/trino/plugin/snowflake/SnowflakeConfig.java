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

import java.util.Optional;

public class SnowflakeConfig
{
    private String account;
    private String database;
    private String role;
    private String warehouse = "";

    public Optional<String> getAccount()
    {
        return Optional.ofNullable(account);
    }

    @Config("snowflake.account")
    public SnowflakeConfig setAccount(String account)
    {
        this.account = account;
        return this;
    }

    public Optional<String> getDatabase()
    {
        return Optional.ofNullable(database);
    }

    @Config("snowflake.database")
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
    public SnowflakeConfig setRole(String role)
    {
        this.role = role;
        return this;
    }

    public String getWarehouse()
    {
        return warehouse;
    }

    @Config("snowflake.warehouse")
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = warehouse;
        return this;
    }
}
