/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

public class SnowflakeConfig
{
    private Optional<String> warehouse = Optional.empty();
    private Optional<String> database = Optional.empty();
    private Optional<String> role = Optional.empty();

    public Optional<String> getWarehouse()
    {
        return warehouse;
    }

    @Config("snowflake.warehouse")
    @ConfigDescription("Name of Snowflake warehouse to use")
    public SnowflakeConfig setWarehouse(String warehouse)
    {
        this.warehouse = Optional.ofNullable(warehouse);
        return this;
    }

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
}
