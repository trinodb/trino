/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

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
