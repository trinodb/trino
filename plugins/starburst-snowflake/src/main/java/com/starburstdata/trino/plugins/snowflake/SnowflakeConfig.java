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
    private boolean databasePrefixForSchemaEnabled;
    // disabled by default for some time
    private boolean experimentalPushdownEnabled;

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

    public boolean getDatabasePrefixForSchemaEnabled()
    {
        return databasePrefixForSchemaEnabled;
    }

    @Config("snowflake.database-prefix-for-schema.enabled")
    @ConfigDescription("Allow accessing other databases by prefixing schema name with the database name in queries")
    public SnowflakeConfig setDatabasePrefixForSchemaEnabled(boolean databasePrefixForSchemaEnabled)
    {
        this.databasePrefixForSchemaEnabled = databasePrefixForSchemaEnabled;
        return this;
    }

    public boolean isExperimentalPushdownEnabled()
    {
        return experimentalPushdownEnabled;
    }

    @Config("snowflake.experimental-pushdown.enabled")
    @ConfigDescription("Enable some experimental pushdowns")
    public SnowflakeConfig setExperimentalPushdownEnabled(boolean experimentalPushdownEnabled)
    {
        this.experimentalPushdownEnabled = experimentalPushdownEnabled;
        return this;
    }
}
