/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.sqlserver;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.Optional;

import static com.google.common.base.Verify.verify;

public class StarburstSqlServerConfig
{
    static final String SQLSERVER_OVERRIDE_CATALOG_NAME = "sqlserver.override-catalog.name";
    static final String SQLSERVER_OVERRIDE_CATALOG_ENABLED = "sqlserver.override-catalog.enabled";

    private boolean overrideCatalogEnabled;
    @Nullable
    private String overrideCatalogName;
    private boolean databasePrefixForSchemaEnabled;
    private int connectionsCount = 1;

    public boolean isOverrideCatalogEnabled()
    {
        return overrideCatalogEnabled;
    }

    @Config(SQLSERVER_OVERRIDE_CATALOG_ENABLED)
    @ConfigDescription("Enable catalog override using system property")
    public StarburstSqlServerConfig setOverrideCatalogEnabled(boolean overrideCatalogEnabled)
    {
        this.overrideCatalogEnabled = overrideCatalogEnabled;
        return this;
    }

    public Optional<String> getOverrideCatalogName()
    {
        return Optional.ofNullable(overrideCatalogName);
    }

    @Config(SQLSERVER_OVERRIDE_CATALOG_NAME)
    @ConfigDescription("Name of the SQL server catalog to use as a catalog override")
    public StarburstSqlServerConfig setOverrideCatalogName(@Nullable String overrideCatalogName)
    {
        this.overrideCatalogName = overrideCatalogName;
        return this;
    }

    @Min(1)
    @Max(1024)
    public int getConnectionsCount()
    {
        return connectionsCount;
    }

    @Config("sqlserver.parallel.connections-count")
    @ConfigDescription("Number of parallel connections, 1 - no parallelism, N - up to number of database partitions")
    public StarburstSqlServerConfig setConnectionsCount(int connectionsCount)
    {
        this.connectionsCount = connectionsCount;
        return this;
    }

    public boolean getDatabasePrefixForSchemaEnabled()
    {
        return databasePrefixForSchemaEnabled;
    }

    @Config("sqlserver.database-prefix-for-schema.enabled")
    @ConfigDescription("Allow accessing other databases by prefixing schema name with the database name in queries")
    public StarburstSqlServerConfig setDatabasePrefixForSchemaEnabled(boolean databasePrefixForSchemaEnabled)
    {
        this.databasePrefixForSchemaEnabled = databasePrefixForSchemaEnabled;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        verify(
                overrideCatalogEnabled || overrideCatalogName == null || overrideCatalogName.isBlank(),
                SQLSERVER_OVERRIDE_CATALOG_ENABLED + " needs to be set in order to use " + SQLSERVER_OVERRIDE_CATALOG_NAME + " parameter");
    }
}
