/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.starburstdata.presto.license.LicenseManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class StarburstSqlServerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String OVERRIDE_CATALOG = "override_catalog";
    public static final String PARALLEL_CONNECTIONS_COUNT = "parallel_connections_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarburstSqlServerSessionProperties(LicenseManager licenseManager, StarburstSqlServerConfig config)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        OVERRIDE_CATALOG,
                        "Override SQL Server catalog name",
                        config.getOverrideCatalogName().orElse(null),
                        value -> {
                            if (!config.isOverrideCatalogEnabled()) {
                                throw new TrinoException(PERMISSION_DENIED, "Catalog override is disabled");
                            }
                        },
                        true),
                integerProperty(
                        PARALLEL_CONNECTIONS_COUNT,
                        "Maximum number of splits for a table scan up to number of table partitions",
                        config.getConnectionsCount(),
                        value -> {
                            if (value > 1) {
                                licenseManager.checkLicense();
                            }
                        },
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<String> getOverrideCatalog(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(OVERRIDE_CATALOG, String.class));
    }

    public static int getConnectionsCount(ConnectorSession session)
    {
        return session.getProperty(PARALLEL_CONNECTIONS_COUNT, Integer.class);
    }

    public static boolean hasParallelism(ConnectorSession session)
    {
        return getConnectionsCount(session) > 1;
    }
}
