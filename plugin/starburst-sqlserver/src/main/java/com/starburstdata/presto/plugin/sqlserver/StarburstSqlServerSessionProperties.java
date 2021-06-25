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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

public final class StarburstSqlServerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String OVERRIDE_CATALOG = "override_catalog";
    public static final String BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE = "bulk_copy_for_write_lock_destination_table";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarburstSqlServerSessionProperties(StarburstSqlServerConfig config)
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
                booleanProperty(
                        BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE,
                        "Obtain a Bulk Update lock on destination table on write",
                        config.isBulkCopyForWriteLockDestinationTable(),
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

    public static boolean isBulkCopyForWriteLockDestinationTable(ConnectorSession session)
    {
        return session.getProperty(BULK_COPY_FOR_WRITE_LOCK_DESTINATION_TABLE, Boolean.class);
    }
}
