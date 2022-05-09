/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.sqlserver.SqlServerConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

// This is separate from SqlServerSessionProperties because Synapse doesn't
// support table locking, so some properties there are not applicable.
// Instead, we just add the one property needed for Synapse.
public class StarburstSynapseSessionProperties
        implements SessionPropertiesProvider
{
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarburstSynapseSessionProperties(SqlServerConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        BULK_COPY_FOR_WRITE,
                        "Use SQL Server Bulk Copy API for writes",
                        config.isBulkCopyForWrite(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    /**
     * Note that certain data types are unsupported by SQL Server and prevent
     * bulk copies from being performed, see the <a href="https://docs.microsoft.com/en-us/sql/connect/jdbc/use-bulk-copy-api-batch-insert-operation?view=sql-server-ver15#known-limitations">
     * SQL Server docs</a>.
     */
    public static boolean isBulkCopyForWrite(ConnectorSession session)
    {
        return session.getProperty(BULK_COPY_FOR_WRITE, Boolean.class);
    }
}
