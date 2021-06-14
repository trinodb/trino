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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class StarburstCommonSqlServerSessionProperties
        implements SessionPropertiesProvider
{
    public static final String BULK_COPY_FOR_WRITE = "bulk_copy_for_write";
    // TODO https://starburstdata.atlassian.net/browse/SEP-6376
    public static final String NON_TRANSACTIONAL_INSERT = "non_transactional_insert";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public StarburstCommonSqlServerSessionProperties(StarburstCommonSqlServerConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        BULK_COPY_FOR_WRITE,
                        "Use SQL Server Bulk Copy API for writes",
                        config.isBulkCopyForWrite(),
                        false),
                booleanProperty(
                        NON_TRANSACTIONAL_INSERT,
                        "Write directly to the target table bypassing temporary table",
                        config.isNonTransactionalInsert(),
                        false));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isBulkCopyForWrite(ConnectorSession session)
    {
        return session.getProperty(BULK_COPY_FOR_WRITE, Boolean.class);
    }

    public static boolean isNonTransactionalInsert(ConnectorSession session)
    {
        return session.getProperty(NON_TRANSACTIONAL_INSERT, Boolean.class);
    }
}
