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

import com.google.inject.Inject;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.sqlserver.SqlServerClient;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.TableScanRedirectApplicationResult;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.hasParallelism;
import static java.util.Objects.requireNonNull;

public class StarburstSqlServerClient
        extends SqlServerClient
{
    private final TableScanRedirection tableScanRedirection;

    @Inject
    public StarburstSqlServerClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            TableScanRedirection tableScanRedirection,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, statisticsConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);
        this.tableScanRedirection = requireNonNull(tableScanRedirection, "tableScanRedirection is null");
    }

    @Override
    public Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle handle)
    {
        return tableScanRedirection.getTableScanRedirection(session, handle, this);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        verify(super.isLimitGuaranteed(session), "Super implementation changed");
        return !hasParallelism(session);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        verify(super.isTopNGuaranteed(session), "Super implementation changed");
        return !hasParallelism(session);
    }
}
