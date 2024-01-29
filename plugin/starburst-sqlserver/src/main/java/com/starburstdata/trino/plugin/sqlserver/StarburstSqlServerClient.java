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

import com.google.inject.Inject;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.sqlserver.SqlServerClient;
import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Verify.verify;
import static com.starburstdata.trino.plugin.sqlserver.StarburstSqlServerSessionProperties.hasParallelism;

public class StarburstSqlServerClient
        extends SqlServerClient
{
    @Inject
    public StarburstSqlServerClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super(config, statisticsConfig, connectionFactory, queryBuilder, identifierMapping, queryModifier);
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
