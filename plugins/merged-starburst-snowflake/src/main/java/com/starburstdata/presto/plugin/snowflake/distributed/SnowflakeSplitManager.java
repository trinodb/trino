/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

class SnowflakeSplitManager
        implements ConnectorSplitManager
{
    private final JdbcClient client;
    private final ListeningExecutorService executorService;
    private final TypeManager typeManager;
    private final QueryBuilder queryBuilder;
    private final SnowflakeConnectionManager connectionManager;
    private final SnowflakeDistributedConfig config;
    private final SnowflakeExportStats exportStats;

    @Inject
    public SnowflakeSplitManager(
            JdbcClient client,
            ListeningExecutorService executorService,
            TypeManager typeManager,
            QueryBuilder queryBuilder,
            SnowflakeConnectionManager connectionManager,
            SnowflakeDistributedConfig config,
            SnowflakeExportStats exportStats)
    {
        this.client = requireNonNull(client, "client is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.queryBuilder = requireNonNull(queryBuilder, "queryBuilder is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.config = requireNonNull(config, "config is null");
        this.exportStats = requireNonNull(exportStats, "exportStats is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            List<ColumnHandle> columns)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;
        return new SnowflakeSplitSource(
                executorService,
                typeManager,
                client,
                queryBuilder,
                session,
                jdbcTableHandle,
                columns.stream()
                        .map(column -> (JdbcColumnHandle) column)
                        .collect(toImmutableList()),
                connectionManager,
                config,
                exportStats);
    }
}
