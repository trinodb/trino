/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.distributed;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Provider;
import com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SnowflakeSplitManager
        implements ConnectorSplitManager
{
    private final Provider<Connector> connector;
    private final SnowflakeClient client;
    private final ListeningExecutorService executorService;
    private final TypeManager typeManager;
    private final SnowflakeConnectionManager connectionManager;
    private final SnowflakeDistributedConfig config;
    private final SnowflakeExportStats exportStats;

    @Inject
    public SnowflakeSplitManager(
            Provider<Connector> connector,
            SnowflakeClient client,
            ListeningExecutorService executorService,
            TypeManager typeManager,
            SnowflakeConnectionManager connectionManager,
            SnowflakeDistributedConfig config,
            SnowflakeExportStats exportStats)
    {
        this.connector = requireNonNull(connector, "connector is null");
        this.client = requireNonNull(client, "client is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.connectionManager = requireNonNull(connectionManager, "connectionManager is null");
        this.config = requireNonNull(config, "config is null");
        this.exportStats = requireNonNull(exportStats, "exportStats is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) table;
        List<JdbcColumnHandle> columns = jdbcTableHandle.getColumns()
                .orElseGet(() -> connector.get().getMetadata(session, transaction)
                        .getColumnHandles(session, table)
                        .values().stream()
                        .map(JdbcColumnHandle.class::cast)
                        .collect(toImmutableList()));
        return new SnowflakeSplitSource(
                executorService,
                typeManager,
                client,
                session,
                jdbcTableHandle,
                columns,
                connectionManager,
                config,
                exportStats);
    }
}
