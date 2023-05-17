/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.ptf.ConnectorTableFunction;

import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SnowflakeParallelConnector
        extends JdbcConnector
{
    private final ConnectorPageSourceProvider connectorPageSourceProvider;

    @Inject
    public SnowflakeParallelConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorRecordSetProvider jdbcRecordSetProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            // used by the parent connector class
            @SuppressWarnings("TrinoExperimentalSpi") Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            JdbcTransactionManager transactionManager,
            ConnectorPageSourceProvider connectorPageSourceProvider)
    {
        super(
                lifeCycleManager,
                jdbcSplitManager,
                jdbcRecordSetProvider,
                jdbcPageSinkProvider,
                accessControl,
                procedures,
                connectorTableFunctions,
                sessionProperties,
                tableProperties,
                transactionManager);
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider, "connectorPageSourceProvider is null");
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return connectorPageSourceProvider;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        throw new UnsupportedOperationException();
    }
}
