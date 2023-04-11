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

import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcPageSinkProvider;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.immutableEnumSet;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class SnowflakeDistributedConnector
        implements Connector
{
    private static final Logger log = Logger.get(SnowflakeDistributedConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final JdbcPageSinkProvider jdbcPageSinkProvider;
    private final JdbcTransactionManager transactionManager;
    private final Optional<ConnectorAccessControl> accessControl;
    private final Set<Procedure> procedures;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SnowflakeDistributedConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            JdbcPageSinkProvider jdbcPageSinkProvider,
            JdbcTransactionManager transactionManager,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<SessionPropertiesProvider> sessionPropertiesProviders)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.jdbcPageSinkProvider = requireNonNull(jdbcPageSinkProvider, "jdbcPageSinkProvider is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.procedures = ImmutableSet.copyOf(requireNonNull(procedures, "procedures is null"));
        this.sessionProperties = requireNonNull(sessionPropertiesProviders, "sessionPropertiesProviders is null").stream()
                .flatMap(provider -> provider.getSessionProperties().stream())
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return transactionManager.beginTransaction(isolationLevel, readOnly, autoCommit);
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return transactionManager.getMetadata(transactionHandle);
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        transactionManager.commit(transaction);
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        transactionManager.rollback(transaction);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return jdbcPageSinkProvider;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl.orElseThrow(UnsupportedOperationException::new);
    }

    @Override
    public Set<Procedure> getProcedures()
    {
        return procedures;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }

    @Override
    public Set<ConnectorCapabilities> getCapabilities()
    {
        return immutableEnumSet(NOT_NULL_COLUMN_CONSTRAINT);
    }
}
