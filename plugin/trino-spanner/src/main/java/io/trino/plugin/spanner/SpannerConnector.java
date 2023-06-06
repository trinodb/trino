package io.trino.plugin.spanner;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.JdbcTransactionHandle;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SpannerConnector implements Connector
{
    private final LifeCycleManager lifeCycleManager;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorRecordSetProvider recordSetProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final List<PropertyMetadata<?>> tableProperties;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public SpannerConnector(
            LifeCycleManager lifeCycleManager,
            SpannerConnectorMetadata metadata,
            ConnectorSplitManager splitManager,
            ConnectorRecordSetProvider recordSetProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            Set<TablePropertiesProvider> tableProperties,
            Set<SessionPropertiesProvider> sessionProperties)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.tableProperties = tableProperties.stream()
                .flatMap(tablePropertiesProvider -> tablePropertiesProvider.getTableProperties().stream())
                .collect(toImmutableList());
        this.sessionProperties = sessionProperties.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
    }
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return new JdbcTransactionHandle();
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return Collections.emptyList();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public final void shutdown()
    {
        lifeCycleManager.stop();
    }
}
