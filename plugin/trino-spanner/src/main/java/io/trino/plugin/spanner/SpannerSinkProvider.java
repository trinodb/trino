package io.trino.plugin.spanner;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

public class SpannerSinkProvider
        implements ConnectorPageSinkProvider
{
    private final SpannerClient client;
    private final RemoteQueryModifier modifier;
    private final SpannerTableProperties spannerTableProperties;

    @Inject
    public SpannerSinkProvider(
            ConnectionFactory connectionFactory,
            JdbcClient jdbcClient,
            RemoteQueryModifier modifier,
            SpannerClient client,
            SpannerTableProperties propertiesProvider)
    {
        System.out.println("Called Spanner Sink provider");
        this.client = client;
        this.modifier = modifier;
        System.out.println("TABLE PROPS in SINK " + propertiesProvider.getTableProperties());
        this.spannerTableProperties = propertiesProvider;
        PropertyMetadata<?> propertyMetadata = propertiesProvider.getTableProperties().get(0);
        System.out.println(propertyMetadata.getDefaultValue());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle, ConnectorPageSinkId pageSinkId)
    {
        return new SpannerSink(session, (JdbcOutputTableHandle) outputTableHandle, client, pageSinkId, modifier);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle, ConnectorPageSinkId pageSinkId)
    {
        System.out.println("SinkProvider 2");
        return null;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, ConnectorPageSinkId pageSinkId)
    {
        System.out.println("SinkProvider 3");
        return null;
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        System.out.println("SinkProvider 4");
        return null;
    }
}
