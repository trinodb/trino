package io.trino.plugin.weaviate;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class WeaviatePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final WeaviateService weaviateService;

    @Inject
    public WeaviatePageSourceProvider(WeaviateService weaviateService)
    {
        this.weaviateService = requireNonNull(weaviateService, "weaviateService is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(connectorTableHandle, "connectorTableHandle is null");

        WeaviateTableHandle tableHandle = (WeaviateTableHandle) connectorTableHandle;
        if (columns.isEmpty()) {
            return new CountQueryPageSource(weaviateService, tableHandle);
        }
        return new FetchObjectsQueryPageSource(
                weaviateService,
                tableHandle,
                columns.stream().map(WeaviateColumnHandle.class::cast).toList());
    }
}
