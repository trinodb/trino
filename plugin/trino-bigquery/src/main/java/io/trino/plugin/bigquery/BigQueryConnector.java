/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bigquery;

import io.airlift.log.Logger;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class BigQueryConnector
        implements Connector
{
    private static final Logger log = Logger.get(BigQueryConnector.class);

    private final BigQueryMetadata metadata;
    private final BigQuerySplitManager splitManager;
    private final BigQueryPageSourceProvider pageSourceProvider;
    private final BigQueryPageSinkProvider pageSinkProvider;
    private final Set<ConnectorTableFunction> connectorTableFunctions;
    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public BigQueryConnector(
            BigQueryMetadata metadata,
            BigQuerySplitManager splitManager,
            BigQueryPageSourceProvider pageSourceProvider,
            BigQueryPageSinkProvider pageSinkProvider,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionPropertiesProviders)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.connectorTableFunctions = requireNonNull(connectorTableFunctions, "connectorTableFunctions is null");
        this.sessionProperties = sessionPropertiesProviders.stream()
                .flatMap(sessionPropertiesProvider -> sessionPropertiesProvider.getSessionProperties().stream())
                .collect(toImmutableList());
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        log.debug("beginTransaction(isolationLevel=%s, readOnly=%s)", isolationLevel, readOnly);
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return BigQueryTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
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
        return pageSinkProvider;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return connectorTableFunctions;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
