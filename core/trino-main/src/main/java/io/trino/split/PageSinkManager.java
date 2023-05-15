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
package io.trino.split;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public class PageSinkManager
        implements PageSinkProvider
{
    private final CatalogServiceProvider<ConnectorPageSinkProvider> pageSinkProvider;

    @Inject
    public PageSinkManager(CatalogServiceProvider<ConnectorPageSinkProvider> pageSinkProvider)
    {
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
    }

    @Override
    public ConnectorPageSink createPageSink(Session session, OutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogHandle());
        return providerFor(tableHandle.getCatalogHandle()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle(), pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(Session session, InsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogHandle());
        return providerFor(tableHandle.getCatalogHandle()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle(), pageSinkId);
    }

    @Override
    public ConnectorPageSink createPageSink(Session session, TableExecuteHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogHandle());
        return providerFor(tableHandle.getCatalogHandle()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle(), pageSinkId);
    }

    @Override
    public ConnectorMergeSink createMergeSink(Session session, MergeHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        // assumes connectorId and catalog are the same
        TableHandle tableHandle = mergeHandle.getTableHandle();
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogHandle());
        return providerFor(tableHandle.getCatalogHandle()).createMergeSink(tableHandle.getTransaction(), connectorSession, mergeHandle.getConnectorMergeHandle(), pageSinkId);
    }

    private ConnectorPageSinkProvider providerFor(CatalogHandle catalogHandle)
    {
        return pageSinkProvider.getService(catalogHandle);
    }
}
