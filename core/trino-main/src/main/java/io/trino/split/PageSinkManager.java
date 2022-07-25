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

import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.TableExecuteHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

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
    public ConnectorPageSink createPageSink(Session session, OutputTableHandle tableHandle)
    {
        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogName());
        return providerFor(tableHandle.getCatalogName()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle());
    }

    @Override
    public ConnectorPageSink createPageSink(Session session, InsertTableHandle tableHandle)
    {
        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogName());
        return providerFor(tableHandle.getCatalogName()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle());
    }

    @Override
    public ConnectorPageSink createPageSink(Session session, TableExecuteHandle tableHandle)
    {
        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(tableHandle.getCatalogName());
        return providerFor(tableHandle.getCatalogName()).createPageSink(tableHandle.getTransactionHandle(), connectorSession, tableHandle.getConnectorHandle());
    }

    private ConnectorPageSinkProvider providerFor(CatalogName catalogName)
    {
        return pageSinkProvider.getService(catalogName);
    }
}
