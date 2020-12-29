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
package io.prestosql.split;

import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.InsertTableHandle;
import io.prestosql.metadata.OutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PageSinkManager
        implements PageSinkProvider
{
    private final ConcurrentMap<CatalogName, ConnectorPageSinkProvider> pageSinkProviders = new ConcurrentHashMap<>();

    public void addConnectorPageSinkProvider(CatalogName catalogName, ConnectorPageSinkProvider pageSinkProvider)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        checkState(pageSinkProviders.put(catalogName, pageSinkProvider) == null, "PageSinkProvider for connector '%s' is already registered", catalogName);
    }

    public void removeConnectorPageSinkProvider(CatalogName catalogName)
    {
        pageSinkProviders.remove(catalogName);
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

    private ConnectorPageSinkProvider providerFor(CatalogName catalogName)
    {
        ConnectorPageSinkProvider provider = pageSinkProviders.get(catalogName);
        checkArgument(provider != null, "No page sink provider for catalog '%s'", catalogName.getCatalogName());
        return provider;
    }
}
