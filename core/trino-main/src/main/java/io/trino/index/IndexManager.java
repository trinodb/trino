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
package io.trino.index;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.IndexHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class IndexManager
{
    private final CatalogServiceProvider<ConnectorIndexProvider> indexProvider;

    @Inject
    public IndexManager(CatalogServiceProvider<ConnectorIndexProvider> indexProvider)
    {
        this.indexProvider = requireNonNull(indexProvider, "indexProvider is null");
    }

    public ConnectorIndex getIndex(Session session, IndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        ConnectorSession connectorSession = session.toConnectorSession(indexHandle.getCatalogHandle());
        ConnectorIndexProvider provider = indexProvider.getService(indexHandle.getCatalogHandle());
        return provider.getIndex(indexHandle.getTransactionHandle(), connectorSession, indexHandle.getConnectorHandle(), lookupSchema, outputSchema);
    }
}
