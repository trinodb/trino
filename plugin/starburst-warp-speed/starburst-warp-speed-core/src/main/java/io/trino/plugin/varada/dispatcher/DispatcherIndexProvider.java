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
package io.trino.plugin.varada.dispatcher;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorIndex;
import io.trino.spi.connector.ConnectorIndexHandle;
import io.trino.spi.connector.ConnectorIndexProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;

public class DispatcherIndexProvider
        implements ConnectorIndexProvider
{
    private final ConnectorIndexProvider connectorIndexProvider;

    public DispatcherIndexProvider(ConnectorIndexProvider varadaConnectorIndexProvider)
    {
        this.connectorIndexProvider = varadaConnectorIndexProvider;
    }

    @Override
    public ConnectorIndex getIndex(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        return connectorIndexProvider.getIndex(transaction, session, indexHandle, lookupSchema, outputSchema);
    }
}
