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
package io.trino.metadata;

import io.trino.connector.ConnectorServices;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.InternalConnector;
import io.trino.transaction.TransactionId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Catalog
{
    private final String catalogName;
    private final CatalogHandle catalogHandle;
    private final String connectorName;
    private final ConnectorServices catalogConnector;
    private final ConnectorServices informationSchemaConnector;
    private final ConnectorServices systemConnector;

    public Catalog(
            String catalogName,
            CatalogHandle catalogHandle,
            String connectorName,
            ConnectorServices catalogConnector,
            ConnectorServices informationSchemaConnector,
            ConnectorServices systemConnector)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        checkArgument(!catalogHandle.getType().isInternal(), "Internal catalogName not allowed");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.catalogConnector = requireNonNull(catalogConnector, "catalogConnector is null");
        this.informationSchemaConnector = requireNonNull(informationSchemaConnector, "informationSchemaConnector is null");
        this.systemConnector = requireNonNull(systemConnector, "systemConnector is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    public String getConnectorName()
    {
        return connectorName;
    }

    public CatalogMetadata beginTransaction(
            TransactionId transactionId,
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommitContext)
    {
        CatalogTransaction catalogTransaction = beginTransaction(catalogConnector, transactionId, isolationLevel, readOnly, autoCommitContext);
        CatalogTransaction informationSchemaTransaction = beginTransaction(informationSchemaConnector, transactionId, isolationLevel, readOnly, autoCommitContext);
        CatalogTransaction systemTransaction = beginTransaction(systemConnector, transactionId, isolationLevel, readOnly, autoCommitContext);

        return new CatalogMetadata(
                catalogName,
                catalogTransaction,
                informationSchemaTransaction,
                systemTransaction,
                catalogConnector.getSecurityManagement(),
                catalogConnector.getCapabilities());
    }

    private static CatalogTransaction beginTransaction(
            ConnectorServices connectorServices,
            TransactionId transactionId,
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommitContext)
    {
        Connector connector = connectorServices.getConnector();
        ConnectorTransactionHandle transactionHandle;
        if (connector instanceof InternalConnector) {
            transactionHandle = ((InternalConnector) connector).beginTransaction(transactionId, isolationLevel, readOnly);
        }
        else {
            transactionHandle = connector.beginTransaction(isolationLevel, readOnly, autoCommitContext);
        }

        return new CatalogTransaction(connectorServices.getCatalogHandle(), connector, transactionHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("catalogHandle", catalogHandle)
                .toString();
    }
}
