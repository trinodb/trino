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

import io.trino.connector.CatalogName;
import io.trino.connector.ConnectorManager.MaterializedConnector;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.transaction.InternalConnector;
import io.trino.transaction.TransactionId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Catalog
{
    private final CatalogName catalogName;
    private final String connectorName;
    private final MaterializedConnector catalogConnector;
    private final MaterializedConnector informationSchemaConnector;
    private final MaterializedConnector systemConnector;

    public Catalog(
            CatalogName catalogName,
            String connectorName,
            MaterializedConnector catalogConnector,
            CatalogName informationSchemaName,
            MaterializedConnector informationSchemaConnector,
            CatalogName systemName,
            MaterializedConnector systemConnector)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.catalogConnector = requireNonNull(catalogConnector, "catalogConnector is null");
        this.informationSchemaConnector = requireNonNull(informationSchemaConnector, "informationSchemaConnector is null");
        this.systemConnector = requireNonNull(systemConnector, "systemConnector is null");
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
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
                catalogTransaction,
                informationSchemaTransaction,
                systemTransaction,
                catalogConnector.getSecurityManagement(),
                catalogConnector.getCapabilities());
    }

    private static CatalogTransaction beginTransaction(
            MaterializedConnector materializedConnector,
            TransactionId transactionId,
            IsolationLevel isolationLevel,
            boolean readOnly,
            boolean autoCommitContext)
    {
        Connector connector = materializedConnector.getConnector();
        ConnectorTransactionHandle transactionHandle;
        if (connector instanceof InternalConnector) {
            transactionHandle = ((InternalConnector) connector).beginTransaction(transactionId, isolationLevel, readOnly);
        }
        else {
            transactionHandle = connector.beginTransaction(isolationLevel, readOnly, autoCommitContext);
        }

        return new CatalogTransaction(materializedConnector.getCatalogName(), connector, transactionHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("connectorConnectorId", catalogName)
                .toString();
    }
}
