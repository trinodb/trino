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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.ConnectorCapabilities;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CatalogMetadata
{
    public enum SecurityManagement
    {
        SYSTEM, CONNECTOR
    }

    private static final Logger log = Logger.get(CatalogMetadata.class);
    private static final String INFORMATION_SCHEMA_NAME = "information_schema";

    private final CatalogTransaction catalogTransaction;
    private final CatalogTransaction informationSchemaTransaction;
    private final CatalogTransaction systemTransaction;

    private final SecurityManagement securityManagement;
    private final Set<ConnectorCapabilities> connectorCapabilities;

    public CatalogMetadata(
            CatalogTransaction catalogTransaction,
            CatalogTransaction informationSchemaTransaction,
            CatalogTransaction systemTransaction,
            SecurityManagement securityManagement,
            Set<ConnectorCapabilities> connectorCapabilities)
    {
        this.catalogTransaction = requireNonNull(catalogTransaction, "catalogTransaction is null");
        this.informationSchemaTransaction = requireNonNull(informationSchemaTransaction, "informationSchemaTransaction is null");
        this.systemTransaction = requireNonNull(systemTransaction, "systemTransaction is null");
        this.securityManagement = requireNonNull(securityManagement, "securityManagement is null");
        this.connectorCapabilities = requireNonNull(connectorCapabilities, "connectorCapabilities is null");
    }

    public CatalogName getCatalogName()
    {
        return catalogTransaction.getCatalogName();
    }

    public boolean isSingleStatementWritesOnly()
    {
        return catalogTransaction.isSingleStatementWritesOnly();
    }

    public ConnectorMetadata getMetadata(Session session)
    {
        return catalogTransaction.getConnectorMetadata(session);
    }

    public ConnectorMetadata getMetadataFor(Session session, CatalogName catalogName)
    {
        if (catalogName.equals(catalogTransaction.getCatalogName())) {
            return catalogTransaction.getConnectorMetadata(session);
        }
        if (catalogName.equals(informationSchemaTransaction.getCatalogName())) {
            return informationSchemaTransaction.getConnectorMetadata(session);
        }
        if (catalogName.equals(systemTransaction.getCatalogName())) {
            return systemTransaction.getConnectorMetadata(session);
        }
        throw new IllegalArgumentException("Unknown connector id: " + catalogName);
    }

    public ConnectorTransactionHandle getTransactionHandleFor(CatalogName catalogName)
    {
        if (catalogName.equals(catalogTransaction.getCatalogName())) {
            return catalogTransaction.getTransactionHandle();
        }
        if (catalogName.equals(informationSchemaTransaction.getCatalogName())) {
            return informationSchemaTransaction.getTransactionHandle();
        }
        if (catalogName.equals(systemTransaction.getCatalogName())) {
            return systemTransaction.getTransactionHandle();
        }
        throw new IllegalArgumentException("Unknown connector id: " + catalogName);
    }

    public CatalogName getConnectorIdForSchema(CatalogSchemaName schema)
    {
        if (schema.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemaTransaction.getCatalogName();
        }
        return catalogTransaction.getCatalogName();
    }

    public CatalogName getConnectorId(Session session, QualifiedObjectName table)
    {
        if (table.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemaTransaction.getCatalogName();
        }

        if (systemTransaction.getConnectorMetadata(session).getTableHandle(session.toConnectorSession(systemTransaction.getCatalogName()), table.asSchemaTableName()) != null) {
            return systemTransaction.getCatalogName();
        }

        return catalogTransaction.getCatalogName();
    }

    public void commit()
    {
        informationSchemaTransaction.commit();
        systemTransaction.commit();
        catalogTransaction.commit();
    }

    public void abort()
    {
        safeAbort(informationSchemaTransaction);
        safeAbort(systemTransaction);
        safeAbort(catalogTransaction);
    }

    private static void safeAbort(CatalogTransaction transaction)
    {
        try {
            transaction.abort();
        }
        catch (Exception e) {
            log.error(e, "Connector threw exception on abort");
        }
    }

    public List<CatalogName> listConnectorIds()
    {
        return ImmutableList.of(informationSchemaTransaction.getCatalogName(), systemTransaction.getCatalogName(), catalogTransaction.getCatalogName());
    }

    public SecurityManagement getSecurityManagement()
    {
        return securityManagement;
    }

    public Set<ConnectorCapabilities> getConnectorCapabilities()
    {
        return connectorCapabilities;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogTransaction.getCatalogName())
                .toString();
    }
}
