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
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.CatalogHandle.CatalogHandleType;
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
    private final String catalogName;

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
            String catalogName,
            CatalogTransaction catalogTransaction,
            CatalogTransaction informationSchemaTransaction,
            CatalogTransaction systemTransaction,
            SecurityManagement securityManagement,
            Set<ConnectorCapabilities> connectorCapabilities)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.catalogTransaction = requireNonNull(catalogTransaction, "catalogTransaction is null");
        this.informationSchemaTransaction = requireNonNull(informationSchemaTransaction, "informationSchemaTransaction is null");
        this.systemTransaction = requireNonNull(systemTransaction, "systemTransaction is null");
        this.securityManagement = requireNonNull(securityManagement, "securityManagement is null");
        this.connectorCapabilities = requireNonNull(connectorCapabilities, "connectorCapabilities is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogTransaction.getCatalogHandle();
    }

    public boolean isSingleStatementWritesOnly()
    {
        return catalogTransaction.isSingleStatementWritesOnly();
    }

    public ConnectorMetadata getMetadata(Session session)
    {
        return catalogTransaction.getConnectorMetadata(session);
    }

    public ConnectorMetadata getMetadataFor(Session session, CatalogHandle catalogHandle)
    {
        if (catalogHandle.equals(catalogTransaction.getCatalogHandle())) {
            return catalogTransaction.getConnectorMetadata(session);
        }
        if (catalogHandle.equals(informationSchemaTransaction.getCatalogHandle())) {
            return informationSchemaTransaction.getConnectorMetadata(session);
        }
        if (catalogHandle.equals(systemTransaction.getCatalogHandle())) {
            return systemTransaction.getConnectorMetadata(session);
        }
        throw new IllegalArgumentException("Unknown catalog handle: " + catalogHandle);
    }

    public ConnectorTransactionHandle getTransactionHandleFor(CatalogHandleType catalogHandleType)
    {
        switch (catalogHandleType) {
            case NORMAL:
                return catalogTransaction.getTransactionHandle();
            case INFORMATION_SCHEMA:
                return informationSchemaTransaction.getTransactionHandle();
            case SYSTEM:
                return systemTransaction.getTransactionHandle();
            default:
                throw new IllegalArgumentException("Unknown catalog handle: " + catalogHandleType);
        }
    }

    public ConnectorTransactionHandle getTransactionHandleFor(CatalogHandle catalogHandle)
    {
        if (catalogHandle.equals(catalogTransaction.getCatalogHandle())) {
            return catalogTransaction.getTransactionHandle();
        }
        if (catalogHandle.equals(informationSchemaTransaction.getCatalogHandle())) {
            return informationSchemaTransaction.getTransactionHandle();
        }
        if (catalogHandle.equals(systemTransaction.getCatalogHandle())) {
            return systemTransaction.getTransactionHandle();
        }
        throw new IllegalArgumentException("Unknown catalog handle: " + catalogHandle);
    }

    public CatalogHandle getConnectorHandleForSchema(CatalogSchemaName schema)
    {
        if (schema.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemaTransaction.getCatalogHandle();
        }
        return catalogTransaction.getCatalogHandle();
    }

    public CatalogHandle getCatalogHandle(Session session, QualifiedObjectName table)
    {
        if (table.getSchemaName().equals(INFORMATION_SCHEMA_NAME)) {
            return informationSchemaTransaction.getCatalogHandle();
        }

        if (systemTransaction.getConnectorMetadata(session).getTableHandle(session.toConnectorSession(systemTransaction.getCatalogHandle()), table.asSchemaTableName()) != null) {
            return systemTransaction.getCatalogHandle();
        }

        return catalogTransaction.getCatalogHandle();
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

    public List<CatalogHandle> listCatalogHandles()
    {
        return ImmutableList.of(informationSchemaTransaction.getCatalogHandle(), systemTransaction.getCatalogHandle(), catalogTransaction.getCatalogHandle());
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
                .add("catalogName", catalogName)
                .add("catalogHandle", getCatalogHandle())
                .toString();
    }
}
