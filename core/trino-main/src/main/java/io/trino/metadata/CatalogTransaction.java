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

import io.opentelemetry.api.trace.Tracer;
import io.trino.Session;
import io.trino.connector.informationschema.InformationSchemaMetadata;
import io.trino.connector.system.SystemTablesMetadata;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.tracing.TracingConnectorMetadata;

import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CatalogTransaction
{
    private final Tracer tracer;
    private final CatalogHandle catalogHandle;
    private final Connector connector;
    private final ConnectorTransactionHandle transactionHandle;
    @GuardedBy("this")
    private ConnectorMetadata connectorMetadata;
    private final AtomicBoolean finished = new AtomicBoolean();

    public CatalogTransaction(
            Tracer tracer,
            CatalogHandle catalogHandle,
            Connector connector,
            ConnectorTransactionHandle transactionHandle)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connector = requireNonNull(connector, "connector is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
    }

    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    public boolean isSingleStatementWritesOnly()
    {
        return connector.isSingleStatementWritesOnly();
    }

    public synchronized ConnectorMetadata getConnectorMetadata(Session session)
    {
        checkState(!finished.get(), "Already finished");
        if (connectorMetadata == null) {
            ConnectorSession connectorSession = session.toConnectorSession(catalogHandle);
            connectorMetadata = connector.getMetadata(connectorSession, transactionHandle);
            connectorMetadata = tracingConnectorMetadata(catalogHandle.getCatalogName(), connectorMetadata);
        }
        return connectorMetadata;
    }

    public ConnectorTransactionHandle getTransactionHandle()
    {
        checkState(!finished.get(), "Already finished");
        return transactionHandle;
    }

    public void commit()
    {
        if (finished.compareAndSet(false, true)) {
            connector.commit(transactionHandle);
        }
    }

    public void abort()
    {
        if (finished.compareAndSet(false, true)) {
            connector.rollback(transactionHandle);
        }
    }

    private ConnectorMetadata tracingConnectorMetadata(String catalogName, ConnectorMetadata delegate)
    {
        if ((delegate instanceof SystemTablesMetadata) || (delegate instanceof InformationSchemaMetadata)) {
            return delegate;
        }
        return new TracingConnectorMetadata(tracer, catalogName, delegate);
    }
}
