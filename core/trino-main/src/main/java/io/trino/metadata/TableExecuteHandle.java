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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.connector.CatalogName;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class TableExecuteHandle
{
    private final CatalogName catalogName;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorTableExecuteHandle connectorHandle;
    private final Optional<TableHandle> sourceTableHandle;

    @JsonCreator
    public TableExecuteHandle(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("connectorHandle") ConnectorTableExecuteHandle connectorHandle)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
        this.sourceTableHandle = connectorHandle.getSourceTableHandle().map(sourceTableHandle -> new TableHandle(catalogName, sourceTableHandle, transactionHandle, Optional.empty()));
    }

    @JsonProperty
    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransactionHandle()
    {
        return transactionHandle;
    }

    @JsonProperty
    public ConnectorTableExecuteHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonIgnore
    public Optional<TableHandle> getSourceTableHandle()
    {
        return sourceTableHandle;
    }

    public TableExecuteHandle withConnectorHandle(ConnectorTableExecuteHandle connectorHandle)
    {
        return new TableExecuteHandle(catalogName, transactionHandle, connectorHandle);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TableExecuteHandle o = (TableExecuteHandle) obj;
        return Objects.equals(this.catalogName, o.catalogName) &&
                Objects.equals(this.transactionHandle, o.transactionHandle) &&
                Objects.equals(this.connectorHandle, o.connectorHandle) &&
                Objects.equals(this.sourceTableHandle, o.sourceTableHandle);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, transactionHandle, connectorHandle, sourceTableHandle);
    }

    @Override
    public String toString()
    {
        return "Execute[" + catalogName + ":" + connectorHandle + "]";
    }
}
