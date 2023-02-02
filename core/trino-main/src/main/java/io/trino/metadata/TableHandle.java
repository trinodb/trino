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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class TableHandle
{
    private final CatalogHandle catalogHandle;
    private final ConnectorTableHandle connectorHandle;
    private final ConnectorTransactionHandle transaction;

    @JsonCreator
    public TableHandle(
            @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
            @JsonProperty("connectorHandle") ConnectorTableHandle connectorHandle,
            @JsonProperty("transaction") ConnectorTransactionHandle transaction)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    @JsonProperty
    public CatalogHandle getCatalogHandle()
    {
        return catalogHandle;
    }

    @JsonProperty
    public ConnectorTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @JsonProperty
    public ConnectorTransactionHandle getTransaction()
    {
        return transaction;
    }

    public TableHandle withConnectorHandle(ConnectorTableHandle connectorHandle)
    {
        return new TableHandle(
                catalogHandle,
                connectorHandle,
                transaction);
    }

    @Override
    public String toString()
    {
        return catalogHandle + ":" + connectorHandle;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableHandle other = (TableHandle) o;
        return Objects.equals(catalogHandle, other.catalogHandle) &&
                Objects.equals(connectorHandle, other.connectorHandle) &&
                Objects.equals(transaction, other.transaction);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogHandle, connectorHandle, transaction);
    }
}
