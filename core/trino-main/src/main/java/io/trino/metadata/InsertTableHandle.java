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
package io.prestosql.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class InsertTableHandle
{
    private final CatalogName catalogName;
    private final ConnectorTransactionHandle transactionHandle;
    private final ConnectorInsertTableHandle connectorHandle;

    @JsonCreator
    public InsertTableHandle(
            @JsonProperty("catalogName") CatalogName catalogName,
            @JsonProperty("transactionHandle") ConnectorTransactionHandle transactionHandle,
            @JsonProperty("connectorHandle") ConnectorInsertTableHandle connectorHandle)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        this.connectorHandle = requireNonNull(connectorHandle, "connectorHandle is null");
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
    public ConnectorInsertTableHandle getConnectorHandle()
    {
        return connectorHandle;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, transactionHandle, connectorHandle);
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
        InsertTableHandle o = (InsertTableHandle) obj;
        return Objects.equals(this.catalogName, o.catalogName) &&
                Objects.equals(this.transactionHandle, o.transactionHandle) &&
                Objects.equals(this.connectorHandle, o.connectorHandle);
    }

    @Override
    public String toString()
    {
        return catalogName + ":" + connectorHandle;
    }
}
