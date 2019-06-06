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
package io.prestosql.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.transaction.TransactionId;

import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SystemTransactionHandle
        implements ConnectorTransactionHandle
{
    private final TransactionId transactionId;
    private final Supplier<ConnectorTransactionHandle> connectorTransactionHandle;

    SystemTransactionHandle(
            TransactionId transactionId,
            Function<TransactionId, ConnectorTransactionHandle> transactionHandleFunction)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        requireNonNull(transactionHandleFunction, "transactionHandleFunction is null");
        this.connectorTransactionHandle = Suppliers.memoize(() -> transactionHandleFunction.apply(transactionId));
    }

    @JsonCreator
    public SystemTransactionHandle(
            @JsonProperty("transactionId") TransactionId transactionId,
            @JsonProperty("connectorTransactionHandle") ConnectorTransactionHandle connectorTransactionHandle)
    {
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        requireNonNull(connectorTransactionHandle, "connectorTransactionHandle is null");
        this.connectorTransactionHandle = () -> connectorTransactionHandle;
    }

    @JsonProperty
    public TransactionId getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public ConnectorTransactionHandle getConnectorTransactionHandle()
    {
        return connectorTransactionHandle.get();
    }

    @Override
    public int hashCode()
    {
        return transactionId.hashCode();
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
        SystemTransactionHandle other = (SystemTransactionHandle) obj;
        return transactionId.equals(other.transactionId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("transactionHandle", transactionId)
                .toString();
    }
}
