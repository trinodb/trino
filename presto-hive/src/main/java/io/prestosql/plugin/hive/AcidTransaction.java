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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static io.prestosql.plugin.hive.AcidOperation.DELETE;
import static io.prestosql.plugin.hive.AcidOperation.NONE;
import static java.util.Objects.requireNonNull;

public class AcidTransaction
{
    public static final AcidTransaction NO_ACID_TRANSACTION = new AcidTransaction(NONE, 0, 0);
    private final AcidOperation operation;
    private final long transactionId;
    private final long writeId;

    @JsonCreator
    public AcidTransaction(@JsonProperty("operation") AcidOperation operation, @JsonProperty("transactionId") long transactionId, @JsonProperty("writeId") long writeId)
    {
        this.operation = requireNonNull(operation, "operation is null");
        this.transactionId = transactionId;
        this.writeId = writeId;
    }

    public boolean isTransactionRunning()
    {
        return operation != AcidOperation.NONE;
    }

    public static AcidTransaction forDelete(long transactionId, long writeId)
    {
        return new AcidTransaction(DELETE, transactionId, writeId);
    }

    @JsonProperty
    public AcidOperation getOperation()
    {
        return operation;
    }

    @JsonProperty("transactionId")
    public long getAcidTransactionIdForSerialization()
    {
        return transactionId;
    }

    @JsonProperty("writeId")
    public long getWriteIdForSerialization()
    {
        return writeId;
    }

    @JsonIgnore
    public long getAcidTransactionId()
    {
        ensureTransactionRunning("accessing transactionId");
        return transactionId;
    }

    @JsonIgnore
    public long getWriteId()
    {
        ensureTransactionRunning("accessing writeId");
        return writeId;
    }

    @JsonIgnore
    public boolean isDelete()
    {
        return operation == DELETE;
    }

    private void ensureTransactionRunning(String description)
    {
        if (!isTransactionRunning()) {
            throw new IllegalStateException("Not in ACID transaction but " + description);
        }
    }

    @Override
    public String toString()
    {
        return "AcidTransaction{" +
                "operation=" + operation +
                ", transactionId=" + transactionId +
                '}';
    }
}
