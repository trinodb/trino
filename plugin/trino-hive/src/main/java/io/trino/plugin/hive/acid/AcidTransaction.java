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
package io.trino.plugin.hive.acid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hive.acid.AcidOperation.CREATE_TABLE;
import static io.trino.plugin.hive.acid.AcidOperation.INSERT;
import static io.trino.plugin.hive.acid.AcidOperation.MERGE;
import static io.trino.plugin.hive.acid.AcidOperation.NONE;
import static java.util.Objects.requireNonNull;

public class AcidTransaction
{
    public static final AcidTransaction NO_ACID_TRANSACTION = new AcidTransaction(NONE, 0, 0);

    private final AcidOperation operation;
    private final long transactionId;
    private final long writeId;

    @JsonCreator
    public AcidTransaction(
            @JsonProperty("operation") AcidOperation operation,
            @JsonProperty("transactionId") long transactionId,
            @JsonProperty("writeId") long writeId)
    {
        this.operation = requireNonNull(operation, "operation is null");
        this.transactionId = transactionId;
        this.writeId = writeId;
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
    public boolean isAcidTransactionRunning()
    {
        return operation == INSERT || operation == CREATE_TABLE || operation == MERGE;
    }

    @JsonIgnore
    public boolean isTransactional()
    {
        return operation != AcidOperation.NONE;
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

    private void ensureTransactionRunning(String description)
    {
        checkState(isAcidTransactionRunning(), "Not in ACID transaction while %s", description);
    }

    @JsonIgnore
    public boolean isInsert()
    {
        return operation == INSERT;
    }

    @JsonIgnore
    public boolean isMerge()
    {
        return operation == MERGE;
    }

    public static AcidTransaction forCreateTable()
    {
        return new AcidTransaction(CREATE_TABLE, 0, 0);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("operation", operation)
                .add("transactionId", transactionId)
                .add("writeId", writeId)
                .toString();
    }
}
