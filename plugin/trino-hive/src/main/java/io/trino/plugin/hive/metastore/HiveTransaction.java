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
package io.trino.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;

public class HiveTransaction
{
    private final String queryId;
    private final long transactionId;
    private final ScheduledFuture<?> heartbeatTask;
    private final AcidTransaction transaction;

    private final Map<SchemaTableName, ValidTxnWriteIdList> validHiveTransactionsForTable = new HashMap<>();

    public HiveTransaction(String queryId, long transactionId, ScheduledFuture<?> heartbeatTask, AcidTransaction transaction)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = transactionId;
        this.heartbeatTask = requireNonNull(heartbeatTask, "heartbeatTask is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public ScheduledFuture<?> getHeartbeatTask()
    {
        return heartbeatTask;
    }

    public AcidTransaction getTransaction()
    {
        return transaction;
    }

    public ValidTxnWriteIdList getValidWriteIds(HiveMetastoreClosure metastore, HiveTableHandle tableHandle)
    {
        List<SchemaTableName> lockedTables;
        List<HivePartition> lockedPartitions;

        if (tableHandle.getPartitionColumns().isEmpty() || tableHandle.getPartitions().isEmpty()) {
            lockedTables = ImmutableList.of(tableHandle.getSchemaTableName());
            lockedPartitions = ImmutableList.of();
        }
        else {
            lockedTables = ImmutableList.of();
            lockedPartitions = tableHandle.getPartitions().get();
        }

        // Different calls for same table might need to lock different partitions so acquire locks every time
        metastore.acquireSharedReadLock(
                queryId,
                transactionId,
                lockedTables,
                lockedPartitions);

        // For repeatable reads within a query, use the same list of valid transactions for a table which have once been used
        return validHiveTransactionsForTable.computeIfAbsent(tableHandle.getSchemaTableName(), schemaTableName -> new ValidTxnWriteIdList(
                metastore.getValidWriteIds(
                        ImmutableList.of(schemaTableName),
                        transactionId)));
    }
}
