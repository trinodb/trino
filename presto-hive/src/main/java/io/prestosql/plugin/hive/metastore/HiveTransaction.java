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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;

public class HiveTransaction
{
    private final HiveIdentity identity;
    private final String queryId;
    private final long transactionId;
    private final ScheduledFuture<?> heartbeatTask;

    private final Map<SchemaTableName, ValidTxnWriteIdList> validHiveTransactionsForTable = new HashMap<>();

    public HiveTransaction(HiveIdentity identity, String queryId, long transactionId, ScheduledFuture<?> heartbeatTask)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = transactionId;
        this.heartbeatTask = requireNonNull(heartbeatTask, "heartbeatTask is null");
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public ScheduledFuture<?> getHeartbeatTask()
    {
        return heartbeatTask;
    }

    public ValidTxnWriteIdList getValidWriteIds(HiveMetastoreClosure metastore, HiveTableHandle tableHandle)
    {
        // Different calls for same table might need to lock different partitions so acquire locks every time
        metastore.acquireSharedReadLock(
                identity,
                queryId,
                transactionId,
                tableHandle.getPartitions().isEmpty() ? ImmutableList.of(tableHandle.getSchemaTableName()) : ImmutableList.of(),
                tableHandle.getPartitions().orElse(ImmutableList.of()));

        // For repeatable reads within a query, use the same list of valid transactions for a table which have once been used
        return validHiveTransactionsForTable.computeIfAbsent(tableHandle.getSchemaTableName(), schemaTableName -> new ValidTxnWriteIdList(
                metastore.getValidWriteIds(
                        identity,
                        ImmutableList.of(schemaTableName),
                        transactionId)));
    }
}
