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

import org.apache.hadoop.hive.common.ValidTxnWriteIdList;

import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;

public class HiveTransaction
{
    private final long transactionId;
    private final ScheduledFuture<?> heartbeatTask;
    private final String validWriteIds;

    public HiveTransaction(long transactionId, ScheduledFuture<?> heartbeatTask, String validWriteIds)
    {
        this.transactionId = transactionId;
        this.heartbeatTask = requireNonNull(heartbeatTask, "heartbeatTask is null");
        this.validWriteIds = requireNonNull(validWriteIds, "validWriteIds is null");
    }

    public long getTransactionId()
    {
        return transactionId;
    }

    public ScheduledFuture<?> getHeartbeatTask()
    {
        return heartbeatTask;
    }

    public ValidTxnWriteIdList getValidWriteIds()
    {
        return new ValidTxnWriteIdList(validWriteIds);
    }
}
