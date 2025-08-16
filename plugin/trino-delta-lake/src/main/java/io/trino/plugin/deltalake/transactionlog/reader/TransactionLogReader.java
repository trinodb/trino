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
package io.trino.plugin.deltalake.transactionlog.reader;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public interface TransactionLogReader
{
    TransactionLogTail loadNewTail(
            ConnectorSession session,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException;

    default Optional<TransactionLogTail> getUpdatedTail(
            ConnectorSession session,
            TransactionLogTail oldTail,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        long version = oldTail.getVersion();
        checkArgument(endVersion.isEmpty() || endVersion.get() > version, "Invalid endVersion, expected higher than %s, but got %s", version, endVersion);
        TransactionLogTail newTail = loadNewTail(session, Optional.of(version), endVersion, transactionLogMaxCachedFileSize);
        if (newTail.getVersion() == version) {
            return Optional.empty();
        }
        return Optional.of(new TransactionLogTail(
                ImmutableList.<Transaction>builder()
                        .addAll(oldTail.getTransactions())
                        .addAll(newTail.getTransactions())
                        .build(),
                newTail.getVersion()));
    }
}
