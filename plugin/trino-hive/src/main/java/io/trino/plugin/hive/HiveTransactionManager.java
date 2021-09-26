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
package io.trino.plugin.hive;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HiveTransactionManager
{
    private final TransactionalMetadataFactory metadataFactory;
    private final Map<ConnectorTransactionHandle, MemoizedMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public HiveTransactionManager(TransactionalMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public void begin(ConnectorTransactionHandle transactionHandle)
    {
        MemoizedMetadata previousValue = transactions.putIfAbsent(transactionHandle, new MemoizedMetadata());
        checkState(previousValue == null);
    }

    public TransactionalMetadata get(ConnectorTransactionHandle transactionHandle)
    {
        return transactions.get(transactionHandle).get(((HiveTransactionHandle) transactionHandle).isAutoCommit());
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata transactionalMetadata = transactions.remove(transaction);
        checkArgument(transactionalMetadata != null, "no such transaction: %s", transaction);
        transactionalMetadata.optionalGet().ifPresent(metadata -> {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                metadata.commit();
            }
        });
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata transactionalMetadata = transactions.remove(transaction);
        checkArgument(transactionalMetadata != null, "no such transaction: %s", transaction);
        transactionalMetadata.optionalGet().ifPresent(metadata -> {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                metadata.rollback();
            }
        });
    }

    private class MemoizedMetadata
    {
        @GuardedBy("this")
        private TransactionalMetadata metadata;

        public synchronized Optional<TransactionalMetadata> optionalGet()
        {
            return Optional.ofNullable(metadata);
        }

        public synchronized TransactionalMetadata get(boolean autoCommit)
        {
            if (metadata == null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                    metadata = metadataFactory.create(autoCommit);
                }
            }
            return metadata;
        }
    }
}
