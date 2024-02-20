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
package io.trino.plugin.deltalake;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTransactionManager
{
    private final DeltaLakeMetadataFactory metadataFactory;
    private final Map<ConnectorTransactionHandle, MemoizedMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public DeltaLakeTransactionManager(DeltaLakeMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public void begin(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata previousValue = transactions.putIfAbsent(transaction, new MemoizedMetadata());
        checkState(previousValue == null);
    }

    public DeltaLakeMetadata get(ConnectorTransactionHandle transaction, ConnectorIdentity identity)
    {
        return transactions.get(transaction).get(identity);
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata deltaLakeMetadata = transactions.remove(transaction);
        checkArgument(deltaLakeMetadata != null, "no such transaction: %s", transaction);
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
        private DeltaLakeMetadata metadata;

        public synchronized Optional<DeltaLakeMetadata> optionalGet()
        {
            return Optional.ofNullable(metadata);
        }

        public synchronized DeltaLakeMetadata get(ConnectorIdentity identity)
        {
            if (metadata == null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
                    metadata = metadataFactory.create(identity);
                }
            }
            return metadata;
        }
    }
}
