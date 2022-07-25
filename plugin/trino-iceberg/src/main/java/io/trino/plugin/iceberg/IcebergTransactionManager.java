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
package io.trino.plugin.iceberg;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class IcebergTransactionManager
{
    private final IcebergMetadataFactory metadataFactory;
    private final ClassLoader classLoader;
    private final ConcurrentMap<ConnectorTransactionHandle, MemoizedMetadata> transactions = new ConcurrentHashMap<>();
    private static boolean isGlobalMetadataCacheEnabled;
    private final CatalogType catalogType;

    @Inject
    public IcebergTransactionManager(IcebergMetadataFactory metadataFactory, IcebergConfig icebergConfig)
    {
        this(metadataFactory, Thread.currentThread().getContextClassLoader(), icebergConfig);
    }

    public IcebergTransactionManager(IcebergMetadataFactory metadataFactory, ClassLoader classLoader, IcebergConfig icebergConfig)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.catalogType = icebergConfig.getCatalogType();
        this.isGlobalMetadataCacheEnabled = icebergConfig.isGlobalMetadataCacheEnabled();
    }

    public void begin(ConnectorTransactionHandle transactionHandle)
    {
        MemoizedMetadata previousValue = transactions.putIfAbsent(transactionHandle, new MemoizedMetadata());
        checkState(previousValue == null);
    }

    public IcebergMetadata get(ConnectorTransactionHandle transactionHandle, ConnectorIdentity identity)
    {
        if (isGlobalMetadataCacheEnabled && catalogType.equals(CatalogType.GLUE)) {
            return transactions.get(transactionHandle).getCachedMetadata(identity);
        }
        else {
            return transactions.get(transactionHandle).get(identity);
        }
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata transactionalMetadata = transactions.remove(transaction);
        checkArgument(transactionalMetadata != null, "no such transaction: %s", transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata transactionalMetadata = transactions.remove(transaction);
        checkArgument(transactionalMetadata != null, "no such transaction: %s", transaction);
        transactionalMetadata.optionalGet().ifPresent(metadata -> {
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                metadata.rollback();
            }
        });
    }

    private class MemoizedMetadata
    {
        @GuardedBy("this")
        private IcebergMetadata metadata;

        @GuardedBy("this")
        private CachedIcebergMetadata cachedMetadata;

        public synchronized Optional<IcebergMetadata> optionalGet()
        {
            return Optional.ofNullable(metadata);
        }

        public synchronized IcebergMetadata get(ConnectorIdentity identity)
        {
            if (metadata == null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                    metadata = metadataFactory.create(identity);
                }
            }
            return metadata;
        }

        public synchronized IcebergMetadata getCachedMetadata(ConnectorIdentity identity)
        {
            if (cachedMetadata == null) {
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
                    cachedMetadata = metadataFactory.createCachedMetadata(identity);
                }
            }
            return cachedMetadata;
        }
    }
}
