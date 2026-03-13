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
package io.trino.plugin.ducklake;

import com.google.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Manages transactions for the Ducklake connector.
 * Each transaction maintains its own metadata instance.
 */
public class DucklakeTransactionManager
{
    private final DucklakeMetadataFactory metadataFactory;
    private final Map<DucklakeTransactionHandle, DucklakeMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public DucklakeTransactionManager(DucklakeMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public void begin(DucklakeTransactionHandle transaction)
    {
        requireNonNull(transaction, "transaction is null");
        DucklakeMetadata metadata = metadataFactory.create();
        transactions.put(transaction, metadata);
    }

    public DucklakeMetadata getMetadata(io.trino.spi.connector.ConnectorTransactionHandle transaction)
    {
        DucklakeMetadata metadata = transactions.get(transaction);
        if (metadata == null) {
            throw new IllegalArgumentException("Unknown transaction: " + transaction);
        }
        return metadata;
    }

    public void commit(DucklakeTransactionHandle transaction)
    {
        requireNonNull(transaction, "transaction is null");
        DucklakeMetadata metadata = transactions.remove(transaction);
        if (metadata == null) {
            throw new IllegalArgumentException("Unknown transaction: " + transaction);
        }
        // For read-only transactions, nothing to commit
        // Write transactions will be implemented later
    }

    public void rollback(DucklakeTransactionHandle transaction)
    {
        requireNonNull(transaction, "transaction is null");
        DucklakeMetadata metadata = transactions.remove(transaction);
        if (metadata == null) {
            throw new IllegalArgumentException("Unknown transaction: " + transaction);
        }
        // For read-only transactions, nothing to rollback
        // Write transactions will be implemented later
    }
}
