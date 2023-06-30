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
package io.trino.plugin.hudi;

import com.google.inject.Inject;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.ConnectorIdentity;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HudiTransactionManager
{
    private final Map<ConnectorTransactionHandle, MemoizedMetadata> transactions = new ConcurrentHashMap<>();
    private final HudiMetadataFactory metadataFactory;

    @Inject
    public HudiTransactionManager(HudiMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public HudiMetadata get(ConnectorTransactionHandle transaction, ConnectorIdentity identity)
    {
        HudiMetadata metadata = transactions.get(transaction).get(identity);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata transactionalMetadata = transactions.remove(transaction);
        checkArgument(transactionalMetadata != null, "no such transaction: %s", transaction);
    }

    public void put(ConnectorTransactionHandle transaction)
    {
        MemoizedMetadata existing = transactions.putIfAbsent(transaction, new MemoizedMetadata());
        checkState(existing == null, "transaction already exists: %s", existing);
    }

    private class MemoizedMetadata
    {
        @GuardedBy("this")
        private HudiMetadata metadata;

        public synchronized HudiMetadata get(ConnectorIdentity identity)
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
