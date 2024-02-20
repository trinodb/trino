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
package io.trino.plugin.bigquery;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class BigQueryTransactionManager
{
    private static final Logger log = Logger.get(BigQueryTransactionManager.class);

    private final ConcurrentMap<ConnectorTransactionHandle, BigQueryMetadata> transactions = new ConcurrentHashMap<>();
    private final BigQueryMetadataFactory metadataFactory;

    @Inject
    public BigQueryTransactionManager(BigQueryMetadataFactory metadataFactory)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
    }

    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        log.debug("beginTransaction(isolationLevel=%s, readOnly=%s)", isolationLevel, readOnly);
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        BigQueryTransactionHandle transaction = new BigQueryTransactionHandle();
        transactions.put(transaction, metadataFactory.create(transaction));
        return transaction;
    }

    public BigQueryMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        BigQueryMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        BigQueryMetadata metadata = transactions.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        metadata.rollback();
    }
}
