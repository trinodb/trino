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

package io.trino.transaction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.connector.CatalogName;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public class TestingTransactionManager
        implements TransactionManager
{
    private final Map<TransactionId, Object> transactions = new ConcurrentHashMap<>();

    @Override
    public boolean transactionExists(TransactionId transactionId)
    {
        return transactions.containsKey(transactionId);
    }

    @Override
    public TransactionInfo getTransactionInfo(TransactionId transactionId)
    {
        checkArgument(transactions.containsKey(transactionId), "Unknown transaction");
        return new TransactionInfo(
                transactionId,
                IsolationLevel.READ_UNCOMMITTED,
                false, //read only
                false, // auto commit
                DateTime.now(), // created
                Duration.succinctNanos(0), // idle
                ImmutableList.of(), // catalogs
                Optional.empty()); // write catalog
    }

    @Override
    public List<TransactionInfo> getAllTransactionInfos()
    {
        return transactions.keySet().stream()
                .map(this::getTransactionInfo)
                .collect(toImmutableList());
    }

    @Override
    public TransactionId beginTransaction(boolean autoCommitContext)
    {
        TransactionId transaction = TransactionId.create();
        transactions.put(transaction, new Object());
        return transaction;
    }

    @Override
    public TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext)
    {
        return beginTransaction(autoCommitContext);
    }

    @Override
    public Map<String, Catalog> getCatalogs(TransactionId transactionId)
    {
        return ImmutableMap.of();
    }

    @Override
    public Optional<CatalogName> getCatalogName(TransactionId transactionId, String catalogName)
    {
        return Optional.empty();
    }

    @Override
    public Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        return Optional.empty();
    }

    @Override
    public CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogName catalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAndSetActive(TransactionId transactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void trySetActive(TransactionId transactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void trySetInactive(TransactionId transactionId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<Void> asyncCommit(TransactionId transactionId)
    {
        checkState(transactions.remove(transactionId) != null, "Transaction is already finished");
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> asyncAbort(TransactionId transactionId)
    {
        checkState(transactions.remove(transactionId) != null, "Transaction is already finished");
        return immediateVoidFuture();
    }

    @Override
    public void fail(TransactionId transactionId)
    {
        checkState(transactions.remove(transactionId) != null, "Transaction is already finished");
    }
}
