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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.CatalogMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_FOUND;

public interface TransactionManager
{
    IsolationLevel DEFAULT_ISOLATION = IsolationLevel.READ_UNCOMMITTED;
    boolean DEFAULT_READ_ONLY = false;

    boolean transactionExists(TransactionId transactionId);

    TransactionInfo getTransactionInfo(TransactionId transactionId);

    Optional<TransactionInfo> getTransactionInfoIfExist(TransactionId transactionId);

    List<TransactionInfo> getAllTransactionInfos();

    Set<TransactionId> getTransactionsUsingCatalog(CatalogHandle catalogHandle);

    TransactionId beginTransaction(boolean autoCommitContext);

    TransactionId beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommitContext);

    List<CatalogInfo> getCatalogs(TransactionId transactionId);

    List<CatalogInfo> getActiveCatalogs(TransactionId transactionId);

    Optional<CatalogHandle> getCatalogHandle(TransactionId transactionId, String catalogName);

    default CatalogMetadata getRequiredCatalogMetadata(TransactionId transactionId, String catalogName)
    {
        return getOptionalCatalogMetadata(transactionId, catalogName)
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + catalogName));
    }

    Optional<CatalogMetadata> getOptionalCatalogMetadata(TransactionId transactionId, String catalogName);

    CatalogMetadata getCatalogMetadata(TransactionId transactionId, CatalogHandle catalogHandle);

    CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, CatalogHandle catalogHandle);

    CatalogMetadata getCatalogMetadataForWrite(TransactionId transactionId, String catalogName);

    ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, String catalogName);

    ConnectorTransactionHandle getConnectorTransaction(TransactionId transactionId, CatalogHandle catalogHandle);

    void checkAndSetActive(TransactionId transactionId);

    void trySetActive(TransactionId transactionId);

    void trySetInactive(TransactionId transactionId);

    ListenableFuture<Void> asyncCommit(TransactionId transactionId);

    ListenableFuture<Void> asyncAbort(TransactionId transactionId);

    void blockCommit(TransactionId transactionId, String reason);

    void fail(TransactionId transactionId);
}
