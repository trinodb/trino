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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.ForDeltaLakeMetadata;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class FileSystemTransactionLogReaderFactory
        implements TransactionLogReaderFactory
{
    private final DeltaLakeFileSystemFactory fileSystemFactory;
    private final Optional<Supplier<Executor>> tableLoadExecutor;

    @VisibleForTesting
    public FileSystemTransactionLogReaderFactory(DeltaLakeFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableLoadExecutor = Optional.empty();
    }

    @Inject
    public FileSystemTransactionLogReaderFactory(
            DeltaLakeFileSystemFactory fileSystemFactory,
            @ForDeltaLakeMetadata ExecutorService executorService,
            DeltaLakeConfig deltaLakeConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        requireNonNull(executorService, "executorService is null");
        if (deltaLakeConfig.isTransactionLogParallelReadEnabled()) {
            // Shared by all table loads, so the number of concurrent reads on the coordinator does not grow with the number of queries
            Executor catalogExecutor = new BoundedExecutor(executorService, deltaLakeConfig.getTransactionLogMaxConcurrentReads());
            int readsPerTableLoad = deltaLakeConfig.getMetadataParallelism();
            // A per-load limit keeps one table load from taking every slot of the shared limit
            this.tableLoadExecutor = Optional.of(() -> new BoundedExecutor(catalogExecutor, readsPerTableLoad));
        }
        else {
            this.tableLoadExecutor = Optional.empty();
        }
    }

    @Override
    public TransactionLogReader createReader(DeltaLakeTableHandle tableHandle, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        return createReader(tableHandle.getLocation(), tableCredentials);
    }

    @Override
    public TransactionLogReader createReader(DeltaMetastoreTable table, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        return createReader(table.location(), tableCredentials);
    }

    @VisibleForTesting
    public TransactionLogReader createReader(String tableLocation, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        return new FileSystemTransactionLogReader(tableLocation, tableCredentials, fileSystemFactory, tableLoadExecutor.map(Supplier::get));
    }
}
