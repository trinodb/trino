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

import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeFileSystemFactory;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.metastore.VendedCredentialsHandle;

import static java.util.Objects.requireNonNull;

public class FileSystemTransactionLogReaderFactory
        implements TransactionLogReaderFactory
{
    private final DeltaLakeFileSystemFactory fileSystemFactory;

    @Inject
    public FileSystemTransactionLogReaderFactory(DeltaLakeFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TransactionLogReader createReader(DeltaLakeTableHandle tableHandle)
    {
        return new FileSystemTransactionLogReader(tableHandle.getLocation(), tableHandle.toCredentialsHandle(), fileSystemFactory);
    }

    @Override
    public TransactionLogReader createReader(DeltaMetastoreTable table)
    {
        return new FileSystemTransactionLogReader(table.location(), VendedCredentialsHandle.of(table), fileSystemFactory);
    }
}
