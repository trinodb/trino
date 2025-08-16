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

import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FileSystemTransactionLogReader
        implements TransactionLogReader
{
    private final String tableLocation;
    private final TrinoFileSystemFactory fileSystemFactory;

    public FileSystemTransactionLogReader(String tableLocation, TrinoFileSystemFactory fileSystemFactory)
    {
        this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public TransactionLogTail loadNewTail(
            ConnectorSession session,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        return TransactionLogTail.loadNewTail(fileSystemFactory.create(session), tableLocation, startVersion, endVersion, transactionLogMaxCachedFileSize);
    }
}
