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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.MissingTransactionLogException;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogEntries;
import io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
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
        ImmutableList.Builder<Transaction> entriesBuilder = ImmutableList.builder();

        if (startVersion.isPresent() && endVersion.isPresent() && startVersion.get().equals(endVersion.get())) {
            // This is time travel to a specific checkpoint. No need to read transaction log files.
            return new TransactionLogTail(entriesBuilder.build(), startVersion.get());
        }

        // TODO: check if we should use startVersion or endVersion, in the case that startVersion is not present this could returns empty entries which is not correct
        long version = startVersion.orElse(0L);
        long entryNumber = startVersion.map(start -> start + 1).orElse(0L);
        checkArgument(endVersion.isEmpty() || entryNumber <= endVersion.get(), "Invalid start/end versions: %s, %s", startVersion, endVersion);

        String transactionLogDir = getTransactionLogDir(tableLocation);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        boolean endOfTail = false;
        while (!endOfTail) {
            Optional<TransactionLogEntries> results = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, transactionLogMaxCachedFileSize);
            if (results.isPresent()) {
                entriesBuilder.add(new Transaction(entryNumber, results.get()));
                version = entryNumber;
                entryNumber++;
            }
            else {
                if (endVersion.isPresent()) {
                    throw new MissingTransactionLogException(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber).toString());
                }
                endOfTail = true;
            }

            if (endVersion.isPresent() && version == endVersion.get()) {
                endOfTail = true;
            }
        }

        return new TransactionLogTail(entriesBuilder.build(), version);
    }
}
