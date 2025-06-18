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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.Transaction;
import io.trino.plugin.deltalake.transactionlog.TransactionLogEntries;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static java.util.Objects.requireNonNull;

public class TransactionLogTail
{
    private static final int INSTANCE_SIZE = instanceSize(TransactionLogTail.class);

    private final List<Transaction> entries;
    private final long version;

    public TransactionLogTail(List<Transaction> entries, long version)
    {
        this.entries = ImmutableList.copyOf(requireNonNull(entries, "entries is null"));
        this.version = version;
    }

    /**
     * @deprecated use {@link #getEntriesFromJson(long, TrinoInputFile, DataSize)}
     */
    @Deprecated
    public static Optional<TransactionLogEntries> getEntriesFromJson(long entryNumber, String transactionLogDir, TrinoFileSystem fileSystem, DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        Location transactionLogFilePath = getTransactionLogJsonEntryPath(transactionLogDir, entryNumber);
        TrinoInputFile inputFile = fileSystem.newInputFile(transactionLogFilePath);
        return getEntriesFromJson(entryNumber, inputFile, transactionLogMaxCachedFileSize);
    }

    public static Optional<TransactionLogEntries> getEntriesFromJson(long entryNumber, TrinoInputFile inputFile, DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        try {
            inputFile.length(); // File length is cached and used in TransactionLogEntries
        }
        catch (FileNotFoundException e) {
            return Optional.empty();  // end of tail
        }
        return Optional.of(new TransactionLogEntries(entryNumber, inputFile, transactionLogMaxCachedFileSize));
    }

    public List<DeltaLakeTransactionLogEntry> getFileEntries(TrinoFileSystem fileSystem)
    {
        return entries.stream()
                .map(Transaction::transactionEntries)
                .flatMap(logEntries -> logEntries.getEntriesList(fileSystem).stream())
                .collect(toImmutableList());
    }

    public List<Transaction> getTransactions()
    {
        return entries;
    }

    public long getVersion()
    {
        return version;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SIZE_OF_LONG
                + estimatedSizeOf(entries, Transaction::getRetainedSizeInBytes);
    }
}
