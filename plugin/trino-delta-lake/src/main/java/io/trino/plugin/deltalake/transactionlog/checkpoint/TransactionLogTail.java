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
import io.trino.plugin.deltalake.transactionlog.MissingTransactionLogException;
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
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
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

    // Load a section of the Transaction Log JSON entries. Optionally from a given start version (exclusive) through an end version (inclusive)
    public static TransactionLogTail loadNewTail(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        if (startVersion.isPresent() && endVersion.isPresent() && startVersion.get().equals(endVersion.get())) {
            // This is time travel to a specific checkpoint. No need to read transaction log files.
            return new TransactionLogTail(ImmutableList.of(), startVersion.get());
        }

        if (endVersion.isPresent()) {
            return loadNewTailBackward(fileSystem, tableLocation, startVersion, endVersion.get(), transactionLogMaxCachedFileSize);
        }

        if (startVersion.isPresent()) {
            return loadNewTail(fileSystem, tableLocation, startVersion.get(), startVersion.get() + 1, transactionLogMaxCachedFileSize);
        }

        return loadNewTail(fileSystem, tableLocation, 0L, 0L, transactionLogMaxCachedFileSize);
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

    /**
     * Loads a section of the Transaction Log JSON entries starting from {@code startVersion} (inclusive) up to the latest version.
     *
     * the {@code version} is the latest table version, which is the last entry number in the transaction log we already know,
     * the {@code starVersion} is the first entry number we want to load, but it is not guaranteed to be the first entry in the transaction log.
     */
    private static TransactionLogTail loadNewTail(
            TrinoFileSystem fileSystem,
            String tableLocation,
            long version,
            long startVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        ImmutableList.Builder<Transaction> entriesBuilder = ImmutableList.builder();
        String transactionLogDir = getTransactionLogDir(tableLocation);

        long entryNumber = startVersion;
        while (true) {
            Optional<TransactionLogEntries> results = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, transactionLogMaxCachedFileSize);
            if (results.isEmpty()) {
                break;
            }

            entriesBuilder.add(new Transaction(entryNumber, results.get()));
            version = entryNumber;
            entryNumber++;
        }

        return new TransactionLogTail(entriesBuilder.build(), version);
    }

    // Load a section of the Transaction Log JSON entries. Optionally from a given end version (inclusive) through a start version (exclusive)
    private static TransactionLogTail loadNewTailBackward(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion,
            long endVersion,
            DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        ImmutableList.Builder<Transaction> transactionsBuilder = ImmutableList.builder();
        String transactionLogDir = getTransactionLogDir(tableLocation);

        long version = endVersion;
        long entryNumber = version;
        boolean endOfHead = false;

        while (!endOfHead) {
            Optional<TransactionLogEntries> results = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, transactionLogMaxCachedFileSize);
            if (results.isPresent()) {
                transactionsBuilder.add(new Transaction(entryNumber, results.get()));
                version = entryNumber;
                entryNumber--;
            }
            else {
                // When there is a gap in the transaction log version, indicate the end of the current head
                endOfHead = true;
                if (startVersion.isPresent() && entryNumber > startVersion.get() + 1) {
                    throw new MissingTransactionLogException(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber).toString());
                }
            }
            if ((startVersion.isPresent() && version == startVersion.get() + 1) || entryNumber < 0) {
                endOfHead = true;
            }
        }
        return new TransactionLogTail(transactionsBuilder.build().reversed(), endVersion);
    }
}
