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

import static com.google.common.base.Preconditions.checkArgument;
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

    private TransactionLogTail(List<Transaction> entries, long version)
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
        ImmutableList.Builder<Transaction> entriesBuilder = ImmutableList.builder();

        if (startVersion.isPresent() && endVersion.isPresent() && startVersion.get().equals(endVersion.get())) {
            // This is time travel to a specific checkpoint. No need to read transaction log files.
            return new TransactionLogTail(entriesBuilder.build(), startVersion.get());
        }

        long version = startVersion.orElse(0L);
        long entryNumber = startVersion.map(start -> start + 1).orElse(0L);
        checkArgument(endVersion.isEmpty() || entryNumber <= endVersion.get(), "Invalid start/end versions: %s, %s", startVersion, endVersion);

        String transactionLogDir = getTransactionLogDir(tableLocation);

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

    public Optional<TransactionLogTail> getUpdatedTail(TrinoFileSystem fileSystem, String tableLocation, Optional<Long> endVersion, DataSize transactionLogMaxCachedFileSize)
            throws IOException
    {
        checkArgument(endVersion.isEmpty() || endVersion.get() > version, "Invalid endVersion, expected higher than %s, but got %s", version, endVersion);
        TransactionLogTail newTail = loadNewTail(fileSystem, tableLocation, Optional.of(version), endVersion, transactionLogMaxCachedFileSize);
        if (newTail.version == version) {
            return Optional.empty();
        }
        return Optional.of(new TransactionLogTail(
                ImmutableList.<Transaction>builder()
                        .addAll(entries)
                        .addAll(newTail.entries)
                        .build(),
                newTail.version));
    }

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
