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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.TRANSACTION_LOG_DIRECTORY;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.extractCommitVersion;
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

    // Same as loadNewTail, but finds the commits with one listing of _delta_log and reads them on the executor
    public static TransactionLogTail loadNewTailWithListing(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion,
            Optional<Long> endVersion,
            DataSize transactionLogMaxCachedFileSize,
            Executor executor)
            throws IOException
    {
        if (startVersion.isPresent() && endVersion.isPresent() && startVersion.get().equals(endVersion.get())) {
            // This is time travel to a specific checkpoint. No need to read transaction log files.
            return new TransactionLogTail(ImmutableList.of(), startVersion.get());
        }

        if (endVersion.isPresent()) {
            if (startVersion.isEmpty()) {
                // Without a start version the head ends at the first gap, which may be far below the end version, so a listing is not bounded
                return loadNewTailBackward(fileSystem, tableLocation, startVersion, endVersion.get(), transactionLogMaxCachedFileSize);
            }
            return loadNewTailBackwardWithListing(fileSystem, getTransactionLogDir(tableLocation), startVersion.get(), endVersion.get(), transactionLogMaxCachedFileSize, executor);
        }

        String transactionLogDir = getTransactionLogDir(tableLocation);
        long version = startVersion.orElse(0L);
        long firstVersion = startVersion.map(start -> start + 1).orElse(0L);
        // Most refreshes find no new commit, and a single existence check is cheaper than a listing
        if (!exists(fileSystem.newInputFile(getTransactionLogJsonEntryPath(transactionLogDir, firstVersion)))) {
            return new TransactionLogTail(ImmutableList.of(), version);
        }

        Map<Long, FileEntry> commitFiles = listCommitFiles(fileSystem, transactionLogDir, firstVersion, Long.MAX_VALUE);
        if (!commitFiles.containsKey(firstVersion)) {
            throw new MissingTransactionLogException(getTransactionLogJsonEntryPath(transactionLogDir, firstVersion).toString());
        }
        ImmutableList.Builder<CommitFile> tailFiles = ImmutableList.builder();
        for (long entryNumber = firstVersion; commitFiles.containsKey(entryNumber); entryNumber++) {
            tailFiles.add(new CommitFile(entryNumber, commitFiles.get(entryNumber)));
            version = entryNumber;
        }
        return new TransactionLogTail(readTransactions(fileSystem, tailFiles.build(), transactionLogMaxCachedFileSize, executor), version);
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
        return Optional.of(TransactionLogEntries.forCommit(entryNumber, inputFile, transactionLogMaxCachedFileSize));
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
     * the {@code startVersion} is the first entry number we want to load, but it is not guaranteed to be the first entry in the transaction log.
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
        // TODO use listFilesStartingFrom to enumerate the log tail in a single listing instead of one InputFile.length per commit
        while (true) {
            Optional<TransactionLogEntries> results = getEntriesFromJson(entryNumber, fileSystem.newInputFile(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber)), transactionLogMaxCachedFileSize);
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
            Optional<TransactionLogEntries> results = getEntriesFromJson(entryNumber, fileSystem.newInputFile(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber)), transactionLogMaxCachedFileSize);
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

    private static TransactionLogTail loadNewTailBackwardWithListing(
            TrinoFileSystem fileSystem,
            String transactionLogDir,
            long startVersion,
            long endVersion,
            DataSize transactionLogMaxCachedFileSize,
            Executor executor)
            throws IOException
    {
        Map<Long, FileEntry> commitFiles = listCommitFiles(fileSystem, transactionLogDir, Math.max(startVersion + 1, 0), endVersion);
        ImmutableList.Builder<CommitFile> headFiles = ImmutableList.builder();
        long entryNumber = endVersion;
        while (entryNumber > startVersion && entryNumber >= 0) {
            FileEntry commitFile = commitFiles.get(entryNumber);
            if (commitFile == null) {
                if (entryNumber > startVersion + 1) {
                    throw new MissingTransactionLogException(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber).toString());
                }
                break;
            }
            headFiles.add(new CommitFile(entryNumber, commitFile));
            entryNumber--;
        }
        return new TransactionLogTail(readTransactions(fileSystem, headFiles.build().reversed(), transactionLogMaxCachedFileSize, executor), endVersion);
    }

    private static boolean exists(TrinoInputFile inputFile)
            throws IOException
    {
        try {
            inputFile.length();
            return true;
        }
        catch (FileNotFoundException e) {
            return false;
        }
    }

    private static Map<Long, FileEntry> listCommitFiles(TrinoFileSystem fileSystem, String transactionLogDir, long firstVersion, long lastVersion)
            throws IOException
    {
        ImmutableMap.Builder<Long, FileEntry> commitFiles = ImmutableMap.builder();
        FileIterator files = fileSystem.listFilesStartingFrom(Location.of(transactionLogDir), "%020d".formatted(firstVersion));
        // Stop once every version in a bounded range is found, so that reading an old version does not page through the rest of the log
        long found = 0;
        while (found <= lastVersion - firstVersion && files.hasNext()) {
            FileEntry file = files.next();
            // Listing is recursive, so skip files in subdirectories such as _sidecars
            if (!file.location().parentDirectory().fileName().equals(TRANSACTION_LOG_DIRECTORY)) {
                continue;
            }
            OptionalLong commitVersion = extractCommitVersion(file.location().fileName());
            if (commitVersion.isPresent() && commitVersion.orElseThrow() >= firstVersion && commitVersion.orElseThrow() <= lastVersion) {
                commitFiles.put(commitVersion.orElseThrow(), file);
                found++;
            }
        }
        return commitFiles.buildOrThrow();
    }

    private static List<Transaction> readTransactions(TrinoFileSystem fileSystem, List<CommitFile> commitFiles, DataSize transactionLogMaxCachedFileSize, Executor executor)
    {
        List<ListenableFuture<Transaction>> transactions = commitFiles.stream()
                .map(commitFile -> Futures.submit(() -> readTransaction(fileSystem, commitFile, transactionLogMaxCachedFileSize), executor))
                .collect(toImmutableList());
        try {
            return getFutureValue(allAsList(transactions));
        }
        finally {
            transactions.forEach(transaction -> transaction.cancel(true));
        }
    }

    private static Transaction readTransaction(TrinoFileSystem fileSystem, CommitFile commitFile, DataSize transactionLogMaxCachedFileSize)
    {
        TrinoInputFile inputFile = fileSystem.newInputFile(commitFile.file().location(), commitFile.file().length());
        return new Transaction(commitFile.version(), TransactionLogEntries.forCommit(commitFile.version(), inputFile, transactionLogMaxCachedFileSize));
    }

    private record CommitFile(long version, FileEntry file)
    {
        public CommitFile
        {
            requireNonNull(file, "file is null");
        }
    }
}
