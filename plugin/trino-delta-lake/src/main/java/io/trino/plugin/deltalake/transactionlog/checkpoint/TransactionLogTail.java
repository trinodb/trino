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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.MissingTransactionLogException;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.Transaction;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.parseJson;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TransactionLogTail
{
    private static final int JSON_LOG_ENTRY_READ_BUFFER_SIZE = 1024 * 1024;

    private final List<Transaction> entries;
    private final long version;

    private final Optional<MetadataEntry> metadataEntry;
    private final Optional<ProtocolEntry> protocolEntry;

    private TransactionLogTail(List<Transaction> entries, long version, Optional<MetadataEntry> metadataEntry, Optional<ProtocolEntry> protocolEntry)
    {
        this.entries = ImmutableList.copyOf(requireNonNull(entries, "entries is null"));
        this.version = version;
        this.metadataEntry = metadataEntry;
        this.protocolEntry = protocolEntry;
    }

    public static TransactionLogTail loadNewTail(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion)
            throws IOException
    {
        return loadNewTail(fileSystem, tableLocation, startVersion, Optional.empty());
    }

    // Load a section of the Transaction Log JSON entries. Optionally from a given start version (exclusive) through an end version (inclusive)
    public static TransactionLogTail loadNewTail(
            TrinoFileSystem fileSystem,
            String tableLocation,
            Optional<Long> startVersion,
            Optional<Long> endVersion)
            throws IOException
    {
        ImmutableList.Builder<Transaction> entriesBuilder = ImmutableList.builder();

        long version = startVersion.orElse(0L);
        long entryNumber = startVersion.map(start -> start + 1).orElse(0L);
        checkArgument(endVersion.isEmpty() || entryNumber <= endVersion.get(), "Invalid start/end versions: %s, %s", startVersion, endVersion);

        String transactionLogDir = getTransactionLogDir(tableLocation);
        Optional<List<DeltaLakeTransactionLogEntry>> results;
        MetadataEntry metadataEntry = null;
        ProtocolEntry protocolEntry = null;

        boolean endOfTail = false;
        while (!endOfTail) {
            results = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem);
            if (results.isPresent()) {
                entriesBuilder.add(new Transaction(entryNumber, results.get()));
                // There is at most one metadata or protocol entry per file https://github.com/delta-io/delta/blob/d74cc6897730f4effb5d7272c21bd2554bdfacdb/PROTOCOL.md#delta-log-entries-1
                metadataEntry = results.get().stream().map(DeltaLakeTransactionLogEntry::getMetaData)
                        .filter(Objects::nonNull).findAny().orElse(metadataEntry);
                protocolEntry = results.get().stream().map(DeltaLakeTransactionLogEntry::getProtocol)
                        .filter(Objects::nonNull).findAny().orElse(protocolEntry);
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

        return new TransactionLogTail(entriesBuilder.build(), version, Optional.ofNullable(metadataEntry), Optional.ofNullable(protocolEntry));
    }

    public Optional<TransactionLogTail> getUpdatedTail(TrinoFileSystem fileSystem, String tableLocation, Optional<Long> endVersion)
            throws IOException
    {
        checkArgument(endVersion.isEmpty() || endVersion.get() > version, "Invalid endVersion, expected higher than %s, but got %s", version, endVersion);
        TransactionLogTail newTail = loadNewTail(fileSystem, tableLocation, Optional.of(version), endVersion);
        if (newTail.version == version) {
            return Optional.empty();
        }
        return Optional.of(new TransactionLogTail(
                ImmutableList.<Transaction>builder()
                        .addAll(entries)
                        .addAll(newTail.entries)
                        .build(),
                newTail.version,
                newTail.getMetadataEntry().or(() -> metadataEntry),
                newTail.getProtocolEntry().or(() -> protocolEntry)));
    }

    public static Optional<List<DeltaLakeTransactionLogEntry>> getEntriesFromJson(long entryNumber, String transactionLogDir, TrinoFileSystem fileSystem)
            throws IOException
    {
        Location transactionLogFilePath = getTransactionLogJsonEntryPath(transactionLogDir, entryNumber);
        TrinoInputFile inputFile = fileSystem.newInputFile(transactionLogFilePath);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputFile.newStream(), UTF_8),
                JSON_LOG_ENTRY_READ_BUFFER_SIZE)) {
            ImmutableList.Builder<DeltaLakeTransactionLogEntry> resultsBuilder = ImmutableList.builder();
            String line = reader.readLine();
            while (line != null) {
                DeltaLakeTransactionLogEntry deltaLakeTransactionLogEntry = parseJson(line);
                if (deltaLakeTransactionLogEntry.getCommitInfo() != null && deltaLakeTransactionLogEntry.getCommitInfo().getVersion() == 0L) {
                    // In case that the commit info version is missing, use the version from the transaction log file name
                    deltaLakeTransactionLogEntry = deltaLakeTransactionLogEntry.withCommitInfo(deltaLakeTransactionLogEntry.getCommitInfo().withVersion(entryNumber));
                }
                resultsBuilder.add(deltaLakeTransactionLogEntry);
                line = reader.readLine();
            }

            return Optional.of(resultsBuilder.build());
        }
        catch (FileNotFoundException e) {
            return Optional.empty();  // end of tail
        }
    }

    public List<DeltaLakeTransactionLogEntry> getFileEntries()
    {
        return entries.stream().map(Transaction::transactionEntries).flatMap(Collection::stream).collect(toImmutableList());
    }

    public List<Transaction> getTransactions()
    {
        return entries;
    }

    public Optional<MetadataEntry> getMetadataEntry()
    {
        return metadataEntry;
    }

    public Optional<ProtocolEntry> getProtocolEntry()
    {
        return protocolEntry;
    }

    public long getVersion()
    {
        return version;
    }
}
