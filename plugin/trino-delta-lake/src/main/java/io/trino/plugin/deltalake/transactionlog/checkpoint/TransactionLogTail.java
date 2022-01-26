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
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.parseJson;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class TransactionLogTail
{
    private static final int JSON_LOG_ENTRY_READ_BUFFER_SIZE = 1024 * 1024;

    private final List<DeltaLakeTransactionLogEntry> entries;
    private final long version;

    private TransactionLogTail(List<DeltaLakeTransactionLogEntry> entries, long version)
    {
        this.entries = ImmutableList.copyOf(requireNonNull(entries, "entries is null"));
        this.version = version;
    }

    public static TransactionLogTail loadNewTail(
            FileSystem fileSystem,
            Path tableLocation,
            Optional<Long> startVersion)
            throws IOException
    {
        return loadNewTail(fileSystem, tableLocation, startVersion, Optional.empty());
    }

    // Load a section of the Transaction Log JSON entries. Optionally from a given start version (exclusive) through an end version (inclusive)
    public static TransactionLogTail loadNewTail(
            FileSystem fileSystem,
            Path tableLocation,
            Optional<Long> startVersion,
            Optional<Long> endVersion)
            throws IOException
    {
        ImmutableList.Builder<DeltaLakeTransactionLogEntry> entriesBuilder = ImmutableList.builder();

        long version = startVersion.orElse(0L);
        long entryNumber = startVersion.map(start -> start + 1).orElse(0L);

        Path transactionLogDir = getTransactionLogDir(tableLocation);
        Optional<List<DeltaLakeTransactionLogEntry>> results;

        boolean endOfTail = false;
        while (!endOfTail) {
            results = getEntriesFromJson(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber), fileSystem);
            if (results.isPresent()) {
                entriesBuilder.addAll(results.get());
                version = entryNumber;
                entryNumber++;
            }
            else {
                endOfTail = true;
            }

            if (endVersion.isPresent() && version == endVersion.get()) {
                endOfTail = true;
            }
        }

        return new TransactionLogTail(entriesBuilder.build(), version);
    }

    public Optional<TransactionLogTail> getUpdatedTail(FileSystem fileSystem, Path tableLocation)
            throws IOException
    {
        ImmutableList.Builder<DeltaLakeTransactionLogEntry> entriesBuilder = ImmutableList.builder();

        long newVersion = version;

        Optional<List<DeltaLakeTransactionLogEntry>> results;
        Path transactionLogDir = getTransactionLogDir(tableLocation);
        boolean endOfTail = false;
        while (!endOfTail) {
            results = getEntriesFromJson(getTransactionLogJsonEntryPath(transactionLogDir, newVersion + 1), fileSystem);
            if (results.isPresent()) {
                if (version == newVersion) {
                    // initialize entriesBuilder with entries we have already read
                    entriesBuilder.addAll(entries);
                }
                entriesBuilder.addAll(results.get());
                newVersion++;
            }
            else {
                endOfTail = true;
            }
        }

        if (newVersion == version) {
            return Optional.empty();
        }
        return Optional.of(new TransactionLogTail(entriesBuilder.build(), newVersion));
    }

    public static Optional<List<DeltaLakeTransactionLogEntry>> getEntriesFromJson(Path transactionLogFilePath, FileSystem fileSystem)
            throws IOException
    {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fileSystem.open(transactionLogFilePath), UTF_8),
                JSON_LOG_ENTRY_READ_BUFFER_SIZE)) {
            ImmutableList.Builder<DeltaLakeTransactionLogEntry> resultsBuilder = ImmutableList.builder();
            String line = reader.readLine();
            while (line != null) {
                resultsBuilder.add(parseJson(line));
                line = reader.readLine();
            }

            return Optional.of(resultsBuilder.build());
        }
        catch (IOException e) {
            if (isFileNotFoundException(e)) {
                return Optional.empty();  // end of tail
            }
            throw new IOException(e);
        }
    }

    public static boolean isFileNotFoundException(IOException e)
    {
        if (e instanceof FileNotFoundException) {
            return true;
        }
        else if (e.getMessage().contains("The specified key does not exist")) {
            return true;
        }
        else {
            return false;
        }
    }

    public List<DeltaLakeTransactionLogEntry> getFileEntries()
    {
        return entries;
    }

    public long getVersion()
    {
        return version;
    }
}
