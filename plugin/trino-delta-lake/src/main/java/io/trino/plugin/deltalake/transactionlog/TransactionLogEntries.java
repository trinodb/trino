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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.deltalake.transactionlog.checkpoint.MetadataAndProtocolEntries;
import io.trino.spi.TrinoException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.parseJson;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class TransactionLogEntries
{
    private static final int INSTANCE_SIZE = instanceSize(TransactionLogEntries.class);
    private static final int JSON_LOG_ENTRY_READ_BUFFER_SIZE = 1024 * 1024;

    private final long entryNumber;
    private final Location transactionLogFilePath;

    private final Optional<List<DeltaLakeTransactionLogEntry>> cachedEntries;

    public TransactionLogEntries(long entryNumber, TrinoInputFile inputFile, DataSize maxCachedFileSize)
    {
        this.entryNumber = entryNumber;
        this.transactionLogFilePath = inputFile.location();
        try {
            if (inputFile.length() > maxCachedFileSize.toBytes()) {
                this.cachedEntries = Optional.empty();
            }
            else {
                this.cachedEntries = Optional.of(ImmutableList.copyOf(new TransactionLogEntryIterator(entryNumber, inputFile)));
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error while reading from transaction entry iterator for the file %s".formatted(transactionLogFilePath));
        }
    }

    /**
     * Returns a stream of DeltaLakeTransactionLogEntry
     * Caller has the responsibility to close this stream as it potentially holds an open file
     */
    public Stream<DeltaLakeTransactionLogEntry> getEntries(TrinoFileSystem fileSystem)
    {
        if (cachedEntries.isPresent()) {
            return cachedEntries.get().stream();
        }
        TransactionLogEntryIterator iterator = new TransactionLogEntryIterator(entryNumber, fileSystem.newInputFile(transactionLogFilePath));
        return stream(iterator).onClose(iterator::close);
    }

    public List<DeltaLakeTransactionLogEntry> getEntriesList(TrinoFileSystem fileSystem)
    {
        try (Stream<DeltaLakeTransactionLogEntry> jsonStream = getEntries(fileSystem)) {
            return jsonStream.collect(toImmutableList());
        }
    }

    public MetadataAndProtocolEntries getMetadataAndProtocol(TrinoFileSystem fileSystem)
    {
        // There can be at most one metadata and protocol entry per transaction log
        // We use that stop reading from file when a metadata and protocol entry are found
        try (Stream<DeltaLakeTransactionLogEntry> logEntryStream = getEntries(fileSystem)) {
            Optional<MetadataEntry> metadataEntry = Optional.empty();
            Optional<ProtocolEntry> protocolEntry = Optional.empty();
            for (Iterator<DeltaLakeTransactionLogEntry> it = logEntryStream.iterator(); it.hasNext(); ) {
                DeltaLakeTransactionLogEntry transactionLogEntry = it.next();
                if (transactionLogEntry.getMetaData() != null) {
                    metadataEntry = Optional.of(transactionLogEntry.getMetaData());
                }
                else if (transactionLogEntry.getProtocol() != null) {
                    protocolEntry = Optional.of(transactionLogEntry.getProtocol());
                }

                if (protocolEntry.isPresent() && metadataEntry.isPresent()) {
                    break;
                }
            }
            return new MetadataAndProtocolEntries(metadataEntry, protocolEntry);
        }
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SIZE_OF_LONG
                + estimatedSizeOf(transactionLogFilePath.path())
                + sizeOf(cachedEntries, entries -> estimatedSizeOf(entries, DeltaLakeTransactionLogEntry::getRetainedSizeInBytes));
    }

    private static final class TransactionLogEntryIterator
            extends AbstractIterator<DeltaLakeTransactionLogEntry>
    {
        private final long entryNumber;
        private final Location location;
        private final BufferedReader reader;

        public TransactionLogEntryIterator(long entryNumber, TrinoInputFile inputFile)
        {
            this.entryNumber = entryNumber;
            this.location = inputFile.location();
            TrinoInputStream inputStream;
            try {
                inputStream = inputFile.newStream();
            }
            catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error while initializing the transaction entry iterator for the file %s".formatted(inputFile.location()));
            }
            this.reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8), JSON_LOG_ENTRY_READ_BUFFER_SIZE);
        }

        @Override
        protected DeltaLakeTransactionLogEntry computeNext()
        {
            String line;
            try {
                line = reader.readLine();
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error while reading from transaction entry iterator for the file %s".formatted(location));
            }
            if (line == null) {
                close();
                return endOfData();
            }
            DeltaLakeTransactionLogEntry deltaLakeTransactionLogEntry = parseJson(line);
            if (deltaLakeTransactionLogEntry.getCommitInfo() != null && deltaLakeTransactionLogEntry.getCommitInfo().version() == 0L) {
                // In case that the commit info version is missing, use the version from the transaction log file name
                deltaLakeTransactionLogEntry = deltaLakeTransactionLogEntry.withCommitInfo(deltaLakeTransactionLogEntry.getCommitInfo().withVersion(entryNumber));
            }
            return deltaLakeTransactionLogEntry;
        }

        public void close()
        {
            try {
                reader.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
