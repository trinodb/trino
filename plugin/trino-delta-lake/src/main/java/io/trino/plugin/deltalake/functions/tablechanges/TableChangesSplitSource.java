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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdfFileEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_TABLE;
import static io.trino.plugin.deltalake.functions.tablechanges.FileSource.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.FileSource.DATA_FILE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static java.util.Objects.requireNonNull;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final TrinoFileSystem fileSystem;
    private final String tableLocation;
    private final long endVersion;
    private final AtomicBoolean isFinished;
    private AtomicLong currentVersion;

    public TableChangesSplitSource(
            ConnectorSession session,
            DeltaLakeMetastore metastore,
            TrinoFileSystemFactory fileSystemFactory,
            TableChangesFunctionTableHandle functionHandle)
    {
        requireNonNull(functionHandle, "functionHandle is null");
        SchemaTableName schemaTableName = new SchemaTableName(functionHandle.getSchemaName(), functionHandle.getTableName());
        this.tableLocation = requireNonNull(metastore, "metastore is null").getTableLocation(schemaTableName, session);
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null").create(session);
        currentVersion = new AtomicLong(functionHandle.getStartVersion());
        endVersion = functionHandle.getEndVersion();
        isFinished = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long processedVersion = currentVersion.getAndIncrement();
        if (processedVersion <= endVersion) {
            Path transactionLogDir = getTransactionLogDir(new Path(tableLocation));
            try {
                Optional<List<DeltaLakeTransactionLogEntry>> entriesFromJson = getEntriesFromJson(processedVersion, transactionLogDir, fileSystem);
                if (entriesFromJson.isPresent()) {
                    List<DeltaLakeTransactionLogEntry> entries = entriesFromJson.get();
                    if (!entries.isEmpty()) {
                        List<CommitInfoEntry> commitInfoEntries = entries.stream()
                                .filter(entry -> entry.getCommitInfo() != null)
                                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                                .collect(toImmutableList());
                        checkState(commitInfoEntries.size() == 1, "There must be exactly 1 commitInfo present in a medatada file");
                        CommitInfoEntry commitInfo = commitInfoEntries.get(0);
                        List<ConnectorSplit> result;
                        if (commitInfo.getOperation().equalsIgnoreCase("merge")) {
                            result = entries.stream()
                                    .map(DeltaLakeTransactionLogEntry::getCDC)
                                    .filter(Objects::nonNull)
                                    .map(cdfFileEntry -> mapToDeltaLakeTableChangesSplit(commitInfo, cdfFileEntry, CDF_FILE, CdfFileEntry::getPath, CdfFileEntry::getCanonicalPartitionValues))
                                    .collect(toImmutableList());
                        }
                        else {
                            ImmutableList.Builder<AddFileEntry> addFileEntries = ImmutableList.builder();
                            ImmutableList.Builder<CdfFileEntry> cdfFileEntries = ImmutableList.builder();
                            entries.forEach(entry -> {
                                if (entry.getAdd() != null) {
                                    addFileEntries.add(entry.getAdd());
                                }
                                else if (entry.getCDC() != null) {
                                    cdfFileEntries.add(entry.getCDC());
                                }
                            });
                            Stream<ConnectorSplit> addEntriesSplits = addFileEntries.build().stream()
                                    .map(addEntry -> mapToDeltaLakeTableChangesSplit(commitInfo, addEntry, DATA_FILE, AddFileEntry::getPath, AddFileEntry::getCanonicalPartitionValues));
                            Stream<ConnectorSplit> cdfEntriesSplits = cdfFileEntries.build().stream()
                                    .map(cdfFileEntry -> mapToDeltaLakeTableChangesSplit(commitInfo, cdfFileEntry, CDF_FILE, CdfFileEntry::getPath, CdfFileEntry::getCanonicalPartitionValues));
                            result = Stream.concat(addEntriesSplits, cdfEntriesSplits).collect(toImmutableList());
                        }

                        return CompletableFuture.completedFuture(new ConnectorSplitBatch(result, isFinished.get()));
                    }
                }
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table metadata", e);
            }
        }
        isFinished.set(true);
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
    }

    private <T> DeltaLakeTableChangesSplit mapToDeltaLakeTableChangesSplit(
            CommitInfoEntry commitInfoEntry,
            T entry,
            FileSource source,
            Function<T, String> getPath,
            Function<T, Map<String, Optional<String>>> getCanonicalPartitionValues)
    {
        String path = tableLocation + "/" + getPath.apply(entry);
        TrinoInputFile inputFile = fileSystem.newInputFile(path);
        try {
            long length = inputFile.length();
            return new DeltaLakeTableChangesSplit(
                    path,
                    0,
                    length,
                    length,
                    inputFile.modificationTime(),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    getCanonicalPartitionValues.apply(entry),
                    commitInfoEntry.getTimestamp(),
                    source,
                    commitInfoEntry.getVersion());
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_INVALID_TABLE, "Failed to access table metadata: " + path, e);
        }
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished()
    {
        return isFinished.get();
    }
}
