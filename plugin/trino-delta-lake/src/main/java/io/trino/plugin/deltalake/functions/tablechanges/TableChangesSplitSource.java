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
import io.trino.filesystem.Locations;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.CdcEntry;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.DeltaLakeConfig.DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.DATA_FILE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final String tableLocation;
    private final Iterator<ConnectorSplit> splits;

    public TableChangesSplitSource(
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            TableChangesTableFunctionHandle functionHandle)
    {
        tableLocation = functionHandle.tableLocation();
        splits = prepareSplits(
                functionHandle.firstReadVersion(),
                functionHandle.tableReadVersion(),
                getTransactionLogDir(functionHandle.tableLocation()),
                fileSystemFactory.create(session))
                .iterator();
    }

    private Stream<ConnectorSplit> prepareSplits(long currentVersion, long tableReadVersion, String transactionLogDir, TrinoFileSystem fileSystem)
    {
        return LongStream.range(currentVersion, tableReadVersion + 1)
                .boxed()
                .flatMap(version -> {
                    try {
                        List<DeltaLakeTransactionLogEntry> entries = getEntriesFromJson(version, transactionLogDir, fileSystem, DEFAULT_TRANSACTION_LOG_MAX_CACHED_SIZE)
                                .orElseThrow(() -> new TrinoException(DELTA_LAKE_BAD_DATA, "Delta Lake log entries are missing for version " + version))
                                .getEntriesList(fileSystem);
                        List<CommitInfoEntry> commitInfoEntries = entries.stream()
                                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                                .filter(Objects::nonNull)
                                .collect(toImmutableList());
                        if (commitInfoEntries.size() != 1) {
                            throw new TrinoException(DELTA_LAKE_BAD_DATA, "There should be exactly 1 commitInfo present in a metadata file");
                        }
                        CommitInfoEntry commitInfo = getOnlyElement(commitInfoEntries);

                        List<ConnectorSplit> splits = new ArrayList<>();
                        boolean containsCdcEntry = false;
                        boolean containsRemoveEntry = false;
                        for (DeltaLakeTransactionLogEntry entry : entries) {
                            CdcEntry cdcEntry = entry.getCDC();
                            if (cdcEntry != null) {
                                containsCdcEntry = true;
                                splits.add(mapToDeltaLakeTableChangesSplit(
                                        commitInfo,
                                        CDF_FILE,
                                        cdcEntry.getSize(),
                                        cdcEntry.getPath(),
                                        cdcEntry.getCanonicalPartitionValues()));
                            }
                            if (entry.getRemove() != null && entry.getRemove().dataChange()) {
                                containsRemoveEntry = true;
                            }
                        }
                        if (containsRemoveEntry && !containsCdcEntry) {
                            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Change Data Feed is not enabled at version %d. Version contains 'remove' entries without 'cdc' entries", version));
                        }
                        if (!containsRemoveEntry) {
                            for (DeltaLakeTransactionLogEntry entry : entries) {
                                if (entry.getAdd() != null && entry.getAdd().isDataChange()) {
                                    // paths can be absolute as well in case of shallow-cloned tables
                                    AddFileEntry addEntry = entry.getAdd();
                                    splits.add(mapToDeltaLakeTableChangesSplit(
                                            commitInfo,
                                            DATA_FILE,
                                            addEntry.getSize(),
                                            addEntry.getPath(),
                                            addEntry.getCanonicalPartitionValues()));
                                }
                            }
                        }
                        return splits.stream();
                    }
                    catch (IOException e) {
                        throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to access table metadata", e);
                    }
                });
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        ImmutableList.Builder<ConnectorSplit> result = ImmutableList.builder();
        int i = 0;
        while (i < maxSize && splits.hasNext()) {
            result.add(splits.next());
            i++;
        }
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(result.build(), isFinished()));
    }

    private TableChangesSplit mapToDeltaLakeTableChangesSplit(
            CommitInfoEntry commitInfoEntry,
            TableChangesFileType source,
            long length,
            String entryPath,
            Map<String, Optional<String>> canonicalPartitionValues)
    {
        String path = Locations.appendPath(tableLocation, entryPath);
        return new TableChangesSplit(
                path,
                length,
                canonicalPartitionValues,
                commitInfoEntry.timestamp(),
                source,
                commitInfoEntry.version());
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished()
    {
        return !splits.hasNext();
    }
}
