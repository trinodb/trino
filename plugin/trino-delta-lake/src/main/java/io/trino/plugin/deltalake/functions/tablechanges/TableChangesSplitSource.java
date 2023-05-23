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
import io.trino.filesystem.Location;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_BAD_DATA;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.CDF_FILE;
import static io.trino.plugin.deltalake.functions.tablechanges.TableChangesFileType.DATA_FILE;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class TableChangesSplitSource
        implements ConnectorSplitSource
{
    private final TrinoFileSystem fileSystem;
    private final String tableLocation;
    private final AtomicLong currentVersion;
    private final long tableReadVersion;
    private final String transactionLogDir;

    public TableChangesSplitSource(
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            TableChangesTableFunctionHandle functionHandle)
    {
        tableLocation = functionHandle.tableLocation();
        fileSystem = fileSystemFactory.create(session);
        String logDir = getTransactionLogDir(tableLocation);
        currentVersion = new AtomicLong(extractCurrentVersion(functionHandle, logDir, fileSystem));
        tableReadVersion = functionHandle.tableReadVersion();
        transactionLogDir = logDir;
    }

    private static long extractCurrentVersion(TableChangesTableFunctionHandle functionHandle, String transactionLogDir, TrinoFileSystem fileSystem)
    {
        if (functionHandle.firstReadVersion().isPresent()) {
            return functionHandle.firstReadVersion().get();
        }
        long currentVersion = functionHandle.tableReadVersion();
        while (currentVersion >= 0) {
            Location transactionLogFilePath = getTransactionLogJsonEntryPath(transactionLogDir, currentVersion);
            try {
                if (fileSystem.newInputFile(transactionLogFilePath).exists()) {
                    List<DeltaLakeTransactionLogEntry> entries = getEntries(currentVersion, transactionLogDir, fileSystem);
                    if (entries.isEmpty()) {
                        throw new TrinoException(DELTA_LAKE_BAD_DATA, "Delta log should not contain empty entry");
                    }
                    CommitInfoEntry commitInfo = extractCommitInfoEntry(entries);
                    if (commitInfo.getTimestamp() < functionHandle.firstReadTimestamp().get()) {
                        return currentVersion + 1;
                    }
                }
            }
            catch (IOException exception) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to access table metadata", exception);
            }
            currentVersion--;
        }
        return 0; // provided sinceTimestamp is so much in the past that we need to read entire table
    }

    private static List<DeltaLakeTransactionLogEntry> getEntries(long currentVersion, String transactionLogDir, TrinoFileSystem fileSystem)
    {
        try {
            return getEntriesFromJson(currentVersion, transactionLogDir, fileSystem)
                    .orElseThrow(() -> new TrinoException(DELTA_LAKE_BAD_DATA, "Delta Lake log entries are missing for version " + currentVersion));
        }
        catch (IOException e) {
            throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR, "Failed to access table metadata", e);
        }
    }

    private static CommitInfoEntry extractCommitInfoEntry(List<DeltaLakeTransactionLogEntry> entries)
    {
        return entries.stream()
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize) // TODO dont ignore maxSize https://github.com/trinodb/trino/issues/17182
    {
        long processedVersion = currentVersion.getAndIncrement();
        if (processedVersion > tableReadVersion) {
            return CompletableFuture.completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
        }
        List<DeltaLakeTransactionLogEntry> entries = getEntries(processedVersion, transactionLogDir, fileSystem);
        if (entries.isEmpty()) {
            return CompletableFuture.completedFuture(new ConnectorSplitBatch(ImmutableList.of(), false));
        }
        CommitInfoEntry commitInfo = extractCommitInfoEntry(entries);
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
            if (entry.getRemove() != null && entry.getRemove().isDataChange()) {
                containsRemoveEntry = true;
            }
        }
        if (containsRemoveEntry && !containsCdcEntry) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Change Data Feed is not enabled at version %d. Version contains 'remove' entries without 'cdc' entries", processedVersion));
        }
        if (!containsRemoveEntry) {
            for (DeltaLakeTransactionLogEntry entry : entries) {
                if (entry.getAdd() != null && entry.getAdd().isDataChange()) {
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
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, processedVersion == tableReadVersion));
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
                commitInfoEntry.getTimestamp(),
                source,
                commitInfoEntry.getVersion());
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished()
    {
        return currentVersion.get() > tableReadVersion;
    }
}
