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
package io.trino.plugin.deltalake.clustering;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.transactionlog.CommitInfoEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_FILESYSTEM_ERROR;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.TEMPORAL_TIME_TRAVEL_LINEAR_SEARCH_MAX_SIZE;
import static io.trino.plugin.deltalake.clustering.Operation.CLUSTER_BY;
import static io.trino.plugin.deltalake.clustering.Operation.CREATE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.MERGE;
import static io.trino.plugin.deltalake.clustering.Operation.OPTIMIZE;
import static io.trino.plugin.deltalake.clustering.Operation.RENAME_COLUMN;
import static io.trino.plugin.deltalake.clustering.Operation.REPLACE_TABLE_KEYWORD;
import static io.trino.plugin.deltalake.clustering.Operation.RESTORE;
import static io.trino.plugin.deltalake.clustering.Operation.UNKNOW_OPERATION;
import static io.trino.plugin.deltalake.clustering.Operation.WRITE;
import static io.trino.plugin.deltalake.clustering.Operation.fromString;
import static io.trino.plugin.deltalake.transactionlog.TemporalTimeTravelUtil.findLatestVersionUsingTemporal;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public final class ClusteringMetadataUtil
{
    private static final Logger LOG = Logger.get(ClusteringMetadataUtil.class);
    private static final DataSize TRANSACTION_LOG_MAX_CACHED_SIZE = DataSize.of(16, MEGABYTE);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final String CLUSTERING_PARAMETER_KEY = "clusterBy";
    private static final String NEW_CLUSTERING_PARAMETER_KEY = "newClusteringColumns";
    private static final String RENAMED_OLD_COLUMN_KEY = "oldColumnPath";
    private static final String RENAMED_NEW_COLUMN_KEY = "newColumnPath";

    private static final String RESTORE_VERSION_KEY = "version";
    private static final String RESTORE_TEMPORAL_KEY = "timestamp";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").withZone(UTC);
    private static final Executor restoredInfoExecutor = new BoundedExecutor(newDirectExecutorService(), 4);

    private static final Map<Operation, String> OPERATION_CLUSTEINFOKEY_MAP = ImmutableMap.of(
            WRITE, CLUSTERING_PARAMETER_KEY,
            MERGE, CLUSTERING_PARAMETER_KEY,
            OPTIMIZE, CLUSTERING_PARAMETER_KEY,
            CREATE_TABLE_KEYWORD, CLUSTERING_PARAMETER_KEY,
            REPLACE_TABLE_KEYWORD, CLUSTERING_PARAMETER_KEY,
            CLUSTER_BY, NEW_CLUSTERING_PARAMETER_KEY);

    private static final ThreadLocal<Map<String, String>> OLD_TO_NEW_RENAMED_COLUMNS = ThreadLocal.withInitial(HashMap::new);

    private ClusteringMetadataUtil()
    {
    }

    public static Optional<List<String>> getLatestClusteredColumns(TrinoFileSystem fileSystem, TableSnapshot tableSnapshot)
    {
        long currentVersion = getCurrentVersion(fileSystem, tableSnapshot, tableSnapshot.getVersion());
        Optional<CommitInfoEntry> commitInfoEntry;
        List<String> clusteredColumns = ImmutableList.of();
        while (currentVersion >= 0) {
            commitInfoEntry = extractCommitInfo(currentVersion, fileSystem, tableSnapshot);
            if (commitInfoEntry.isEmpty()) {
                break;
            }
            clusteredColumns = extractClusteredColumns(commitInfoEntry);
            Operation operation = getOperation(commitInfoEntry.get().operation());
            if (shouldStopLookup(operation, clusteredColumns)) {
                break;
            }
            if (operation == RESTORE) {
                currentVersion = getCurrentVersion(fileSystem, tableSnapshot, currentVersion);
            }
            else {
                currentVersion--;
            }
        }
        if (!clusteredColumns.isEmpty()) {
            clusteredColumns = clusteredColumns.stream()
                    .map(c -> OLD_TO_NEW_RENAMED_COLUMNS.get().getOrDefault(c, c))
                    .collect(toImmutableList());
        }
        OLD_TO_NEW_RENAMED_COLUMNS.remove();
        return Optional.of(clusteredColumns);
    }

    @VisibleForTesting
    static long getCurrentVersion(TrinoFileSystem fileSystem, TableSnapshot tableSnapshot, long currentVersion)
    {
        long version = currentVersion;
        CommitInfoEntry commitInfoEntry = extractCommitInfo(currentVersion, fileSystem, tableSnapshot)
                .orElseThrow(() -> new IllegalStateException("No commit info found for table at version " + tableSnapshot.getVersion()));
        if (getOperation(commitInfoEntry.operation()) == RESTORE) {
            version = getRestoreVersion(commitInfoEntry, fileSystem, tableSnapshot);
        }
        return version;
    }

    @VisibleForTesting
    static long getRestoreVersion(CommitInfoEntry commitInfoEntry, TrinoFileSystem fileSystem, TableSnapshot tableSnapshot)
    {
        if (getOperation(commitInfoEntry.operation()) != RESTORE) {
            throw new IllegalArgumentException("The provided commitInfoEntry is not of RESTORE operation");
        }
        long version;
        String restoredVersion = commitInfoEntry.operationParameters().get(RESTORE_VERSION_KEY);
        String restoredTimestamp = commitInfoEntry.operationParameters().get(RESTORE_TEMPORAL_KEY);
        if (!Strings.isNullOrEmpty(restoredVersion)) {
            version = Long.parseLong(restoredVersion);
        }
        else if (!Strings.isNullOrEmpty(restoredTimestamp)) {
            String tableLocation = tableSnapshot.getTableLocation();
            LocalDateTime localDateTime = LocalDateTime.parse(restoredTimestamp, TIME_FORMATTER);
            long epochMillis = localDateTime.toInstant(UTC).toEpochMilli(); // all timestamp recorded in commitInfoEntry are in UTC
            try {
                version = findLatestVersionUsingTemporal(fileSystem, tableLocation, epochMillis, restoredInfoExecutor, TEMPORAL_TIME_TRAVEL_LINEAR_SEARCH_MAX_SIZE);
            }
            catch (IOException e) {
                throw new TrinoException(DELTA_LAKE_FILESYSTEM_ERROR,
                        format("Unexpected IO exception occurred while reading the entries under the location %s for finding latest snapshot id before or at %s",
                                tableLocation, Instant.ofEpochMilli(epochMillis)), e);
            }
        }
        else {
            throw new IllegalArgumentException("Both restored version and timestamp are null or empty, should never happen");
        }
        return version;
    }

    @VisibleForTesting
    static Optional<CommitInfoEntry> extractCommitInfo(Long version, TrinoFileSystem fileSystem, TableSnapshot tableSnapshot)
    {
        Location transactionLogPath = getTransactionLogJsonEntryPath(getTransactionLogDir(tableSnapshot.getTableLocation()), version);
        TrinoInputFile inputFile = fileSystem.newInputFile(transactionLogPath);

        Stream<DeltaLakeTransactionLogEntry> transactionLogEntries;
        try {
            transactionLogEntries = getEntriesFromJson(version, inputFile, TRANSACTION_LOG_MAX_CACHED_SIZE).map(entries -> entries.getEntries(fileSystem))
                    // transaction log does not exist. Might have been expired.
                    .orElseGet(Stream::of);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Optional<CommitInfoEntry> commitInfoEntry = transactionLogEntries
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filter(Objects::nonNull)
                .findFirst();
        if (commitInfoEntry.isEmpty()) {
            LOG.error(String.format("No commit info found for table at version %d", version));
            throw new IllegalStateException(format("No commit info found for table at version %d", version));
        }
        return commitInfoEntry;
    }

    @VisibleForTesting
    static List<String> extractClusteredColumns(Optional<CommitInfoEntry> commitInfoEntry)
    {
        if (commitInfoEntry.isEmpty()) {
            return ImmutableList.of();
        }
        Operation operation = fromString(commitInfoEntry.get().operation());
        if (operation == RENAME_COLUMN) {
            recordRenamedColumns(commitInfoEntry.get());
        }
        if (operation == UNKNOW_OPERATION) {
            LOG.warn(String.format("Unknown operation: %s", commitInfoEntry.get().operation()));
        }
        String clusteredKey = OPERATION_CLUSTEINFOKEY_MAP.get(operation);
        String clusteredValue = commitInfoEntry.get().operationParameters().get(clusteredKey);
        if (Strings.isNullOrEmpty(clusteredValue)) {
            return ImmutableList.of();
        }
        return getClusteredColumnList(clusteredKey, clusteredValue);
    }

    @VisibleForTesting
    static boolean shouldStopLookup(Operation operation, List<String> clusteredColumns)
    {
        if (!clusteredColumns.isEmpty()) {
            return true;
        }
        if (operation == OPTIMIZE) {
            // this means this optimize could be triggered by Delete, Update, Merge, or Manually triggered Optimize
            // the action is just merging small files,
            // no clustering-info or partition-info is recorded
            // also, no clustered columns changed
            // so we should continue to look for the latest clustering info
            return false;
        }
        return OPERATION_CLUSTEINFOKEY_MAP.containsKey(operation);
    }

    @VisibleForTesting
    static Operation getOperation(String operationStr)
    {
        if (Strings.isNullOrEmpty(operationStr)) {
            throw new IllegalArgumentException("Operation parameter is empty");
        }
        return fromString(operationStr);
    }

    @VisibleForTesting
    static void recordRenamedColumns(CommitInfoEntry commitInfoEntry)
    {
        String oldName = commitInfoEntry.operationParameters().get(RENAMED_OLD_COLUMN_KEY);
        String newName = commitInfoEntry.operationParameters().get(RENAMED_NEW_COLUMN_KEY);
        if (Strings.isNullOrEmpty(oldName) || Strings.isNullOrEmpty(newName)) {
            throw new IllegalArgumentException("old or renamed columns are null or empty, should never happen");
        }
        if (OLD_TO_NEW_RENAMED_COLUMNS.get().containsKey(newName)) {
            String oldValue = OLD_TO_NEW_RENAMED_COLUMNS.get().get(newName);
            OLD_TO_NEW_RENAMED_COLUMNS.get().remove(newName);
            OLD_TO_NEW_RENAMED_COLUMNS.get().put(oldName, oldValue);
        }
        else {
            OLD_TO_NEW_RENAMED_COLUMNS.get().put(oldName, newName);
        }
    }

    @VisibleForTesting
    static List<String> getClusteredColumnList(String clusteredKey, String clusteredValue)
    {
        List<String> clusteredColumns;
        if (clusteredKey.equals(CLUSTERING_PARAMETER_KEY)) {
            try {
                clusteredColumns = ImmutableList.copyOf(OBJECT_MAPPER.readValue(clusteredValue, new TypeReference<List<String>>() {}));
            }
            catch (JsonProcessingException e) {
                LOG.error("Failed to extract clustering columns from commitInfoEntry: %s", e);
                return ImmutableList.of();
            }
        }
        else if (clusteredKey.equals(NEW_CLUSTERING_PARAMETER_KEY)) {
            clusteredColumns = Arrays.stream(clusteredValue.split(","))
                    .map(String::trim)
                    .collect(toImmutableList());
        }
        else {
            LOG.error("Unknown clustering key: %s", clusteredKey);
            return ImmutableList.of();
        }
        return clusteredColumns;
    }

    @VisibleForTesting
    static ThreadLocal<Map<String, String>> getOldToNewRenamedColumns()
    {
        return OLD_TO_NEW_RENAMED_COLUMNS;
    }
}
