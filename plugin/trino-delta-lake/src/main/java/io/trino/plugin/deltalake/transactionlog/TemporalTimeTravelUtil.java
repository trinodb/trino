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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readLastCheckpoint;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogDir;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogUtil.getTransactionLogJsonEntryPath;
import static io.trino.plugin.deltalake.transactionlog.checkpoint.TransactionLogTail.getEntriesFromJson;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static java.lang.String.format;

public final class TemporalTimeTravelUtil
{
    private static final Pattern TRANSACTION_LOG_PATTERN = Pattern.compile("^(\\d{20})\\.json$");
    private static final int VERSION_NOT_FOUND = -1;

    private TemporalTimeTravelUtil() {}

    /**
     * Finds the latest version where the commit timestamp is less than or equal to the given `epochMillis`:
     * <ul>
     *     <li>If `_last_checkpoint` exists in `_delta_log` and provides a valid `commitTime`:
     *         <ul>
     *             <li>If `commitTime` equals `epochMillis`, the target version is found.</li>
     *             <li>If `commitTime` is less than `epochMillis`, the target version is beyond the checkpoint; proceed with `searchTowardsTailLinear`.</li>
     *             <li>If `commitTime` is greater than `epochMillis`, the target version is before the checkpoint; proceed with `searchTowardsHead`.</li>
     *         </ul>
     *     </li>
     *     <li>If the earliest (version 0) transaction log exists, start `searchTowardsTailLinear` from version 0.</li>
     *     <li>If neither the latest nor earliest commit exists, determine the version by listing all metadata files under `_delta_log`.</li>
     * </ul>
     * <p>`searchTowardsHead` run in parallel if the total number of commits exceeds `maxLinearSearchSize`.</p>
     *
     * <p>If no version is found with a commit timestamp less than or equal to `epochMillis`,
     * a {@link TrinoException} with error code {@code INVALID_ARGUMENTS} is thrown,
     * indicating that the table did not exist yet at the specified time or the version at that time is expired.</p>
     */
    public static long findLatestVersionUsingTemporal(TrinoFileSystem fileSystem, String tableLocation, long epochMillis, Executor executor, int maxLinearSearchSize)
            throws IOException
    {
        checkArgument(maxLinearSearchSize > 0, "maxLinearSearchSize must be greater than 0");
        long version = findLatestVersionUsingTemporalInternal(fileSystem, tableLocation, epochMillis, executor, maxLinearSearchSize);
        if (version >= 0) {
            return version;
        }

        throw new TrinoException(INVALID_ARGUMENTS, format("No temporal version history at or before %s", Instant.ofEpochMilli(epochMillis)));
    }

    private static long findLatestVersionUsingTemporalInternal(TrinoFileSystem fileSystem, String tableLocation, long epochMillis, Executor executor, int maxLinearSearchSize)
            throws IOException
    {
        String transactionLogDir = getTransactionLogDir(tableLocation);

        Optional<LastCheckpoint> lastCheckpoint = readLastCheckpoint(fileSystem, tableLocation);
        if (lastCheckpoint.isPresent()) {
            long entryNumber = lastCheckpoint.map(LastCheckpoint::version).orElseThrow();
            Optional<CommitInfoEntry> commitInfo = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0))
                    .orElseThrow(() -> new MissingTransactionLogException(getTransactionLogJsonEntryPath(transactionLogDir, entryNumber).toString()))
                    .getEntries(fileSystem)
                    .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                    .filter(Objects::nonNull)
                    .collect(toOptional());
            if (commitInfo.isPresent()) {
                return searchBasedOnLatestCheckPoint(fileSystem, transactionLogDir, epochMillis, commitInfo.orElseThrow(), executor, maxLinearSearchSize);
            }
        }

        if (fileSystem.newInputFile(getTransactionLogJsonEntryPath(transactionLogDir, 0)).exists()) {
            return searchTowardsTailLinear(fileSystem, 0, transactionLogDir, epochMillis);
        }

        return findLatestVersionFromWholeTransactions(fileSystem, transactionLogDir, epochMillis);
    }

    private static long searchBasedOnLatestCheckPoint(TrinoFileSystem fileSystem, String transactionLogDir, long epochMillis, CommitInfoEntry commitInfo, Executor executor, int maxLinearSearchSize)
            throws IOException
    {
        long commitTime = getCommitInfoTimestamp(commitInfo);
        if (commitTime == epochMillis) {
            return commitInfo.version();
        }

        if (commitTime < epochMillis) {
            long tail = searchTowardsTailLinear(fileSystem, commitInfo.version() + 1, transactionLogDir, epochMillis);
            if (tail >= 0) {
                return tail;
            }
            return commitInfo.version();
        }

        return searchTowardsHead(fileSystem, commitInfo.version() - 1, transactionLogDir, epochMillis, executor, maxLinearSearchSize);
    }

    private static long searchTowardsHead(TrinoFileSystem fileSystem, long entryNumber, String transactionLogDir, long epochMillis, Executor executor, int maxLinearSearchSize)
            throws IOException
    {
        if (entryNumber >= maxLinearSearchSize) {
            return searchTowardsHeadParallel(fileSystem, entryNumber, transactionLogDir, epochMillis, executor, maxLinearSearchSize);
        }

        return searchTowardsHeadLinear(fileSystem, 0, entryNumber, transactionLogDir, epochMillis);
    }

    private static long searchTowardsHeadLinear(TrinoFileSystem fileSystem, long start, long end, String transactionLogDir, long epochMillis)
            throws IOException
    {
        long entryNumber = end;
        Optional<TransactionLogEntries> entries = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0));
        while (start <= entryNumber && entries.isPresent()) {
            try (Stream<DeltaLakeTransactionLogEntry> logEntryStream = entries.get().getEntries(fileSystem)) {
                Optional<CommitInfoEntry> commitInfo = logEntryStream
                        .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                        .filter(Objects::nonNull)
                        .findFirst();
                if (commitInfo.isPresent() && commitInfo.get().timestamp() <= epochMillis) {
                    return commitInfo.get().version();
                }

                entryNumber--;
                if (entryNumber < 0) {
                    break;
                }
                entries = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0));
            }
        }

        return VERSION_NOT_FOUND;
    }

    private static long searchTowardsHeadParallel(TrinoFileSystem fileSystem, long end, String transactionLogDir, long epochMillis, Executor executor, int maxLinearSearchSize)
    {
        ImmutableList.Builder<Callable<Long>> versionSearchTasks = ImmutableList.builder();
        for (long start = 0; start <= end; start += maxLinearSearchSize) {
            long head = start;
            long tail = start + maxLinearSearchSize - 1;
            versionSearchTasks.add(() -> searchTowardsHeadLinear(fileSystem, head, tail, transactionLogDir, epochMillis));
        }
        try {
            return processWithAdditionalThreads(versionSearchTasks.build(), executor).stream()
                    .max(Long::compare)
                    .orElse(-1L);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private static long searchTowardsTailLinear(TrinoFileSystem fileSystem, long start, String transactionLogDir, long epochMillis)
            throws IOException
    {
        long entryNumber = start;
        long version = VERSION_NOT_FOUND;
        Optional<TransactionLogEntries> entries = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0));
        while (entries.isPresent()) {
            try (Stream<DeltaLakeTransactionLogEntry> logEntries = entries.get().getEntries(fileSystem)) {
                Optional<CommitInfoEntry> commitInfo = logEntries
                        .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                        .filter(Objects::nonNull)
                        .findFirst();

                if (commitInfo.isEmpty()) {
                    entryNumber++;
                    entries = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0));
                    continue;
                }

                if (commitInfo.map(TemporalTimeTravelUtil::getCommitInfoTimestamp).orElseThrow() > epochMillis) {
                    break;
                }

                version = entryNumber;

                entryNumber++;
                entries = getEntriesFromJson(entryNumber, transactionLogDir, fileSystem, DataSize.ofBytes(0));
            }
        }

        return version;
    }

    // In case neither last check point and earliest commit exists, we need to check the file directly
    private static long findLatestVersionFromWholeTransactions(TrinoFileSystem fileSystem, String transactionLogDir, long epochMillis)
            throws IOException
    {
        FileIterator fileIterator = fileSystem.listFiles(Location.of(transactionLogDir));
        if (!fileIterator.hasNext()) {
            return VERSION_NOT_FOUND;
        }

        long version = VERSION_NOT_FOUND;
        while (fileIterator.hasNext()) {
            Location location = fileIterator.next().location();
            Matcher matcher = TRANSACTION_LOG_PATTERN.matcher(location.fileName());
            if (!matcher.matches()) {
                continue;
            }
            long entryNumber = Long.parseLong(matcher.group(1));
            Optional<CommitInfoEntry> commitInfo = getEntriesFromJson(entryNumber, fileSystem.newInputFile(location), DataSize.ofBytes(0))
                    .map(entry -> entry.getEntries(fileSystem))
                    .orElseThrow()
                    .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                    .filter(Objects::nonNull)
                    .filter(commitInfoEntry -> getCommitInfoTimestamp(commitInfoEntry) <= epochMillis)
                    .findFirst();
            if (commitInfo.isEmpty()) {
                continue;
            }
            version = Math.max(version, commitInfo.get().version());
        }

        return version;
    }

    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#in-commit-timestamps
    // Use the inCommitTimestamps if exists, otherwise use the timestamp in commitInfo
    private static long getCommitInfoTimestamp(CommitInfoEntry commitInfoEntry)
    {
        return commitInfoEntry.inCommitTimestamp().orElse(commitInfoEntry.timestamp());
    }
}
