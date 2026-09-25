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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.connector.SchemaTableName;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.V2_CHECKPOINT_FEATURE_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public final class TransactionLogCleanup
{
    private static final Logger log = Logger.get(TransactionLogCleanup.class);

    private static final int DELETE_BATCH_SIZE = 1000;
    private static final Pattern JSON_FILE = Pattern.compile("^(\\d{20})\\.json$");
    private static final Pattern VERSION_CHECKSUM = Pattern.compile("^(\\d{20})\\.crc$");
    private static final Pattern SINGLE_CHECKPOINT = Pattern.compile("^(\\d{20})\\.checkpoint\\.parquet$");
    private static final Pattern MULTIPART_CHECKPOINT = Pattern.compile("^(\\d{20})\\.checkpoint\\.(\\d{10})\\.(\\d{10})\\.parquet$");
    private static final Pattern CHECKPOINT_FILE = Pattern.compile("^\\d{20}\\.checkpoint\\..*$");
    private static final Pattern CHECKPOINT_CRC = Pattern.compile("^\\.?\\d{20}\\.checkpoint\\..*\\.crc$");
    private static final Pattern V2_CHECKPOINT = Pattern.compile(
            "^\\d{20}\\.checkpoint\\.[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\.(json|parquet)$");
    private static final Pattern COMPACTED_LOG = Pattern.compile("^\\d{20}\\.\\d{20}\\.compacted\\.json$");

    private static final Set<String> UNSUPPORTED_FEATURES = Set.of(
            V2_CHECKPOINT_FEATURE_NAME,
            "checkpointProtection",
            "catalogManaged",
            "coordinatedCommits",
            "commitCoordinator-preview");

    private final Clock clock;

    @Inject
    public TransactionLogCleanup()
    {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    TransactionLogCleanup(Clock clock)
    {
        this.clock = requireNonNull(clock, "clock is null");
    }

    public void cleanup(
            SchemaTableName table,
            TrinoFileSystem fileSystem,
            Location transactionLogDirectory,
            long newCheckpointVersion,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry)
    {
        try {
            cleanupInternal(fileSystem, transactionLogDirectory, newCheckpointVersion, metadataEntry, protocolEntry);
        }
        catch (IOException | RuntimeException e) {
            log.warn(e, "Failed to clean transaction log for table %s", table);
        }
    }

    private void cleanupInternal(
            TrinoFileSystem fileSystem,
            Location transactionLogDirectory,
            long newCheckpointVersion,
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry)
            throws IOException
    {
        if (!metadataEntry.isExpiredLogCleanupEnabled() || hasUnsupportedProtocol(protocolEntry) || hasUnsupportedMetadata(metadataEntry)) {
            return;
        }

        List<FileEntry> jsonFiles = new ArrayList<>();
        List<FileEntry> versionChecksums = new ArrayList<>();
        ListMultimap<Long, CheckpointPart> checkpointParts = ArrayListMultimap.create();
        FileIterator iterator = fileSystem.listFiles(transactionLogDirectory);
        while (iterator.hasNext()) {
            FileEntry file = iterator.next();
            if (!file.location().parentDirectory().equals(transactionLogDirectory)) {
                if (isUnsupportedNestedLayout(transactionLogDirectory, file.location())) {
                    return;
                }
                continue;
            }

            String name = file.location().fileName();
            Matcher json = JSON_FILE.matcher(name);
            if (json.matches()) {
                jsonFiles.add(file);
                continue;
            }
            if (VERSION_CHECKSUM.matcher(name).matches()) {
                versionChecksums.add(file);
                continue;
            }
            Matcher singleCheckpoint = SINGLE_CHECKPOINT.matcher(name);
            if (singleCheckpoint.matches()) {
                checkpointParts.put(Long.parseLong(singleCheckpoint.group(1)), new CheckpointPart(file, 1, 1, true));
                continue;
            }
            Matcher multipartCheckpoint = MULTIPART_CHECKPOINT.matcher(name);
            if (multipartCheckpoint.matches()) {
                checkpointParts.put(
                        Long.parseLong(multipartCheckpoint.group(1)),
                        new CheckpointPart(file, Integer.parseInt(multipartCheckpoint.group(2)), Integer.parseInt(multipartCheckpoint.group(3)), false));
                continue;
            }
            if (V2_CHECKPOINT.matcher(name).matches() || COMPACTED_LOG.matcher(name).matches()) {
                return;
            }
            if (CHECKPOINT_FILE.matcher(name).matches() && !CHECKPOINT_CRC.matcher(name).matches()) {
                return;
            }
        }

        jsonFiles.sort(Comparator.comparingLong(TransactionLogCleanup::fileVersion));

        Map<Long, List<CheckpointPart>> checkpoints = checkpointParts.asMap().entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
        List<CheckpointPart> newCheckpoint = checkpoints.get(newCheckpointVersion);
        if (newCheckpoint == null || !isCompleteCheckpoint(newCheckpoint)) {
            return;
        }
        if (jsonFiles.stream().noneMatch(file -> fileVersion(file) == newCheckpointVersion)) {
            return;
        }

        Instant cutoff = clock.instant().minus(metadataEntry.getLogRetentionDuration()).truncatedTo(ChronoUnit.DAYS);
        long boundaryVersion = oldestNonexpiredVersion(jsonFiles, cutoff).orElse(newCheckpointVersion);
        for (Map.Entry<Long, List<CheckpointPart>> checkpoint : checkpoints.entrySet()) {
            boolean recentlyModified = false;
            for (CheckpointPart part : checkpoint.getValue()) {
                recentlyModified |= part.file().lastModified().isAfter(cutoff);
            }
            if (checkpoint.getKey() < boundaryVersion && recentlyModified) {
                boundaryVersion = checkpoint.getKey();
            }
        }
        for (FileEntry versionChecksum : versionChecksums) {
            long checksumVersion = fileVersion(versionChecksum);
            if (checksumVersion < boundaryVersion && versionChecksum.lastModified().isAfter(cutoff)) {
                boundaryVersion = checksumVersion;
            }
        }

        // Keep a replay anchor at or before the retained history, not just the newest checkpoint.
        long anchorVersion = -1;
        for (Map.Entry<Long, List<CheckpointPart>> checkpoint : checkpoints.entrySet()) {
            long checkpointVersion = checkpoint.getKey();
            if (checkpointVersion <= boundaryVersion && checkpointVersion <= newCheckpointVersion && isCompleteCheckpoint(checkpoint.getValue())) {
                anchorVersion = Math.max(anchorVersion, checkpointVersion);
            }
        }
        if (anchorVersion < 0 || !hasContiguousJson(jsonFiles, anchorVersion, newCheckpointVersion)) {
            return;
        }

        ImmutableList.Builder<Location> filesToDelete = ImmutableList.builder();
        for (FileEntry jsonFile : jsonFiles) {
            if (fileVersion(jsonFile) < anchorVersion && !jsonFile.lastModified().isAfter(cutoff)) {
                filesToDelete.add(jsonFile.location());
            }
        }
        for (Map.Entry<Long, List<CheckpointPart>> checkpoint : checkpoints.entrySet()) {
            if (checkpoint.getKey() < anchorVersion &&
                    isCompleteCheckpoint(checkpoint.getValue()) &&
                    checkpoint.getValue().stream().allMatch(part -> !part.file().lastModified().isAfter(cutoff))) {
                checkpoint.getValue().forEach(part -> filesToDelete.add(part.file().location()));
            }
        }
        for (FileEntry versionChecksum : versionChecksums) {
            if (fileVersion(versionChecksum) < anchorVersion && !versionChecksum.lastModified().isAfter(cutoff)) {
                filesToDelete.add(versionChecksum.location());
            }
        }

        List<Location> deletions = filesToDelete.build();
        for (int start = 0; start < deletions.size(); start += DELETE_BATCH_SIZE) {
            fileSystem.deleteFiles(deletions.subList(start, Math.min(start + DELETE_BATCH_SIZE, deletions.size())));
        }
    }

    private static boolean hasUnsupportedProtocol(ProtocolEntry protocolEntry)
    {
        return UNSUPPORTED_FEATURES.stream()
                .anyMatch(feature -> protocolEntry.readerFeaturesContains(feature) || protocolEntry.writerFeaturesContains(feature));
    }

    private static boolean hasUnsupportedMetadata(MetadataEntry metadataEntry)
    {
        if (metadataEntry.getConfiguration() == null) {
            return false;
        }
        String checkpointPolicy = metadataEntry.getConfiguration().get("delta.checkpointPolicy");
        if (checkpointPolicy != null && !checkpointPolicy.equalsIgnoreCase("classic")) {
            return true;
        }
        return metadataEntry.getConfiguration().keySet().stream()
                .anyMatch(key -> key.startsWith("delta.coordinatedCommits.") || key.startsWith("delta.commitCoordinator."));
    }

    private static boolean isUnsupportedNestedLayout(Location transactionLogDirectory, Location file)
    {
        String relativePath = file.toString().substring(transactionLogDirectory.toString().length());
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        return relativePath.startsWith("_sidecars/") ||
                relativePath.startsWith("_commits/") ||
                relativePath.startsWith("_staged_commits/");
    }

    private static long fileVersion(FileEntry file)
    {
        return Long.parseLong(file.location().fileName().substring(0, 20));
    }

    private static OptionalLong oldestNonexpiredVersion(List<FileEntry> jsonFiles, Instant cutoff)
    {
        Instant previousAdjusted = null;
        long adjustmentRunStart = -1;
        for (int index = 0; index < jsonFiles.size(); index++) {
            FileEntry file = jsonFiles.get(index);
            Instant adjusted = file.lastModified();
            if (previousAdjusted != null && !adjusted.isAfter(previousAdjusted)) {
                // Retain the start of an adjustment run so cleanup cannot change time-travel timestamps.
                if (adjustmentRunStart < 0) {
                    adjustmentRunStart = fileVersion(jsonFiles.get(index - 1));
                }
                adjusted = plusOneMillisecond(previousAdjusted);
            }
            else {
                adjustmentRunStart = -1;
            }
            if (adjusted.isAfter(cutoff)) {
                return OptionalLong.of(adjustmentRunStart >= 0 ? adjustmentRunStart : fileVersion(file));
            }
            previousAdjusted = adjusted;
        }
        return OptionalLong.empty();
    }

    private static Instant plusOneMillisecond(Instant instant)
    {
        if (instant.isAfter(Instant.MAX.minusMillis(1))) {
            return Instant.MAX;
        }
        return instant.plusMillis(1);
    }

    private static boolean isCompleteCheckpoint(List<CheckpointPart> parts)
    {
        if (parts.stream().anyMatch(part -> part.file().length() == 0)) {
            return false;
        }
        if (parts.size() == 1 && parts.getFirst().single()) {
            return true;
        }
        if (parts.isEmpty() || parts.stream().anyMatch(CheckpointPart::single)) {
            return false;
        }

        int partCount = parts.getFirst().partCount();
        if (partCount <= 0 || parts.size() != partCount || parts.stream().anyMatch(part -> part.partCount() != partCount)) {
            return false;
        }
        Set<Integer> ordinals = new HashSet<>();
        for (CheckpointPart part : parts) {
            if (part.partNumber() <= 0 || part.partNumber() > partCount || !ordinals.add(part.partNumber())) {
                return false;
            }
        }
        return true;
    }

    private static boolean hasContiguousJson(List<FileEntry> jsonFiles, long startVersion, long endVersion)
    {
        long expectedVersion = startVersion;
        for (FileEntry file : jsonFiles) {
            long version = fileVersion(file);
            if (version < startVersion) {
                continue;
            }
            if (version > endVersion) {
                break;
            }
            if (version != expectedVersion) {
                return false;
            }
            expectedVersion++;
        }
        return expectedVersion == endVersion + 1;
    }

    private record CheckpointPart(FileEntry file, int partNumber, int partCount, boolean single) {}
}
