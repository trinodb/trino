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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

final class TestTransactionLogCleanup
{
    private static final Instant NOW = Instant.parse("2025-02-15T12:00:00Z");
    private static final Instant EXPIRED = Instant.parse("2025-01-15T00:00:00Z");
    private static final Instant CUTOFF = Instant.parse("2025-01-16T00:00:00Z");
    private static final Instant RECENT = Instant.parse("2025-02-01T00:00:00Z");
    private static final Location LOG_DIRECTORY = Location.of("memory:///table/_delta_log");
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final ProtocolEntry CLASSIC_PROTOCOL = new ProtocolEntry(1, 2, Optional.empty(), Optional.empty());

    private final TransactionLogCleanup cleanup = new TransactionLogCleanup(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void testDefaultRetentionDeletesExpiredPrefixAndPreservesReplayChain()
    {
        TestingFileSystem fileSystem = new TestingFileSystem();
        addJsonRange(fileSystem, 0, 5, EXPIRED);
        fileSystem.setLastModified(jsonName(3), RECENT);
        fileSystem.setLastModified(jsonName(4), RECENT.plusSeconds(1));
        fileSystem.setLastModified(jsonName(5), RECENT.plusSeconds(2));
        fileSystem.add(checkpointName(0), EXPIRED, 1);
        fileSystem.add(checkpointName(2), EXPIRED, 1);
        fileSystem.add(checkpointName(5), RECENT, 1);
        fileSystem.add("_last_checkpoint", RECENT, 1);
        fileSystem.add("notes.txt", EXPIRED, 1);
        fileSystem.add("_trino_meta/statistics", EXPIRED, 1);
        fileSystem.reverseListing = true;

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(
                jsonName(2),
                jsonName(3),
                jsonName(4),
                jsonName(5),
                checkpointName(2),
                checkpointName(5),
                "_last_checkpoint",
                "notes.txt",
                "_trino_meta/statistics");
    }

    @Test
    void testCustomRetentionAndCutoffEquality()
    {
        TestingFileSystem fileSystem = new TestingFileSystem();
        fileSystem.add(jsonName(0), CUTOFF, 1);
        fileSystem.add(jsonName(1), CUTOFF.plusSeconds(1), 1);
        fileSystem.add(jsonName(2), CUTOFF.plusSeconds(2), 1);
        fileSystem.add(checkpointName(0), CUTOFF, 1);
        fileSystem.add(checkpointName(1), CUTOFF.plusSeconds(1), 1);
        fileSystem.add(checkpointName(2), CUTOFF.plusSeconds(2), 1);

        cleanup(fileSystem, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(1), jsonName(2), checkpointName(1), checkpointName(2));

        TestingFileSystem customRetention = new TestingFileSystem();
        addJsonRange(customRetention, 0, 2, NOW.minus(Duration.ofDays(3)));
        customRetention.add(checkpointName(2), NOW.minus(Duration.ofDays(3)), 1);
        cleanup(customRetention, 2, metadata(true, Duration.ofDays(2)), CLASSIC_PROTOCOL);
        assertThat(customRetention.names()).containsExactlyInAnyOrder(jsonName(2), checkpointName(2));
    }

    @Test
    void testDisabledCleanupDoesNothing()
    {
        TestingFileSystem fileSystem = completeExpiredLog(2);

        cleanup(fileSystem, 2, metadata(false, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).isEmpty();
    }

    @Test
    void testCompleteMultipartCheckpointIsDeletedAsFamily()
    {
        TestingFileSystem fileSystem = completeExpiredLog(4);
        fileSystem.add(multipartCheckpointName(1, 2, 2), EXPIRED, 1);
        fileSystem.add(multipartCheckpointName(1, 1, 2), EXPIRED, 1);
        fileSystem.add(checkpointName(3), EXPIRED, 1);

        cleanup(fileSystem, 4, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(4), checkpointName(4));
    }

    @Test
    void testIncompleteCheckpointFamiliesAreRetainedWithoutBlockingCleanup()
    {
        for (String checkpoint : List.of(
                multipartCheckpointName(1, 1, 2),
                format("%020d.checkpoint.0000000000.0000000002.parquet", 1))) {
            TestingFileSystem fileSystem = completeExpiredLog(3);
            fileSystem.add(checkpoint, EXPIRED, 1);

            cleanup(fileSystem, 3, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

            assertThat(fileSystem.names()).as(checkpoint).containsExactlyInAnyOrder(jsonName(3), checkpointName(3), checkpoint);
        }
    }

    @Test
    void testRecentIncompleteCheckpointPinsRetainedBoundary()
    {
        TestingFileSystem fileSystem = completeExpiredLog(5);
        fileSystem.add(checkpointName(0), EXPIRED, 1);
        fileSystem.add(multipartCheckpointName(2, 1, 2), RECENT, 1);

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).isEmpty();
    }

    @Test
    void testMalformedCheckpointNameFailsClosed()
    {
        TestingFileSystem fileSystem = completeExpiredLog(3);
        fileSystem.add(format("%020d.checkpoint.bad.parquet", 1), EXPIRED, 1);

        cleanup(fileSystem, 3, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).isEmpty();
    }

    @Test
    void testMissingJsonGapFailsClosed()
    {
        TestingFileSystem fileSystem = completeExpiredLog(4);
        fileSystem.remove(jsonName(3));
        fileSystem.add(checkpointName(2), EXPIRED, 1);
        fileSystem.setLastModified(jsonName(2), RECENT);
        fileSystem.setLastModified(jsonName(4), RECENT.plusSeconds(2));

        cleanup(fileSystem, 4, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).isEmpty();
    }

    @Test
    void testRequiresNonemptyNewCheckpointAndItsJson()
    {
        TestingFileSystem emptyCheckpoint = completeExpiredLog(2);
        emptyCheckpoint.add(checkpointName(2), EXPIRED, 0);
        cleanup(emptyCheckpoint, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);
        assertThat(emptyCheckpoint.deletedBatches).isEmpty();

        TestingFileSystem incompleteCheckpoint = completeExpiredLog(2);
        incompleteCheckpoint.remove(checkpointName(2));
        incompleteCheckpoint.add(multipartCheckpointName(2, 1, 2), EXPIRED, 1);
        cleanup(incompleteCheckpoint, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);
        assertThat(incompleteCheckpoint.deletedBatches).isEmpty();

        TestingFileSystem missingJson = completeExpiredLog(2);
        missingJson.remove(jsonName(2));
        cleanup(missingJson, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);
        assertThat(missingJson.deletedBatches).isEmpty();
    }

    @Test
    void testDeletesExpiredVersionChecksumsBelowAnchor()
    {
        TestingFileSystem fileSystem = completeExpiredLog(2);
        fileSystem.add(versionChecksumName(0), EXPIRED, 1);
        fileSystem.add(versionChecksumName(2), EXPIRED, 1);
        fileSystem.add(versionChecksumName(3), EXPIRED, 1);

        cleanup(fileSystem, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(
                jsonName(2), checkpointName(2), versionChecksumName(2), versionChecksumName(3));
    }

    @Test
    void testRecentVersionChecksumPinsRetainedAnchor()
    {
        TestingFileSystem fileSystem = completeExpiredLog(5);
        fileSystem.add(checkpointName(2), EXPIRED, 1);
        fileSystem.add(versionChecksumName(2), RECENT, 1);

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(
                jsonName(2),
                jsonName(3),
                jsonName(4),
                jsonName(5),
                checkpointName(2),
                checkpointName(5),
                versionChecksumName(2));
    }

    @Test
    void testPreservesUnknownFilesCrcAndNestedTrinoMetadata()
    {
        TestingFileSystem fileSystem = completeExpiredLog(2);
        fileSystem.add(".00000000000000000000.crc", EXPIRED, 1);
        fileSystem.add(".00000000000000000000.checkpoint.parquet.crc", EXPIRED, 1);
        fileSystem.add("unrecognized", EXPIRED, 1);
        fileSystem.add("_trino_meta/data", EXPIRED, 1);

        cleanup(fileSystem, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(
                jsonName(2),
                checkpointName(2),
                ".00000000000000000000.crc",
                ".00000000000000000000.checkpoint.parquet.crc",
                "unrecognized",
                "_trino_meta/data");
    }

    @Test
    void testUnsupportedLayoutsAndFeaturesFailClosed()
    {
        for (String unsupportedFile : List.of(
                "00000000000000000001.00000000000000000002.compacted.json",
                "00000000000000000002.checkpoint.123e4567-e89b-12d3-a456-426614174000.parquet",
                "_sidecars/part.parquet",
                "_commits/00000000000000000002.json",
                "_staged_commits/00000000000000000002.123e4567-e89b-12d3-a456-426614174000.json")) {
            TestingFileSystem fileSystem = completeExpiredLog(2);
            fileSystem.add(unsupportedFile, EXPIRED, 1);
            cleanup(fileSystem, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);
            assertThat(fileSystem.deletedBatches).as(unsupportedFile).isEmpty();
        }

        for (String feature : List.of("v2Checkpoint", "checkpointProtection", "catalogManaged", "coordinatedCommits", "commitCoordinator-preview")) {
            TestingFileSystem fileSystem = completeExpiredLog(2);
            ProtocolEntry protocol = new ProtocolEntry(3, 7, Optional.of(ImmutableSet.of(feature)), Optional.of(ImmutableSet.of(feature)));
            cleanup(fileSystem, 2, metadata(true, Duration.ofDays(30)), protocol);
            assertThat(fileSystem.deletedBatches).as(feature).isEmpty();
        }

        TestingFileSystem fileSystem = completeExpiredLog(2);
        cleanup(fileSystem,
                2,
                metadata(true, Duration.ofDays(30), ImmutableMap.of("delta.checkpointPolicy", "v2")),
                CLASSIC_PROTOCOL);
        assertThat(fileSystem.deletedBatches).isEmpty();

        TestingFileSystem coordinatedCommits = completeExpiredLog(2);
        cleanup(coordinatedCommits,
                2,
                metadata(true, Duration.ofDays(30), ImmutableMap.of("delta.coordinatedCommits.commitCoordinator-preview", "{}")),
                CLASSIC_PROTOCOL);
        assertThat(coordinatedCommits.deletedBatches).isEmpty();
    }

    @Test
    void testConcurrentVersionsAboveSuppliedCheckpointArePreserved()
    {
        TestingFileSystem fileSystem = completeExpiredLog(3);
        fileSystem.add(jsonName(4), EXPIRED.plusSeconds(4), 1);
        fileSystem.add(jsonName(5), EXPIRED.plusSeconds(5), 1);

        cleanup(fileSystem, 3, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(3), jsonName(4), jsonName(5), checkpointName(3));
    }

    @Test
    void testEqualExpiredCommitTimestampsAllowCleanup()
    {
        TestingFileSystem fileSystem = completeExpiredLog(3);
        fileSystem.setLastModified(jsonName(1), EXPIRED);
        fileSystem.setLastModified(jsonName(2), EXPIRED);
        fileSystem.setLastModified(jsonName(3), EXPIRED);

        cleanup(fileSystem, 3, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(3), checkpointName(3));
    }

    @Test
    void testTimestampAdjustmentRunCrossingCutoffIsRetained()
    {
        TestingFileSystem fileSystem = new TestingFileSystem();
        Instant justBeforeCutoff = CUTOFF.minusMillis(1);
        for (int version = 0; version <= 3; version++) {
            fileSystem.add(jsonName(version), justBeforeCutoff, 1);
        }
        fileSystem.add(checkpointName(0), justBeforeCutoff, 1);
        fileSystem.add(checkpointName(3), justBeforeCutoff, 1);

        cleanup(fileSystem, 3, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).isEmpty();
    }

    @Test
    void testRecentOldCheckpointPinsRetainedAnchor()
    {
        TestingFileSystem fileSystem = completeExpiredLog(5);
        fileSystem.add(checkpointName(0), EXPIRED, 1);
        fileSystem.add(checkpointName(2), RECENT, 1);

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(
                jsonName(2), jsonName(3), jsonName(4), jsonName(5), checkpointName(2), checkpointName(5));
    }

    @Test
    void testListAndDeleteFailuresNeverEscapeCommittedWrite()
    {
        TestingFileSystem listFailure = completeExpiredLog(2);
        listFailure.failList = true;
        assertThatCode(() -> cleanup(listFailure, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL))
                .doesNotThrowAnyException();

        TestingFileSystem deleteFailure = completeExpiredLog(2);
        deleteFailure.failDelete = true;
        assertThatCode(() -> cleanup(deleteFailure, 2, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL))
                .doesNotThrowAnyException();
        assertThat(deleteFailure.names()).contains(jsonName(2), checkpointName(2));
    }

    @Test
    void testPartialMultipartCheckpointDeletionDoesNotBlockLaterCleanup()
    {
        TestingFileSystem fileSystem = completeExpiredLog(5);
        String firstPart = multipartCheckpointName(1, 1, 2);
        String remainingPart = multipartCheckpointName(1, 2, 2);
        fileSystem.add(firstPart, EXPIRED, 1);
        fileSystem.add(remainingPart, EXPIRED, 1);
        fileSystem.add(checkpointName(3), EXPIRED, 1);
        fileSystem.failDeleteAfterRemoving(firstPart);

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);
        assertThat(fileSystem.names()).doesNotContain(firstPart);

        cleanup(fileSystem, 5, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(5), checkpointName(5), remainingPart);
    }

    @Test
    void testDeletesInBatchesOfAtMostOneThousand()
    {
        TestingFileSystem fileSystem = completeExpiredLog(1005);

        cleanup(fileSystem, 1005, metadata(true, Duration.ofDays(30)), CLASSIC_PROTOCOL);

        assertThat(fileSystem.deletedBatches).hasSize(2);
        assertThat(fileSystem.deletedBatches.get(0)).hasSize(1000);
        assertThat(fileSystem.deletedBatches.get(1)).hasSize(5);
        assertThat(fileSystem.names()).containsExactlyInAnyOrder(jsonName(1005), checkpointName(1005));
    }

    private void cleanup(TestingFileSystem fileSystem, long checkpointVersion, MetadataEntry metadata, ProtocolEntry protocol)
    {
        cleanup.cleanup(TABLE, fileSystem, LOG_DIRECTORY, checkpointVersion, metadata, protocol);
    }

    private static TestingFileSystem completeExpiredLog(long checkpointVersion)
    {
        TestingFileSystem fileSystem = new TestingFileSystem();
        addJsonRange(fileSystem, 0, checkpointVersion, EXPIRED);
        fileSystem.add(checkpointName(checkpointVersion), EXPIRED, 1);
        return fileSystem;
    }

    private static void addJsonRange(TestingFileSystem fileSystem, long start, long end, Instant firstModified)
    {
        for (long version = start; version <= end; version++) {
            fileSystem.add(jsonName(version), firstModified.plusSeconds(version - start), 1);
        }
    }

    private static String jsonName(long version)
    {
        return format("%020d.json", version);
    }

    private static String checkpointName(long version)
    {
        return format("%020d.checkpoint.parquet", version);
    }

    private static String versionChecksumName(long version)
    {
        return format("%020d.crc", version);
    }

    private static String multipartCheckpointName(long version, int part, int parts)
    {
        return format("%020d.checkpoint.%010d.%010d.parquet", version, part, parts);
    }

    private static MetadataEntry metadata(boolean cleanupEnabled, Duration retention)
    {
        return metadata(cleanupEnabled, retention, ImmutableMap.of());
    }

    private static MetadataEntry metadata(boolean cleanupEnabled, Duration retention, Map<String, String> configuration)
    {
        return MetadataEntry.builder()
                .setConfiguration(ImmutableMap.<String, String>builder()
                        .putAll(configuration)
                        .put("delta.enableExpiredLogCleanup", Boolean.toString(cleanupEnabled))
                        .put("delta.logRetentionDuration", "interval %s seconds".formatted(retention.toSeconds()))
                        .buildOrThrow())
                .build();
    }

    private static class TestingFileSystem
            implements TrinoFileSystem
    {
        private final Map<Location, FileEntry> files = new LinkedHashMap<>();
        private final List<List<Location>> deletedBatches = new ArrayList<>();
        private boolean reverseListing;
        private boolean failList;
        private boolean failDelete;
        private Location deleteBeforeFailure;

        void add(String relativePath, Instant lastModified, long length)
        {
            Location location = LOG_DIRECTORY.appendPath(relativePath);
            files.put(location, new FileEntry(location, length, lastModified, Optional.empty()));
        }

        void remove(String relativePath)
        {
            files.remove(LOG_DIRECTORY.appendPath(relativePath));
        }

        void setLastModified(String relativePath, Instant lastModified)
        {
            Location location = LOG_DIRECTORY.appendPath(relativePath);
            FileEntry entry = files.get(location);
            files.put(location, new FileEntry(location, entry.length(), lastModified, Optional.empty()));
        }

        Set<String> names()
        {
            return files.keySet().stream()
                    .map(location -> location.toString().substring(LOG_DIRECTORY.toString().length() + 1))
                    .collect(toSet());
        }

        void failDeleteAfterRemoving(String relativePath)
        {
            deleteBeforeFailure = LOG_DIRECTORY.appendPath(relativePath);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            if (failList) {
                throw new IOException("list failed");
            }
            List<FileEntry> listing = new ArrayList<>(files.values());
            if (reverseListing) {
                Collections.reverse(listing);
            }
            return new FileIterator()
            {
                private int index;

                @Override
                public boolean hasNext()
                {
                    return index < listing.size();
                }

                @Override
                public FileEntry next()
                {
                    return listing.get(index++);
                }
            };
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            deletedBatches.add(ImmutableList.copyOf(locations));
            if (deleteBeforeFailure != null) {
                assertThat(locations).contains(deleteBeforeFailure);
                files.remove(deleteBeforeFailure);
                deleteBeforeFailure = null;
                throw new IOException("delete failed");
            }
            if (failDelete) {
                locations.stream().findFirst().ifPresent(files::remove);
                throw new IOException("delete failed");
            }
            locations.forEach(files::remove);
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Location> listDirectories(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException();
        }
    }
}
