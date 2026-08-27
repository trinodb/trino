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
package io.trino.plugin.paimon;

import org.apache.paimon.Snapshot.CommitKind;
import org.apache.paimon.crosspartition.KeyPartPartitionKeyExtractor;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonKeyDynamicBootstrapTest
{
    @Test
    void testFullRowAndKeyPartExtractorsProduceSameTrimmedPrimaryKey()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-key-layout");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(
                new DataField(0, "dt", new IntType()),
                new DataField(1, "id", new IntType()),
                new DataField(2, "value", new VarCharType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                List.of("dt"),
                List.of("id"),
                Map.of("bucket", "-1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        BinaryRow writerKey = new RowPartitionKeyExtractor(table.schema())
                .trimmedPrimaryKey(GenericRow.of(41, 2_000_000_001, fromString("value")));
        BinaryRow validatorKey = new KeyPartPartitionKeyExtractor(table.schema())
                .trimmedPrimaryKey(GenericRow.of(2_000_000_001, 41));

        assertThat(writerKey).isEqualTo(validatorKey);
    }

    @Test
    void testLocalFilesystemSupportsAtomicCommitValidation()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-capability");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(new DataField(0, "a", new IntType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Map.of("bucket", "1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);

        assertThatCode(() -> PaimonKeyDynamicBootstrap.validateAtomicCommitCapability(table))
                .doesNotThrowAnyException();
    }

    @Test
    void testKeyFingerprintSameKeyAndDisjointKey()
    {
        PaimonKeyDynamicBootstrap.KeyFingerprint fingerprint = PaimonKeyDynamicBootstrap.KeyFingerprint.empty();
        fingerprint.addHash(17);

        assertThat(fingerprint.mightContainHash(17)).isTrue();

        int disjointHash = 18;
        while (fingerprint.mightContainHash(disjointHash)) {
            disjointHash++;
        }
        assertThat(fingerprint.mightContainHash(disjointHash)).isFalse();

        PaimonKeyDynamicBootstrap.KeyFingerprint merged = PaimonKeyDynamicBootstrap.KeyFingerprint.empty();
        merged.addHash(disjointHash);
        merged.merge(fingerprint);
        assertThat(merged.mightContainHash(17)).isTrue();
        assertThat(merged.mightContainHash(disjointHash)).isTrue();
    }

    @Test
    void testKeyFingerprintSidecarRoundTripAndChecksum()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-fingerprint");
        FileIO fileIO = LocalFileIO.create();
        Path path = new Path(directory.toUri().toString(), "part-0");
        fileIO.mkdirs(new Path(directory.toUri().toString()));

        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                new PaimonKeyDynamicBootstrap.KeyFingerprintWriter(fileIO, path, "schema", 1, 0)) {
            writer.add(BinaryRow.singleColumn(17));
        }

        PaimonKeyDynamicBootstrap.KeyFingerprint roundTrip = PaimonKeyDynamicBootstrap.KeyFingerprint.read(
                fileIO, path, "schema", 1, 0);
        assertThat(roundTrip.mightContain(BinaryRow.singleColumn(17))).isTrue();
        assertThatCode(() -> PaimonKeyDynamicBootstrap.KeyFingerprint.read(fileIO, path, "other", 1, 0))
                .isInstanceOf(IOException.class);

        byte[] bytes;
        try (InputStream input = fileIO.newInputStream(path)) {
            bytes = input.readAllBytes();
        }
        bytes[bytes.length / 2] ^= 1;
        Files.write(directory.resolve("part-0"), bytes);
        assertThatThrownBy(() -> PaimonKeyDynamicBootstrap.KeyFingerprint.read(fileIO, path, "schema", 1, 0))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void testConcurrentDisjointAppendIsAllowedAndSameKeyIsRejected()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-validation");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(
                new DataField(0, "a", new IntType()),
                new DataField(1, "v", new VarCharType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Map.of("bucket", "1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        writeRow(table, 1);

        PaimonKeyDynamicBootstrap.OptionalSnapshot expected =
                PaimonKeyDynamicBootstrap.OptionalSnapshot.pinned(OptionalLong.of(1));
        String bootstrapQuery = "bootstrap-" + UUID.randomUUID();
        PaimonKeyDynamicBootstrap.prepare(table, bootstrapQuery, expected, 1);
        writeRow(table, 4);
        assertThatCode(() -> {
            PaimonKeyDynamicBootstrap.Artifact artifact =
                    PaimonKeyDynamicBootstrap.open(table, bootstrapQuery, expected, 1);
            try (PaimonKeyDynamicBootstrap.ShardReader reader = artifact.openShard(0)) {
                while (reader.next() != null) {
                    // Drain the pinned artifact to validate the worker read path.
                }
            }
        }).doesNotThrowAnyException();
        PaimonKeyDynamicBootstrap.cleanup(table, bootstrapQuery, expected, 1);

        String disjointQuery = "disjoint-" + UUID.randomUUID();
        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(table, disjointQuery, expected, 1, 0, 10)) {
            writer.add(BinaryRow.singleColumn(2));
        }
        writeRow(table, 3);
        assertThatCode(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, disjointQuery, expected, 1, false)).doesNotThrowAnyException();

        String conflictingQuery = "conflicting-" + UUID.randomUUID();
        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(table, conflictingQuery, expected, 1, 0, 11)) {
            writer.add(BinaryRow.singleColumn(3));
        }
        assertThatThrownBy(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, conflictingQuery, expected, 1, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary key");
    }

    @Test
    void testOverwriteStillRejectsConcurrentSnapshot()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-overwrite-validation");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(new DataField(0, "a", new IntType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Map.of("bucket", "1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        writeRow(table, 1);
        PaimonKeyDynamicBootstrap.OptionalSnapshot expected =
                PaimonKeyDynamicBootstrap.OptionalSnapshot.pinned(OptionalLong.of(1));
        String queryId = "overwrite-" + UUID.randomUUID();
        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(table, queryId, expected, 1, 0, 12)) {
            writer.add(BinaryRow.singleColumn(2));
        }
        writeRow(table, 3);

        assertThatThrownBy(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, queryId, expected, 1, true))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("snapshot changed");
    }

    @Test
    void testConcurrentCompactionAndAnalyzeCanBeRebased()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-metadata-snapshots");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(
                new DataField(0, "a", new IntType()),
                new DataField(1, "v", new VarCharType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.emptyList(),
                Collections.singletonList("a"),
                Map.of("bucket", "1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        writeRow(table, 1);
        writeRow(table, 4);

        PaimonKeyDynamicBootstrap.OptionalSnapshot expected =
                PaimonKeyDynamicBootstrap.OptionalSnapshot.pinned(OptionalLong.of(2));
        String compactQuery = "compact-" + UUID.randomUUID();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        }
        assertThat(table.store().snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(CommitKind.COMPACT);
        writeFingerprint(table, compactQuery, expected, 2);
        assertThatCode(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, compactQuery, expected, 1, false)).doesNotThrowAnyException();

        String analyzeQuery = "analyze-" + UUID.randomUUID();
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.updateStatistics(new Statistics(1L, 0L, 1L, 1L));
        }
        assertThat(table.store().snapshotManager().latestSnapshot().commitKind())
                .isEqualTo(CommitKind.ANALYZE);
        writeFingerprint(table, analyzeQuery, expected, 3);
        assertThatCode(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, analyzeQuery, expected, 1, false)).doesNotThrowAnyException();
    }

    @Test
    void testConcurrentSameKeyInDifferentPartitionIsRejected()
            throws Exception
    {
        java.nio.file.Path directory = Files.createTempDirectory("paimon-key-dynamic-partition-validation");
        Path tablePath = new Path(directory.toUri().toString());
        RowType rowType = new RowType(List.of(
                new DataField(0, "a", new IntType()),
                new DataField(1, "p", new VarCharType()),
                new DataField(2, "v", new VarCharType())));
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(new Schema(
                rowType.getFields(),
                Collections.singletonList("p"),
                Collections.singletonList("a"),
                Map.of("bucket", "1"),
                ""));
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath);
        writePartitionedRow(table, 1, "old");

        PaimonKeyDynamicBootstrap.OptionalSnapshot expected =
                PaimonKeyDynamicBootstrap.OptionalSnapshot.pinned(OptionalLong.of(1));
        String queryId = "partition-conflict-" + UUID.randomUUID();
        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(table, queryId, expected, 1, 0, 20)) {
            writer.add(BinaryRow.singleColumn(1));
        }
        writePartitionedRow(table, 1, "new");

        assertThatThrownBy(() -> PaimonKeyDynamicBootstrap.validateSnapshotForCommit(
                table, queryId, expected, 1, false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("primary key");
    }

    private static void writeRow(FileStoreTable table, int key)
            throws Exception
    {
        String commitUser = "test-" + UUID.randomUUID();
        InnerTableWrite writer = table.newWrite(commitUser);
        InnerTableCommit commit = table.newCommit(commitUser);
        try {
            writer.write(GenericRow.of(key, fromString("value-" + key)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }
        finally {
            writer.close();
            commit.close();
        }
    }

    private static void writeFingerprint(
            FileStoreTable table,
            String queryId,
            PaimonKeyDynamicBootstrap.OptionalSnapshot expected,
            int writerId)
            throws Exception
    {
        try (PaimonKeyDynamicBootstrap.KeyFingerprintWriter writer =
                PaimonKeyDynamicBootstrap.openKeyFingerprintWriter(table, queryId, expected, 1, 0, writerId)) {
            writer.add(BinaryRow.singleColumn(2));
        }
    }

    private static void writePartitionedRow(FileStoreTable table, int key, String partition)
            throws Exception
    {
        String commitUser = "partition-test-" + UUID.randomUUID();
        InnerTableWrite writer = table.newWrite(commitUser);
        InnerTableCommit commit = table.newCommit(commitUser);
        try {
            writer.write(GenericRow.of(
                    key,
                    fromString(partition),
                    fromString("value-" + partition)));
            commit.commit(0, writer.prepareCommit(true, 0));
        }
        finally {
            writer.close();
            commit.close();
        }
    }
}
