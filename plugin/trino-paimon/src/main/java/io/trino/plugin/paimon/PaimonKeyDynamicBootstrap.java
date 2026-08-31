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

import jakarta.annotation.Nullable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.crosspartition.KeyPartPartitionKeyExtractor;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.TypeUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE;
import static org.apache.paimon.CoreOptions.IncrementalBetweenScanMode.DELTA;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.StartupMode.FROM_SNAPSHOT;
import static org.apache.paimon.CoreOptions.StartupMode.LATEST;
import static org.apache.paimon.io.SplitsParallelReadUtil.parallelExecute;

/**
 * Shared, snapshot-pinned bootstrap files for Paimon KEY_DYNAMIC writers.
 */
final class PaimonKeyDynamicBootstrap
{
    private static final int MANIFEST_MAGIC = 0x504B4442;
    private static final int MANIFEST_VERSION = 1;
    private static final String ROOT_DIRECTORY = ".trino-key-dynamic-bootstrap";
    private static final String MANIFEST_FILE = "manifest";
    private static final String KEY_FINGERPRINT_DIRECTORY = "key-fingerprints";
    private static final int KEY_FINGERPRINT_MAGIC = 0x504B4446;
    private static final int KEY_FINGERPRINT_VERSION = 1;
    private static final int KEY_FINGERPRINT_BITS = 1 << 20;
    private static final int KEY_FINGERPRINT_HASHES = 4;
    private static final int KEY_FINGERPRINT_WORDS = KEY_FINGERPRINT_BITS / Long.SIZE;
    private static final int KEY_FINGERPRINT_DIGEST_BYTES = 32;
    private static final int MAX_SNAPSHOT_VALIDATION_RETRIES = 3;
    private static final AtomicLong VALIDATION_SCAN_BYTES = new AtomicLong();
    private static final AtomicLong VALIDATION_SCAN_FILES = new AtomicLong();
    private static final AtomicLong VALIDATION_SCAN_NANOS = new AtomicLong();

    private PaimonKeyDynamicBootstrap() {}

    /**
     * Check the actual Paimon snapshot commit implementation before generating worker bootstrap
     * files. The commit path repeats this check through the validator API because planning and
     * commit happen at different lifecycle points.
     */
    static void validateAtomicCommitCapability(FileStoreTable table)
            throws Exception
    {
        requireNonNull(table, "table is null");
        SnapshotCommit snapshotCommit = table.catalogEnvironment().snapshotCommit(table.snapshotManager());
        if (snapshotCommit == null) {
            throw unsupportedAtomicCommit(table);
        }
        try (snapshotCommit) {
            // Paimon 2.0 removed supportsAtomicCommitValidation(); pre-commit validation
            // is used instead of under-lock validation.
        }
    }

    private static UnsupportedOperationException unsupportedAtomicCommit(FileStoreTable table)
    {
        return new UnsupportedOperationException(
                "Paimon KEY_DYNAMIC writes require an atomic snapshot validator boundary for table "
                        + table.name()
                        + "; configure a Paimon catalog lock for object-store tables and use a validator-capable commit path");
    }

    static Artifact open(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        Long snapshot = expectedSnapshot.pinned()
                ? optionalSnapshotValue(expectedSnapshot.snapshotId())
                : latestSnapshotId(table);
        FileIO fileIO = table.fileIO();
        Path root = artifactRoot(table, queryId, expectedSnapshot.pinned() ? snapshot : null, assignerParallelism);
        Path manifest = new Path(root, MANIFEST_FILE);
        if (!fileIO.exists(manifest)) {
            throw new IOException("Paimon KEY_DYNAMIC bootstrap artifact was not prepared by the coordinator: " + root);
        }
        return readManifest(fileIO, manifest, root, table, snapshot, assignerParallelism);
    }

    static void prepare(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        Long snapshot = snapshotForBootstrap(table, expectedSnapshot);
        FileIO fileIO = table.fileIO();
        Path root = artifactRoot(table, queryId, expectedSnapshot.pinned() ? snapshot : null, assignerParallelism);
        Path manifest = new Path(root, MANIFEST_FILE);
        fileIO.mkdirs(root);
        if (!fileIO.exists(manifest)) {
            generate(fileIO, root, manifest, table, snapshot, assignerParallelism);
        }
        readManifest(fileIO, manifest, root, table, snapshot, assignerParallelism);
    }

    static void cleanup(FileStoreTable table, String queryId, OptionalSnapshot expectedSnapshot, int assignerParallelism)
    {
        try {
            Long snapshot = optionalSnapshotValue(expectedSnapshot.snapshotId());
            table.fileIO().delete(artifactRoot(table, queryId, snapshot, assignerParallelism), true);
        }
        catch (Exception ignored) {
            // Bootstrap artifacts are temporary. A later table cleanup can remove an artifact left by a failed query.
        }
    }

    static OptionalLong latestSnapshot(FileStoreTable table)
    {
        Long snapshot = latestSnapshotId(requireNonNull(table, "table is null"));
        return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot);
    }

    static void validateSnapshotForCommit(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism,
            boolean rejectConcurrentSnapshot)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        if (!expectedSnapshot.pinned()) {
            return;
        }

        Long expected = optionalSnapshotValue(expectedSnapshot.snapshotId());
        Long end = latestSnapshotId(table);
        if (equalsNullable(end, expected)) {
            return;
        }
        if (rejectConcurrentSnapshot) {
            throw snapshotChanged(expected, end, "before commit");
        }
        if (expected != null && (end == null || end < expected)) {
            throw snapshotChanged(expected, end, "before commit");
        }

        KeyFingerprint inputKeys = readKeyFingerprints(table, queryId, expectedSnapshot, assignerParallelism);
        for (int attempt = 0; attempt < MAX_SNAPSHOT_VALIDATION_RETRIES; attempt++) {
            checkSnapshotRangeCanRebase(table, expected, end);
            if (incrementalKeysIntersect(table, expected, end, inputKeys)) {
                throw new IllegalStateException(
                        "Paimon KEY_DYNAMIC concurrent snapshot contains a primary key written by this query: "
                                + "expected " + expected + ", actual " + end);
            }

            Long afterScan = latestSnapshotId(table);
            if (equalsNullable(end, afterScan)) {
                return;
            }
            if (expected != null && (afterScan == null || afterScan < expected)) {
                throw snapshotChanged(expected, afterScan, "during commit validation");
            }
            end = afterScan;
            if (end == null) {
                throw snapshotChanged(expected, end, "during commit validation");
            }
        }

        throw new IllegalStateException(
                "Paimon KEY_DYNAMIC table snapshot continued changing during commit validation: expected "
                        + expected + ", actual " + end);
    }

    /**
     * Validate a pinned KEY_DYNAMIC write against the snapshot already observed by Paimon's
     * atomic commit implementation. The caller must invoke this only inside that implementation's
     * catalog lock; this method deliberately performs no second latest-snapshot read.
     */
    static boolean validateSnapshotForAtomicCommit(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism,
            @Nullable Snapshot latestSnapshot,
            boolean rejectConcurrentSnapshot)
            throws Exception
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);

        if (!expectedSnapshot.pinned()) {
            throw new CommitValidationException(
                    "Paimon KEY_DYNAMIC commit is missing its pinned bootstrap snapshot");
        }

        Long expected = optionalSnapshotValue(expectedSnapshot.snapshotId());
        Long actual = latestSnapshot == null ? null : latestSnapshot.id();
        if (equalsNullable(actual, expected)) {
            return true;
        }
        if (rejectConcurrentSnapshot) {
            throw new CommitValidationException(snapshotChanged(expected, actual, "under the Paimon commit lock").getMessage());
        }
        if (expected != null && (actual == null || actual < expected)) {
            throw new CommitValidationException(snapshotChanged(expected, actual, "under the Paimon commit lock").getMessage());
        }
        if (actual == null) {
            throw new CommitValidationException(snapshotChanged(expected, null, "under the Paimon commit lock").getMessage());
        }

        try {
            checkSnapshotRangeCanRebase(table, expected, actual);
            KeyFingerprint inputKeys = readKeyFingerprints(table, queryId, expectedSnapshot, assignerParallelism);
            if (incrementalKeysIntersect(table, expected, actual, inputKeys)) {
                throw new CommitValidationException(
                        "Paimon KEY_DYNAMIC concurrent snapshot contains a primary key written by this query: "
                                + "expected " + expected + ", actual " + actual);
            }
            return true;
        }
        catch (CommitValidationException e) {
            throw e;
        }
        catch (Exception e) {
            throw new CommitValidationException(
                    "Paimon KEY_DYNAMIC commit validation failed under the Paimon commit lock", e);
        }
    }

    private static IllegalStateException snapshotChanged(@Nullable Long expected, @Nullable Long actual, String phase)
    {
        return new IllegalStateException(
                "Paimon KEY_DYNAMIC table snapshot changed " + phase + ": expected " + expected + ", actual " + actual);
    }

    private static void checkSnapshotRangeCanRebase(FileStoreTable table, @Nullable Long start, long end)
    {
        long first = start == null ? Snapshot.FIRST_SNAPSHOT_ID : start + 1;
        for (long snapshotId = first; snapshotId <= end; snapshotId++) {
            Snapshot snapshot = table.store().snapshotManager().snapshot(snapshotId);
            // COMPACT rewrites files without changing key-to-(partition,bucket) state and ANALYZE
            // only updates statistics. Both are ignored by Paimon's DELTA scanner and can be
            // safely rebased against the pinned global-key index. Keep unknown future kinds
            // fail-closed until their effect on the index is understood.
            if (snapshot.commitKind() != Snapshot.CommitKind.APPEND
                    && snapshot.commitKind() != Snapshot.CommitKind.COMPACT
                    && snapshot.commitKind() != Snapshot.CommitKind.ANALYZE) {
                throw new IllegalStateException(
                        "Paimon KEY_DYNAMIC cannot validate concurrent " + snapshot.commitKind()
                                + " snapshot " + snapshotId + "; refusing to mix bootstrap state with it");
            }
        }
    }

    private static boolean incrementalKeysIntersect(
            FileStoreTable table,
            @Nullable Long start,
            long end,
            KeyFingerprint inputKeys)
            throws IOException
    {
        long startNanos = System.nanoTime();
        long startSnapshot = start == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : start;
        Map<String, String> scanOptions = new HashMap<>();
        scanOptions.put(INCREMENTAL_BETWEEN.key(), startSnapshot + "," + end);
        scanOptions.put(INCREMENTAL_BETWEEN_SCAN_MODE.key(), DELTA.toString());

        FileStoreTable scanTable = table.copy(scanOptions);
        List<String> fieldNames = scanTable.rowType().getFieldNames();
        int[] keyAndPartitionProjection = Stream
                .concat(scanTable.schema().trimmedPrimaryKeys().stream(), scanTable.schema().partitionKeys().stream())
                .map(fieldNames::indexOf)
                .mapToInt(Integer::intValue)
                .toArray();
        ReadBuilder readBuilder = scanTable.newReadBuilder().withProjection(keyAndPartitionProjection);
        DataTableScan tableScan = (DataTableScan) readBuilder.newScan();
        List<Split> splits = tableScan.withLevelFilter(_ -> true).plan().splits();
        VALIDATION_SCAN_FILES.addAndGet(splits.stream()
                .map(DataSplit.class::cast)
                .mapToLong(split -> split.dataFiles().size())
                .sum());
        VALIDATION_SCAN_BYTES.addAndGet(splits.stream()
                .map(DataSplit.class::cast)
                .flatMap(split -> split.dataFiles().stream())
                .mapToLong(DataFileMeta::fileSize)
                .sum());
        TableRead read = readBuilder.newRead();
        KeyPartPartitionKeyExtractor keyExtractor = new KeyPartPartitionKeyExtractor(scanTable.schema());
        try {
            for (Split split : splits) {
                try (RecordReader<InternalRow> reader = read.createReader(split)) {
                    RecordIterator<InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        try {
                            InternalRow row;
                            while ((row = batch.next()) != null) {
                                if (inputKeys.mightContain(keyExtractor.trimmedPrimaryKey(row))) {
                                    return true;
                                }
                            }
                        }
                        finally {
                            batch.releaseBatch();
                        }
                    }
                }
            }
            return false;
        }
        finally {
            VALIDATION_SCAN_NANOS.addAndGet(System.nanoTime() - startNanos);
        }
    }

    static ValidationScanMetrics validationScanMetrics()
    {
        return new ValidationScanMetrics(
                VALIDATION_SCAN_BYTES.get(),
                VALIDATION_SCAN_FILES.get(),
                VALIDATION_SCAN_NANOS.get());
    }

    record ValidationScanMetrics(long bytes, long files, long nanos) {}

    private static KeyFingerprint readKeyFingerprints(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism)
            throws IOException
    {
        Path root = artifactRoot(
                table,
                queryId,
                expectedSnapshot.pinned() ? optionalSnapshotValue(expectedSnapshot.snapshotId()) : null,
                assignerParallelism);
        KeyFingerprint result = KeyFingerprint.empty();
        FileIO fileIO = table.fileIO();
        Path directory = new Path(root, KEY_FINGERPRINT_DIRECTORY);
        FileStatus[] statuses = fileIO.listStatus(directory);
        for (int assigner = 0; assigner < assignerParallelism; assigner++) {
            String prefix = "part-" + assigner + "-";
            boolean found = false;
            for (FileStatus status : statuses) {
                if (!status.isDir() && status.getPath().getName().startsWith(prefix)) {
                    found = true;
                    result.merge(KeyFingerprint.read(
                            fileIO,
                            status.getPath(),
                            schemaFingerprint(table.schema()),
                            assignerParallelism,
                            assigner));
                }
            }
            if (!found) {
                throw new IOException(
                        "Missing Paimon KEY_DYNAMIC key fingerprint sidecar for assigner " + assigner + ": " + directory);
            }
        }
        return result;
    }

    static OptionalSnapshot snapshotFor(PaimonTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (!tableHandle.isKeyDynamicBootstrapSnapshotPlanned()) {
            return OptionalSnapshot.unpinned();
        }
        return OptionalSnapshot.pinned(tableHandle.getKeyDynamicBootstrapSnapshot());
    }

    private static void generate(
            FileIO fileIO,
            Path root,
            Path manifest,
            FileStoreTable table,
            @Nullable Long snapshot,
            int assignerParallelism)
            throws Exception
    {
        Path attempt = new Path(root, "attempt-" + UUID.randomUUID());
        fileIO.mkdirs(attempt);
        List<ShardWriter> writers = new ArrayList<>();
        try {
            RowType bootstrapType = IndexBootstrap.bootstrapType(table.schema());
            for (int assigner = 0; assigner < assignerParallelism; assigner++) {
                writers.add(new ShardWriter(fileIO, new Path(attempt, "part-" + assigner), bootstrapType));
            }

            KeyPartPartitionKeyExtractor keyExtractor = new KeyPartPartitionKeyExtractor(table.schema());
            try (RecordReader<InternalRow> reader = SnapshotPinnedBootstrap.bootstrap(table, snapshot)) {
                RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            BinaryRow key = keyExtractor.trimmedPrimaryKey(row);
                            int assigner = Math.abs(key.hashCode() % assignerParallelism);
                            writers.get(assigner).write(row);
                        }
                    }
                    finally {
                        batch.releaseBatch();
                    }
                }
            }
            closeWriters(writers);
            List<ShardMetadata> shardMetadata = writersFromClosed(writers);

            Path attemptManifest = new Path(attempt, MANIFEST_FILE);
            writeManifest(
                    fileIO,
                    attemptManifest,
                    table.schema(),
                    snapshot,
                    assignerParallelism,
                    attempt.getName(),
                    shardMetadata);
            if (!fileIO.rename(attemptManifest, manifest)) {
                fileIO.delete(attempt, true);
                return;
            }
        }
        catch (Exception e) {
            closeWriters(writers, e);
            fileIO.deleteDirectoryQuietly(attempt);
            throw e;
        }
    }

    private static List<ShardMetadata> writersFromClosed(List<ShardWriter> writers)
    {
        return writers.stream().map(ShardWriter::metadata).collect(Collectors.toUnmodifiableList());
    }

    private static void closeWriters(List<ShardWriter> writers)
            throws IOException
    {
        IOException failure = null;
        for (ShardWriter writer : writers) {
            try {
                writer.close();
            }
            catch (IOException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void closeWriters(List<ShardWriter> writers, Exception failure)
    {
        for (ShardWriter writer : writers) {
            try {
                writer.close();
            }
            catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
    }

    private static void writeManifest(
            FileIO fileIO,
            Path path,
            TableSchema schema,
            @Nullable Long snapshot,
            int assignerParallelism,
            String attempt,
            List<ShardMetadata> shards)
            throws IOException
    {
        try (PositionOutputStream output = fileIO.newOutputStream(path, false);
                DataOutputStream data = new DataOutputStream(output)) {
            data.writeInt(MANIFEST_MAGIC);
            data.writeInt(MANIFEST_VERSION);
            data.writeBoolean(snapshot != null);
            if (snapshot != null) {
                data.writeLong(snapshot);
            }
            data.writeInt(assignerParallelism);
            data.writeUTF(schemaFingerprint(schema));
            data.writeUTF(attempt);
            data.writeInt(shards.size());
            for (ShardMetadata shard : shards) {
                data.writeLong(shard.records());
                data.writeLong(shard.length());
                data.writeUTF(shard.sha256());
            }
        }
    }

    private static Artifact readManifest(
            FileIO fileIO,
            Path manifest,
            Path root,
            FileStoreTable table,
            @Nullable Long expectedSnapshot,
            int expectedParallelism)
            throws IOException
    {
        ManifestData data;
        try (InputStream input = fileIO.newInputStream(manifest);
                DataInputStream stream = new DataInputStream(input)) {
            int magic = stream.readInt();
            int version = stream.readInt();
            if (magic != MANIFEST_MAGIC || version != MANIFEST_VERSION) {
                throw new IOException("Unsupported Paimon KEY_DYNAMIC bootstrap manifest: " + manifest);
            }
            boolean hasSnapshot = stream.readBoolean();
            Long snapshot = hasSnapshot ? stream.readLong() : null;
            int parallelism = stream.readInt();
            String schemaFingerprint = stream.readUTF();
            String attempt = stream.readUTF();
            int shardCount = stream.readInt();
            if (shardCount < 0 || shardCount > expectedParallelism) {
                throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap shard count: " + shardCount);
            }
            List<ShardMetadata> shards = new ArrayList<>(shardCount);
            for (int index = 0; index < shardCount; index++) {
                long records = stream.readLong();
                long length = stream.readLong();
                String checksum = stream.readUTF();
                if (records < 0 || length < 0 || !checksum.matches("[0-9a-f]{64}")) {
                    throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap shard metadata: " + manifest);
                }
                shards.add(new ShardMetadata(records, length, checksum));
            }
            if (attempt.isBlank() || attempt.contains("/") || attempt.contains("\\") || attempt.equals(".")
                    || attempt.equals("..")) {
                throw new IOException("Invalid Paimon KEY_DYNAMIC bootstrap attempt: " + attempt);
            }
            data = new ManifestData(snapshot, parallelism, schemaFingerprint, attempt, List.copyOf(shards));
        }

        if (!equalsNullable(expectedSnapshot, data.snapshot())
                || data.parallelism() != expectedParallelism
                || !schemaFingerprint(table.schema()).equals(data.schemaFingerprint())
                || data.shards().size() != expectedParallelism) {
            throw new IOException("Paimon KEY_DYNAMIC bootstrap manifest does not match the planned write: " + manifest);
        }
        return new Artifact(fileIO, root, data, IndexBootstrap.bootstrapType(table.schema()));
    }

    private static Long snapshotForBootstrap(FileStoreTable table, OptionalSnapshot expectedSnapshot)
    {
        Long currentSnapshot = latestSnapshotId(table);
        if (expectedSnapshot.pinned()) {
            Long snapshot = optionalSnapshotValue(expectedSnapshot.snapshotId());
            if (snapshot != null && !table.store().snapshotManager().snapshotExists(snapshot)) {
                throw new IllegalStateException("Paimon KEY_DYNAMIC bootstrap snapshot no longer exists: " + snapshot);
            }
            return snapshot;
        }
        Long snapshot = currentSnapshot;
        return snapshot;
    }

    private static Path artifactRoot(FileStoreTable table, String queryId, @Nullable Long snapshot, int parallelism)
    {
        // Query id and pinned snapshot provide the artifact identity. Keep the current schema out
        // of the path so a schema change cannot strand the old artifact before cleanup.
        String identity = table.location() + "\n" + queryId + "\n" + snapshot + "\n" + parallelism;
        return new Path(table.location(), ROOT_DIRECTORY + "/" + sha256(identity));
    }

    static KeyFingerprintWriter openKeyFingerprintWriter(
            FileStoreTable table,
            String queryId,
            OptionalSnapshot expectedSnapshot,
            int assignerParallelism,
            int assigner,
            long writerId)
            throws IOException
    {
        requireNonNull(table, "table is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(expectedSnapshot, "expectedSnapshot is null");
        checkArgument(!queryId.isBlank(), "queryId is blank");
        checkArgument(assignerParallelism > 0, "assignerParallelism must be positive: %s", assignerParallelism);
        checkArgument(assigner >= 0 && assigner < assignerParallelism,
                "assigner must be within fingerprint parallelism: %s",
                assigner);

        Path root = artifactRoot(
                table,
                queryId,
                expectedSnapshot.pinned() ? optionalSnapshotValue(expectedSnapshot.snapshotId()) : null,
                assignerParallelism);
        FileIO fileIO = table.fileIO();
        fileIO.mkdirs(new Path(root, KEY_FINGERPRINT_DIRECTORY));
        return new KeyFingerprintWriter(
                fileIO,
                keyFingerprintPath(root, assigner, writerId),
                schemaFingerprint(table.schema()),
                assignerParallelism,
                assigner);
    }

    private static Path keyFingerprintPath(Path root, int assigner, long writerId)
    {
        // PageSinkId identifies a task, but a task can create more than one writer driver.
        // Keep every finished sidecar so retries and multiple drivers can be merged at commit.
        return new Path(
                new Path(root, KEY_FINGERPRINT_DIRECTORY),
                "part-" + assigner + "-" + writerId + "-" + UUID.randomUUID());
    }

    private static Long latestSnapshotId(FileStoreTable table)
    {
        return table.store().snapshotManager().latestSnapshotId();
    }

    private static boolean equalsNullable(@Nullable Long left, @Nullable Long right)
    {
        return left == null ? right == null : left.equals(right);
    }

    @Nullable
    private static Long optionalSnapshotValue(OptionalLong snapshot)
    {
        return snapshot.isPresent() ? snapshot.orElseThrow() : null;
    }

    private static String schemaFingerprint(TableSchema schema)
    {
        return sha256(schema.toString());
    }

    private static String sha256(String value)
    {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte valueByte : digest) {
                result.append(String.format("%02x", valueByte));
            }
            return result.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    record OptionalSnapshot(boolean pinned, OptionalLong snapshotId)
    {
        OptionalSnapshot
        {
            requireNonNull(snapshotId, "snapshotId is null");
            if (snapshotId.isPresent() && snapshotId.orElseThrow() < 0) {
                throw new IllegalArgumentException("snapshotId must be non-negative");
            }
        }

        static OptionalSnapshot pinned(OptionalLong snapshotId)
        {
            return new OptionalSnapshot(true, snapshotId);
        }

        static OptionalSnapshot unpinned()
        {
            return new OptionalSnapshot(false, OptionalLong.empty());
        }
    }

    static final class Artifact
    {
        private final FileIO fileIO;
        private final Path root;
        private final ManifestData manifest;
        private final RowType bootstrapType;

        private Artifact(FileIO fileIO, Path root, ManifestData manifest, RowType bootstrapType)
        {
            this.fileIO = fileIO;
            this.root = root;
            this.manifest = manifest;
            this.bootstrapType = bootstrapType;
        }

        ShardReader openShard(int assigner)
                throws IOException
        {
            checkArgument(assigner >= 0 && assigner < manifest.shards().size(),
                    "assigner must be within bootstrap shard count: %s",
                    assigner);
            ShardMetadata metadata = manifest.shards().get(assigner);
            Path path = new Path(new Path(root, manifest.attempt()), "part-" + assigner);
            if (fileIO.getFileStatus(path).getLen() != metadata.length()) {
                throw new IOException("Paimon KEY_DYNAMIC bootstrap shard length changed: " + path);
            }
            return new ShardReader(fileIO, path, metadata, bootstrapType);
        }

        List<Long> recordCounts()
        {
            return manifest.shards().stream().map(ShardMetadata::records).toList();
        }
    }

    static final class ShardReader
            implements AutoCloseable
    {
        private final DataInputViewStreamWrapper input;
        private final CountingInputStream countedInput;
        private final DigestInputStream digestInput;
        private final InternalRowSerializer serializer;
        private final ShardMetadata metadata;
        private long remaining;
        private boolean closed;

        private ShardReader(FileIO fileIO, Path path, ShardMetadata metadata, RowType rowType)
                throws IOException
        {
            this.digestInput = new DigestInputStream(fileIO.newInputStream(path), newDigest());
            this.countedInput = new CountingInputStream(digestInput);
            this.input = new DataInputViewStreamWrapper(countedInput);
            this.serializer = new InternalRowSerializer(rowType);
            this.metadata = metadata;
            this.remaining = metadata.records();
        }

        @Nullable
        InternalRow next()
                throws IOException
        {
            if (remaining == 0) {
                close();
                return null;
            }
            InternalRow row = serializer.deserialize(input);
            remaining--;
            return row;
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            if (remaining == 0 && countedInput.count() != metadata.length()) {
                failure = new IOException("Paimon KEY_DYNAMIC bootstrap shard length does not match record data");
            }
            if (remaining == 0 && !metadata.sha256().equals(hex(digestInput.getMessageDigest().digest()))) {
                failure = new IOException("Paimon KEY_DYNAMIC bootstrap shard checksum mismatch");
            }
            try {
                input.close();
            }
            catch (IOException e) {
                if (failure == null) {
                    failure = e;
                }
                else {
                    failure.addSuppressed(e);
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class ShardWriter
    {
        private final PositionOutputStream output;
        private final DataOutputViewStreamWrapper data;
        private final DigestOutputStream digestOutput;
        private long records;
        private ShardMetadata metadata;
        private boolean closed;

        private ShardWriter(FileIO fileIO, Path path, RowType rowType)
                throws IOException
        {
            this.output = fileIO.newOutputStream(path, false);
            this.digestOutput = new DigestOutputStream(output, newDigest());
            this.data = new DataOutputViewStreamWrapper(digestOutput);
            this.serializer = new InternalRowSerializer(rowType);
        }

        private final InternalRowSerializer serializer;

        private void write(InternalRow row)
                throws IOException
        {
            serializer.serialize(row, data);
            records++;
        }

        private void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            data.flush();
            long length = output.getPos();
            data.close();
            metadata = new ShardMetadata(records, length, hex(digestOutput.getMessageDigest().digest()));
        }

        private ShardMetadata metadata()
        {
            return requireNonNull(metadata, "shard writer is not closed");
        }
    }

    private record ShardMetadata(long records, long length, String sha256) {}

    private record ManifestData(
            @Nullable Long snapshot,
            int parallelism,
            String schemaFingerprint,
            String attempt,
            List<ShardMetadata> shards) {}

    static final class KeyFingerprintWriter
            implements AutoCloseable
    {
        private final FileIO fileIO;
        private final Path path;
        private final String schemaFingerprint;
        private final int parallelism;
        private final int assigner;
        private final KeyFingerprint fingerprint = KeyFingerprint.empty();
        private boolean closed;

        KeyFingerprintWriter(
                FileIO fileIO,
                Path path,
                String schemaFingerprint,
                int parallelism,
                int assigner)
        {
            this.fileIO = requireNonNull(fileIO, "fileIO is null");
            this.path = requireNonNull(path, "path is null");
            this.schemaFingerprint = requireNonNull(schemaFingerprint, "schemaFingerprint is null");
            this.parallelism = parallelism;
            this.assigner = assigner;
        }

        void add(BinaryRow key)
        {
            checkState(!closed, "key fingerprint writer is already closed");
            fingerprint.add(requireNonNull(key, "key is null"));
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            byte[] payload = fingerprint.serialize(schemaFingerprint, parallelism, assigner);
            try (PositionOutputStream output = fileIO.newOutputStream(path, false);
                    DataOutputStream data = new DataOutputStream(output)) {
                data.write(payload);
                data.write(sha256Bytes(payload));
            }
            closed = true;
        }

        void abort()
        {
            closed = true;
            fileIO.deleteQuietly(path);
        }
    }

    static final class KeyFingerprint
    {
        private final long[] words;

        private KeyFingerprint(long[] words)
        {
            this.words = requireNonNull(words, "words is null");
            checkArgument(words.length == KEY_FINGERPRINT_WORDS,
                    "Unexpected key fingerprint word count: %s",
                    words.length);
        }

        static KeyFingerprint empty()
        {
            return new KeyFingerprint(new long[KEY_FINGERPRINT_WORDS]);
        }

        void add(BinaryRow key)
        {
            addHash(requireNonNull(key, "key is null").hashCode());
        }

        void addHash(int hash)
        {
            long unsignedHash = Integer.toUnsignedLong(hash);
            for (int salt = 0; salt < KEY_FINGERPRINT_HASHES; salt++) {
                int bit = fingerprintBit(unsignedHash, salt);
                words[bit >>> 6] |= 1L << (bit & 63);
            }
        }

        boolean mightContain(BinaryRow key)
        {
            return mightContainHash(requireNonNull(key, "key is null").hashCode());
        }

        boolean mightContainHash(int hash)
        {
            long unsignedHash = Integer.toUnsignedLong(hash);
            for (int salt = 0; salt < KEY_FINGERPRINT_HASHES; salt++) {
                int bit = fingerprintBit(unsignedHash, salt);
                if ((words[bit >>> 6] & (1L << (bit & 63))) == 0) {
                    return false;
                }
            }
            return true;
        }

        void merge(KeyFingerprint other)
        {
            requireNonNull(other, "other is null");
            for (int index = 0; index < words.length; index++) {
                words[index] |= other.words[index];
            }
        }

        byte[] serialize(String schemaFingerprint, int parallelism, int assigner)
                throws IOException
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(128 * 1024);
            try (DataOutputStream data = new DataOutputStream(bytes)) {
                data.writeInt(KEY_FINGERPRINT_MAGIC);
                data.writeInt(KEY_FINGERPRINT_VERSION);
                data.writeInt(KEY_FINGERPRINT_BITS);
                data.writeInt(KEY_FINGERPRINT_HASHES);
                data.writeInt(parallelism);
                data.writeInt(assigner);
                data.writeUTF(schemaFingerprint);
                data.writeInt(words.length);
                for (long word : words) {
                    data.writeLong(word);
                }
            }
            return bytes.toByteArray();
        }

        static KeyFingerprint read(
                FileIO fileIO,
                Path path,
                String expectedSchemaFingerprint,
                int expectedParallelism,
                int expectedAssigner)
                throws IOException
        {
            long length = fileIO.getFileStatus(path).getLen();
            if (length <= KEY_FINGERPRINT_DIGEST_BYTES || length > 2 * 1024 * 1024
                    || length > Integer.MAX_VALUE) {
                throw new IOException("Invalid Paimon KEY_DYNAMIC key fingerprint length: " + path);
            }
            byte[] encoded;
            try (InputStream input = fileIO.newInputStream(path)) {
                encoded = input.readAllBytes();
            }
            if (encoded.length != length || encoded.length <= KEY_FINGERPRINT_DIGEST_BYTES) {
                throw new IOException("Paimon KEY_DYNAMIC key fingerprint length changed: " + path);
            }
            int payloadLength = encoded.length - KEY_FINGERPRINT_DIGEST_BYTES;
            byte[] payload = Arrays.copyOf(encoded, payloadLength);
            byte[] expectedDigest = Arrays.copyOfRange(encoded, payloadLength, encoded.length);
            if (!MessageDigest.isEqual(expectedDigest, sha256Bytes(payload))) {
                throw new IOException("Paimon KEY_DYNAMIC key fingerprint checksum mismatch: " + path);
            }

            try (DataInputStream data = new DataInputStream(new ByteArrayInputStream(payload))) {
                if (data.readInt() != KEY_FINGERPRINT_MAGIC
                        || data.readInt() != KEY_FINGERPRINT_VERSION
                        || data.readInt() != KEY_FINGERPRINT_BITS
                        || data.readInt() != KEY_FINGERPRINT_HASHES
                        || data.readInt() != expectedParallelism
                        || data.readInt() != expectedAssigner
                        || !expectedSchemaFingerprint.equals(data.readUTF())) {
                    throw new IOException("Paimon KEY_DYNAMIC key fingerprint metadata mismatch: " + path);
                }
                int wordCount = data.readInt();
                if (wordCount != KEY_FINGERPRINT_WORDS) {
                    throw new IOException("Paimon KEY_DYNAMIC key fingerprint word count mismatch: " + path);
                }
                long[] words = new long[wordCount];
                for (int index = 0; index < wordCount; index++) {
                    words[index] = data.readLong();
                }
                if (data.available() != 0) {
                    throw new IOException("Paimon KEY_DYNAMIC key fingerprint has trailing data: " + path);
                }
                return new KeyFingerprint(words);
            }
        }

        private static int fingerprintBit(long hash, int salt)
        {
            long value = hash + 0x9E3779B97F4A7C15L * (salt + 1);
            value = mix64(value);
            return (int) value & (KEY_FINGERPRINT_BITS - 1);
        }

        private static long mix64(long value)
        {
            value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
            value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
            return value ^ (value >>> 31);
        }
    }

    private static MessageDigest newDigest()
    {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] sha256Bytes(byte[] value)
    {
        return newDigest().digest(requireNonNull(value, "value is null"));
    }

    private static String hex(byte[] bytes)
    {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value));
        }
        return result.toString();
    }

    private static final class CountingInputStream
            extends InputStream
    {
        private final InputStream delegate;
        private long count;

        private CountingInputStream(InputStream delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public int read()
                throws IOException
        {
            int value = delegate.read();
            if (value >= 0) {
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length)
                throws IOException
        {
            int read = delegate.read(bytes, offset, length);
            if (read > 0) {
                count += read;
            }
            return read;
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        private long count()
        {
            return count;
        }
    }

    private static final class SnapshotPinnedBootstrap
    {
        private SnapshotPinnedBootstrap() {}

        private static RecordReader<InternalRow> bootstrap(FileStoreTable table, @Nullable Long snapshot)
                throws IOException
        {
            RowType rowType = table.rowType();
            List<String> fieldNames = rowType.getFieldNames();
            int[] keyProjection = table.schema().trimmedPrimaryKeys().stream()
                    .map(fieldNames::indexOf)
                    .mapToInt(Integer::intValue)
                    .toArray();

            Map<String, String> scanOptions = new HashMap<>();
            if (snapshot == null) {
                scanOptions.put(SCAN_MODE.key(), LATEST.toString());
            }
            else {
                scanOptions.put(SCAN_MODE.key(), FROM_SNAPSHOT.toString());
                scanOptions.put(SCAN_SNAPSHOT_ID.key(), snapshot.toString());
            }
            FileStoreTable scanTable = table.copy(scanOptions);
            ReadBuilder readBuilder = scanTable.newReadBuilder().withProjection(keyProjection);
            DataTableScan tableScan = (DataTableScan) readBuilder.newScan();
            List<Split> splits = tableScan
                    .withLevelFilter(_ -> true)
                    .plan()
                    .splits();

            CoreOptions options = CoreOptions.fromMap(scanTable.options());
            Duration indexTtl = options.crossPartitionUpsertIndexTtl();
            if (indexTtl != null) {
                long ttlMillis = indexTtl.toMillis();
                long currentTime = System.currentTimeMillis();
                splits = splits.stream()
                        .filter(split -> filterSplit(split, ttlMillis, currentTime))
                        .collect(Collectors.toList());
            }

            RowDataToObjectArrayConverter partBucketConverter = new RowDataToObjectArrayConverter(
                    TypeUtils.concat(TypeUtils.project(rowType, table.partitionKeys()),
                            RowType.of(DataTypes.INT())));
            return parallelExecute(
                    TypeUtils.project(rowType, keyProjection),
                    split -> readBuilder.newRead().createReader(split),
                    splits,
                    options.pageSize(),
                    options.crossPartitionUpsertBootstrapParallelism(),
                    split -> {
                        DataSplit dataSplit = (DataSplit) split;
                        return partBucketConverter.toGenericRow(
                                new JoinedRow(
                                        dataSplit.partition(),
                                        GenericRow.of(dataSplit.bucket())));
                    },
                    (row, extra) -> new JoinedRow().replace(row, extra));
        }

        private static boolean filterSplit(Split split, long indexTtl, long currentTime)
        {
            for (DataFileMeta file : ((DataSplit) split).dataFiles()) {
                if (currentTime <= file.creationTimeEpochMillis() + indexTtl) {
                    return true;
                }
            }
            return false;
        }
    }
}
