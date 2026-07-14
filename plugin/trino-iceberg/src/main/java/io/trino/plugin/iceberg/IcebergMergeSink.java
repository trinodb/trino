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
package io.trino.plugin.iceberg;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.plugin.iceberg.delete.PositionDeleteWriter;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MergePage;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergMergeSink
        implements ConnectorMergeSink
{
    private static final Logger log = Logger.get(IcebergMergeSink.class);

    private final int formatVersion;
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final TrinoFileSystem fileSystem;
    private final ForwardingFileIoFactory fileIoFactory;
    private final ExecutorService copyOnWriteRewriteExecutor;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final IcebergFileFormat fileFormat;
    private final Map<String, String> fileIoProperties;
    private final Map<String, String> storageProperties;
    private final Schema schema;
    private final Schema rewriteSchema;
    private final Map<Integer, PartitionSpec> partitionsSpecs;
    private final ConnectorPageSink insertPageSink;
    private final int columnCount;
    private final int writeSortOrderId;
    private final RowLevelOperationMode rowLevelOperationMode;
    private final CopyOnWriteFileRewriter copyOnWriteFileRewriter;
    // Frozen base-table properties captured once on the coordinator in beginMerge and shipped via
    // IcebergMergeTableHandle. Workers no longer parse metadata.json to recover these values.
    private final Map<String, String> baseTableProperties;
    // Pre-existing position/equality delete files per data file, planned once on the coordinator
    // in IcebergMetadata.beginMerge and shipped here via IcebergMergeTableHandle. Workers no
    // longer issue newScan().useSnapshot().planFiles() against the manifest list.
    private final Map<String, List<DeleteFile>> preExistingDeletesByDataFile;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();
    // Paths of CoW rewrite output files submitted to the executor but not yet handed off to the
    // coordinator via the fragment list. Populated before supplyAsync and drained in the task's
    // finally block. On abort(), any remaining paths are bulk-deleted so orphans from in-flight
    // tasks are recovered as soon as the tasks (which cancel(true) cannot interrupt) complete.
    private final Set<String> inFlightRewritePaths = ConcurrentHashMap.newKeySet();
    private long writtenBytes;

    public IcebergMergeSink(
            int formatVersion,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            TrinoFileSystem fileSystem,
            ForwardingFileIoFactory fileIoFactory,
            ExecutorService copyOnWriteRewriteExecutor,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            IcebergFileFormat fileFormat,
            Map<String, String> fileIoProperties,
            Map<String, String> storageProperties,
            Schema schema,
            Map<Integer, PartitionSpec> partitionsSpecs,
            ConnectorPageSink insertPageSink,
            int columnCount,
            int writeSortOrderId,
            RowLevelOperationMode rowLevelOperationMode,
            CopyOnWriteFileRewriter copyOnWriteFileRewriter,
            Map<String, String> baseTableProperties,
            Map<String, List<DeleteFile>> preExistingDeletesByDataFile)
    {
        this.formatVersion = formatVersion;
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.copyOnWriteRewriteExecutor = requireNonNull(copyOnWriteRewriteExecutor, "copyOnWriteRewriteExecutor is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.fileIoProperties = ImmutableMap.copyOf(requireNonNull(fileIoProperties, "fileIoProperties is null"));
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.schema = requireNonNull(schema, "schema is null");
        this.rewriteSchema = getRewriteSchema(this.schema, formatVersion);
        this.partitionsSpecs = ImmutableMap.copyOf(requireNonNull(partitionsSpecs, "partitionsSpecs is null"));
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.columnCount = columnCount;
        this.writeSortOrderId = writeSortOrderId;
        this.rowLevelOperationMode = requireNonNull(rowLevelOperationMode, "rowLevelOperationMode is null");
        this.copyOnWriteFileRewriter = requireNonNull(copyOnWriteFileRewriter, "copyOnWriteFileRewriter is null");
        this.baseTableProperties = ImmutableMap.copyOf(requireNonNull(baseTableProperties, "baseTableProperties is null"));
        this.preExistingDeletesByDataFile = ImmutableMap.copyOf(requireNonNull(preExistingDeletesByDataFile, "preExistingDeletesByDataFile is null"));
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = MergePage.createDeleteAndInsertPages(page, columnCount);

        mergePage.getDeletionsPage().ifPresent(this::processRemovals);
        mergePage.getInsertionsPage().ifPresent(insertionsPage -> {
            if (formatVersion >= 3) {
                insertPageSink.appendPage(createInsertionsPageWithRowLineage(insertionsPage, page));
            }
            else {
                insertPageSink.appendPage(insertionsPage);
            }
        });

        writtenBytes = insertPageSink.getCompletedBytes();
    }

    private void processRemovals(Page removals)
    {
        List<Block> fields = RowBlock.getRowFieldsFromBlock(removals.getBlock(removals.getChannelCount() - 1));
        Block filePathBlock = fields.get(0);
        Block rowPositionBlock = fields.get(1);
        Block partitionSpecIdBlock = fields.get(2);
        Block partitionDataBlock = fields.get(3);
        // fields[4] = sourceRowId (not needed for CoW)
        Block fileFormatBlock = fields.get(5);
        Block fileSizeBlock = fields.get(6);
        Block fileRecordCountBlock = fields.get(7);
        Block dataSequenceNumberBlock = fields.get(8);
        Block fileFirstRowIdBlock = fields.get(9);
        for (int position = 0; position < filePathBlock.getPositionCount(); position++) {
            Slice filePath = VarcharType.VARCHAR.getSlice(filePathBlock, position);
            long rowPosition = BIGINT.getLong(rowPositionBlock, position);

            int index = position;
            FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, _ -> {
                int partitionSpecId = INTEGER.getInt(partitionSpecIdBlock, index);
                String partitionData = VarcharType.VARCHAR.getSlice(partitionDataBlock, index).toStringUtf8();
                int fileFormatOrdinal = INTEGER.getInt(fileFormatBlock, index);
                IcebergFileFormat[] formats = IcebergFileFormat.values();
                verify(fileFormatOrdinal >= 0 && fileFormatOrdinal < formats.length, "Invalid file format ordinal: %s", fileFormatOrdinal);
                IcebergFileFormat fileFormat = formats[fileFormatOrdinal];
                long fileSize = BIGINT.getLong(fileSizeBlock, index);
                long fileRecordCount = BIGINT.getLong(fileRecordCountBlock, index);
                long dataSequenceNumber = BIGINT.getLong(dataSequenceNumberBlock, index);
                OptionalLong fileFirstRowId = fileFirstRowIdBlock.isNull(index)
                        ? OptionalLong.empty()
                        : OptionalLong.of(BIGINT.getLong(fileFirstRowIdBlock, index));
                return new FileDeletion(partitionSpecId, partitionData, fileFormat, fileSize, fileRecordCount, dataSequenceNumber, fileFirstRowId);
            });

            deletion.rowsToDelete().add(rowPosition);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (rowLevelOperationMode == RowLevelOperationMode.COPY_ON_WRITE) {
            return finishCopyOnWrite();
        }
        return finishMergeOnRead();
    }

    /**
     * For copy-on-write mode, each merge-writer task has the complete deletion
     * vector for its files (guaranteed by file-path-based update partitioning
     * via IcebergUpdateBucketFunction). This method rewrites affected files in
     * parallel using CopyOnWriteFileRewriter while applying pre-existing delete
     * files (position/equality) in read path, and produces rewrite fragments for
     * the coordinator to commit.
     */
    private CompletableFuture<Collection<Slice>> finishCopyOnWrite()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());
        writtenBytes = insertPageSink.getCompletedBytes();

        if (fileDeletions.isEmpty()) {
            return completedFuture(fragments);
        }

        long startNanos = System.nanoTime();
        List<RewriteInput> inputs = buildRewriteInputs();
        List<CopyOnWriteFileRewriter.RewriteResult> results = executeParallelRewrites(inputs, baseTableProperties);
        collectRewriteFragments(inputs, results, getWriteFileFormat(baseTableProperties), fragments, startNanos);

        return completedFuture(fragments);
    }

    private record RewriteInput(
            String originalPath,
            DeletionVector deletionVector,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData,
            String partitionDataJson,
            IcebergFileFormat sourceFileFormat,
            long sourceFileSizeInBytes,
            long sourceRecordCount,
            long sourceDataSequenceNumber,
            OptionalLong sourceFileFirstRowId,
            List<DeleteFile> preExistingDeletes) {}

    private List<RewriteInput> buildRewriteInputs()
    {
        Map<String, DeletionVector.Builder> deletionVectors = new HashMap<>();
        Map<String, FileDeletion> deletionDetails = new HashMap<>();
        fileDeletions.forEach((dataFilePath, deletion) -> {
            String path = dataFilePath.toStringUtf8();
            deletionVectors.put(path, deletion.rowsToDelete());
            deletionDetails.put(path, deletion);
        });

        // Pre-existing deletes map was planned once on the coordinator in
        // IcebergMetadata.beginMerge and shipped via IcebergMergeTableHandle; look up per file
        // directly instead of re-scanning the snapshot's manifest list on every worker.

        List<RewriteInput> inputs = new ArrayList<>();
        for (Map.Entry<String, DeletionVector.Builder> entry : deletionVectors.entrySet()) {
            String originalPath = entry.getKey();
            DeletionVector deletionVector = entry.getValue().build()
                    .orElseThrow(() -> new VerifyException("Empty aggregated deletion vector"));
            FileDeletion detail = deletionDetails.get(originalPath);
            verify(detail != null, "Missing deletion detail for file: %s", originalPath);
            PartitionSpec partitionSpec = partitionsSpecs.get(detail.partitionSpecId());
            verify(partitionSpec != null, "Unknown partition spec ID %s for file: %s", detail.partitionSpecId(), originalPath);
            Optional<PartitionData> partitionData = createPartitionData(partitionSpec, detail.partitionDataJson());
            List<DeleteFile> preExistingDeletes = preExistingDeletesByDataFile.getOrDefault(originalPath, List.of());
            inputs.add(new RewriteInput(
                    originalPath,
                    deletionVector,
                    partitionSpec,
                    partitionData,
                    detail.partitionDataJson(),
                    detail.fileFormat(),
                    detail.fileSizeInBytes(),
                    detail.fileRecordCount(),
                    detail.dataSequenceNumber(),
                    detail.fileFirstRowId(),
                    preExistingDeletes));
        }
        return inputs;
    }

    private List<CopyOnWriteFileRewriter.RewriteResult> executeParallelRewrites(
            List<RewriteInput> inputs,
            Map<String, String> tableProperties)
    {
        MetricsConfig metricsConfig = MetricsConfig.fromProperties(tableProperties);
        Optional<NameMapping> nameMapping = Optional.ofNullable(tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING))
                .map(NameMappingParser::fromJson);
        IcebergFileFormat writeFileFormat = getWriteFileFormat(tableProperties);

        List<CompletableFuture<CopyOnWriteFileRewriter.RewriteResult>> futures = new ArrayList<>(inputs.size());
        for (RewriteInput input : inputs) {
            // Compute the destination path up front so abort() can clean it up even if the
            // supplyAsync task is still running (CompletableFuture.cancel cannot interrupt it).
            String newFilePath = CopyOnWriteFileRewriter.computeNewFilePath(
                    session, writeFileFormat, locationProvider, input.partitionSpec(), input.partitionData());
            inFlightRewritePaths.add(newFilePath);
            futures.add(CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return copyOnWriteFileRewriter.rewriteFile(
                                    session,
                                    input.originalPath(),
                                    newFilePath,
                                    input.sourceFileSizeInBytes(),
                                    input.deletionVector(),
                                    rewriteSchema,
                                    input.partitionSpec(),
                                    input.partitionData(),
                                    input.preExistingDeletes(),
                                    input.sourceDataSequenceNumber(),
                                    input.sourceFileFirstRowId(),
                                    input.sourceFileFormat(),
                                    writeFileFormat,
                                    metricsConfig,
                                    fileIoProperties,
                                    tableProperties,
                                    nameMapping);
                        }
                        finally {
                            // Normal completion: the successful task's rollback is captured in the
                            // returned RewriteResult; a failed task has already self-cleaned in
                            // finalizeRewrite. Either way, abort() no longer needs to track this path.
                            inFlightRewritePaths.remove(newFilePath);
                        }
                    },
                    copyOnWriteRewriteExecutor));
        }

        return awaitAllRewritesOrRollback(futures);
    }

    /**
     * Waits for every submitted rewrite future to settle, then either returns every
     * {@link CopyOnWriteFileRewriter.RewriteResult} or, if any rewrite failed, rolls back every
     * successful one and rethrows the first failure with the rest attached as suppressed.
     *
     * <p>Cancelling via {@link CompletableFuture#cancel(boolean)} is deliberately avoided: it
     * does not interrupt a running {@code supplyAsync} task, so a "late" rewrite could still
     * commit a new file after the caller walked away from its future, leaving an orphan that
     * could not be reached through the {@link CopyOnWriteFileRewriter.RewriteResult}.
     */
    static List<CopyOnWriteFileRewriter.RewriteResult> awaitAllRewritesOrRollback(
            List<CompletableFuture<CopyOnWriteFileRewriter.RewriteResult>> futures)
    {
        List<CopyOnWriteFileRewriter.RewriteResult> results = new ArrayList<>(futures.size());
        Throwable failure = null;
        for (CompletableFuture<CopyOnWriteFileRewriter.RewriteResult> future : futures) {
            try {
                results.add(future.join());
            }
            catch (CompletionException | CancellationException e) {
                Throwable unwrapped = e instanceof CompletionException completion && completion.getCause() != null
                        ? completion.getCause()
                        : e;
                if (failure == null) {
                    failure = unwrapped;
                }
                else if (failure != unwrapped) {
                    failure.addSuppressed(unwrapped);
                }
            }
        }
        if (failure == null) {
            return results;
        }
        // Every future has settled above. Roll back every successful rewrite before surfacing
        // the failure so no CoW output is left orphaned on storage.
        for (CopyOnWriteFileRewriter.RewriteResult result : results) {
            try {
                result.rollbackAction().run();
            }
            catch (Exception suppressed) {
                failure.addSuppressed(suppressed);
            }
        }
        throw failure instanceof RuntimeException re ? re : new RuntimeException(failure);
    }

    private void collectRewriteFragments(
            List<RewriteInput> inputs,
            List<CopyOnWriteFileRewriter.RewriteResult> results,
            IcebergFileFormat writeFileFormat,
            List<Slice> fragments,
            long startNanos)
    {
        // Capture rollback handles for every successful rewrite up front, so any failure in
        // fragment assembly below rolls back every already-committed new file rather than only
        // the prefix we have iterated. Each rewrite task's inFlightRewritePaths entry has
        // already been cleared in executeParallelRewrites' finally block, so this is the only
        // remaining handle to those files.
        List<RollbackAction> rollbackActions = new ArrayList<>(results.size());
        for (CopyOnWriteFileRewriter.RewriteResult result : results) {
            rollbackActions.add(result.rollbackAction());
        }
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalRowsDeleted = 0;
        int filesRemoved = 0;
        try {
            for (int i = 0; i < results.size(); i++) {
                CopyOnWriteFileRewriter.RewriteResult result = results.get(i);
                RewriteInput input = inputs.get(i);

                CopyOnWriteFileRewriter.RewriteMetrics rewriteMetrics = result.metrics();
                totalBytesRead += rewriteMetrics.originalFileSizeBytes();
                totalBytesWritten += rewriteMetrics.newFileSizeBytes();
                totalRowsDeleted += rewriteMetrics.deletedRowCount();

                List<CommitTaskData.DanglingDeleteFile> danglingDeleteFiles = buildDanglingDeleteFiles(
                        input.originalPath(), input.partitionSpec(), input.partitionData(), input.preExistingDeletes());
                CommitTaskData.RewriteInfo rewriteInfo = new CommitTaskData.RewriteInfo(
                        input.originalPath(),
                        input.sourceFileSizeInBytes(),
                        input.sourceRecordCount(),
                        input.sourceFileFormat(),
                        danglingDeleteFiles);

                if (result.newFile().isPresent()) {
                    DataFile newFile = result.newFile().get();
                    writtenBytes += newFile.fileSizeInBytes();
                    CommitTaskData task = new CommitTaskData(
                            newFile.location(),
                            writeFileFormat,
                            newFile.fileSizeInBytes(),
                            new MetricsWrapper(result.newFileMetrics()),
                            PartitionSpecParser.toJson(input.partitionSpec()),
                            input.partitionData().map(PartitionData::toJson),
                            FileContent.DATA,
                            Optional.empty(),
                            Optional.ofNullable(newFile.splitOffsets()),
                            writeSortOrderId,
                            Optional.empty(),
                            Optional.of(rewriteInfo));
                    fragments.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
                }
                else {
                    filesRemoved++;
                    CommitTaskData task = new CommitTaskData(
                            "",
                            input.sourceFileFormat(),
                            0,
                            new MetricsWrapper(new Metrics(0L)),
                            PartitionSpecParser.toJson(input.partitionSpec()),
                            input.partitionData().map(PartitionData::toJson),
                            FileContent.DATA,
                            Optional.empty(),
                            Optional.empty(),
                            SortOrder.unsorted().orderId(),
                            Optional.empty(),
                            Optional.of(rewriteInfo));
                    fragments.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
                }
            }
        }
        catch (Throwable t) {
            for (RollbackAction rollbackAction : rollbackActions) {
                try {
                    rollbackAction.run();
                }
                catch (Exception ex) {
                    t.addSuppressed(ex);
                }
            }
            throw t instanceof RuntimeException re ? re : new RuntimeException(t);
        }

        if (log.isDebugEnabled()) {
            long rewriteDurationMs = (System.nanoTime() - startNanos) / 1_000_000;
            log.debug(
                    "CoW rewrite on worker: %d files rewritten (%d removed), %d rows deleted, " +
                            "%d bytes read, %d bytes written, %d ms elapsed",
                    results.size(),
                    filesRemoved,
                    totalRowsDeleted,
                    totalBytesRead,
                    totalBytesWritten,
                    rewriteDurationMs);
        }
    }

    private CompletableFuture<Collection<Slice>> finishMergeOnRead()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());
        writtenBytes = insertPageSink.getCompletedBytes();

        if (formatVersion < 2) {
            // position deletes are only supported in Iceberg format v2 and above
            verify(fileDeletions.isEmpty(), "Position deletes are not supported in Iceberg format version %s", formatVersion);
        }
        else if (formatVersion == 2) {
            fileDeletions.forEach((dataFilePath, deletion) -> deletion.rowsToDelete().build().ifPresent(deletionVector -> {
                PositionDeleteWriter writer = createPositionDeleteWriter(
                        dataFilePath.toStringUtf8(),
                        partitionsSpecs.get(deletion.partitionSpecId()),
                        deletion.partitionDataJson());
                fragments.add(writePositionDeletes(writer, deletionVector));
            }));
        }
        else if (formatVersion == 3) {
            fileDeletions.forEach((dataFilePath, deletion) -> deletion.rowsToDelete().build().ifPresent(deletionVector -> {
                PartitionSpec partitionSpec = partitionsSpecs.get(deletion.partitionSpecId());
                Optional<PartitionData> partitionData = createPartitionData(partitionSpec, deletion.partitionDataJson());
                CommitTaskData task = new CommitTaskData(
                        "", // path of the v2 delete file
                        fileFormat,
                        0, // size of the v2 delete file
                        new MetricsWrapper(new Metrics(deletionVector.cardinality())),
                        PartitionSpecParser.toJson(partitionSpec),
                        partitionData.map(PartitionData::toJson),
                        FileContent.POSITION_DELETES,
                        Optional.of(dataFilePath.toStringUtf8()),
                        Optional.empty(), // unused for v3
                        SortOrder.unsorted().orderId(),
                        Optional.of(deletionVector.serialize().getBytes()));
                fragments.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
            }));
        }
        else {
            throw new VerifyException("Unsupported Iceberg format version: " + formatVersion);
        }

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        try {
            insertPageSink.abort();
        }
        finally {
            cleanupInFlightRewrites(inFlightRewritePaths, fileIoFactory.create(fileSystem), copyOnWriteRewriteExecutor);
        }
    }

    /**
     * Bulk-deletes any rewrite output paths that are still tracked in {@code inFlightRewritePaths}
     * and clears the set. Called on abort: the in-flight async tasks cannot be interrupted and may
     * still be producing files when this runs, so any file that lands after our delete is orphaned
     * and recoverable via {@code remove_orphan_files}. Files whose tasks already completed are
     * covered either by the task's {@code finally} removal or the sink's rollback chain.
     */
    static void cleanupInFlightRewrites(Set<String> inFlightRewritePaths, FileIO fileIo, ExecutorService executor)
    {
        if (inFlightRewritePaths.isEmpty()) {
            return;
        }
        List<String> snapshot = List.copyOf(inFlightRewritePaths);
        inFlightRewritePaths.clear();
        IcebergMetadata.deleteOrphanFilesInParallel(fileIo, snapshot, executor);
    }

    private PositionDeleteWriter createPositionDeleteWriter(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson)
    {
        return new PositionDeleteWriter(
                dataFilePath,
                partitionSpec,
                createPartitionData(partitionSpec, partitionDataJson),
                locationProvider,
                fileWriterFactory,
                fileSystem,
                session,
                fileFormat,
                storageProperties);
    }

    private Slice writePositionDeletes(PositionDeleteWriter writer, DeletionVector rowsToDelete)
    {
        try {
            CommitTaskData task = writer.write(rowsToDelete);
            writtenBytes += task.fileSizeInBytes();
            return wrappedBuffer(jsonCodec.toJsonBytes(task));
        }
        catch (Throwable t) {
            closeAllSuppress(t, writer::abort);
            throw t;
        }
    }

    private Optional<PartitionData> createPartitionData(PartitionSpec partitionSpec, String partitionDataAsJson)
    {
        if (!partitionSpec.isPartitioned()) {
            return Optional.empty();
        }

        Type[] columnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                .toArray(Type[]::new);
        return Optional.of(PartitionData.fromJson(partitionDataAsJson, columnTypes));
    }

    private static IcebergFileFormat getWriteFileFormat(Map<String, String> tableProperties)
    {
        return IcebergFileFormat.fromIceberg(FileFormat.fromString(tableProperties.getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT)));
    }

    private static Schema getRewriteSchema(Schema schema, int formatVersion)
    {
        if (formatVersion < 3) {
            return schema;
        }

        List<Types.NestedField> columns = new ArrayList<>(schema.columns());
        if (schema.findField(MetadataColumns.ROW_ID.fieldId()) == null) {
            columns.add(MetadataColumns.ROW_ID);
        }
        if (schema.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()) == null) {
            columns.add(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);
        }
        return new Schema(columns);
    }

    private static List<CommitTaskData.DanglingDeleteFile> buildDanglingDeleteFiles(
            String originalPath,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData,
            List<DeleteFile> preExistingDeletes)
    {
        if (preExistingDeletes.isEmpty()) {
            return List.of();
        }
        List<CommitTaskData.DanglingDeleteFile> danglingDeleteFiles = new ArrayList<>();
        for (DeleteFile deleteFile : preExistingDeletes) {
            // A delete file becomes dangling when the sole data file it references is rewritten.
            // - Deletion vectors (V3) are always 1:1 with a data file.
            // - V2 position delete files may be file-scoped (referencedDataFile set) or
            //   partition-scoped (null). Only file-scoped deletes matching this data file are
            //   safe to remove; partition-scoped deletes may still apply to sibling data files.
            if (deleteFile.isDeletionVector() || deleteFile.isFileScopedPositionDelete(originalPath)) {
                danglingDeleteFiles.add(new CommitTaskData.DanglingDeleteFile(
                        deleteFile.path(),
                        deleteFile.fileSizeInBytes(),
                        deleteFile.recordCount(),
                        PartitionSpecParser.toJson(partitionSpec),
                        partitionData.map(PartitionData::toJson),
                        deleteFile.contentOffset().isPresent() ? deleteFile.contentOffset().getAsLong() : null,
                        deleteFile.contentSizeInBytes().isPresent() ? (long) deleteFile.contentSizeInBytes().get() : null,
                        originalPath));
            }
        }
        return danglingDeleteFiles;
    }

    private Page createInsertionsPageWithRowLineage(Page insertionsPage, Page inputPage)
    {
        Block[] blocks = new Block[columnCount + 2];
        for (int channel = 0; channel < columnCount; channel++) {
            blocks[channel] = insertionsPage.getBlock(channel);
        }
        blocks[columnCount] = createRowIdBlock(inputPage, columnCount, insertionsPage.getPositionCount());
        blocks[columnCount + 1] = createNullLongBlock(insertionsPage.getPositionCount());
        return new Page(insertionsPage.getPositionCount(), blocks);
    }

    private static Block createNullLongBlock(int positionCount)
    {
        boolean[] nulls = new boolean[positionCount];
        Arrays.fill(nulls, true);
        return new LongArrayBlock(positionCount, Optional.of(nulls), new long[positionCount]);
    }

    private static Block createRowIdBlock(Page inputPage, int dataColumnCount, int additionCount)
    {
        // For V3, preserve source_row_id on UPDATE_INSERT rows when it is available.
        // Rows updated from pre-lineage files in upgraded v2->v3 tables legitimately have
        // a null source_row_id, in which case Iceberg assigns a fresh row ID to the new row.
        Block operationBlock = inputPage.getBlock(dataColumnCount);
        Block mergeRowIdBlock = inputPage.getBlock(dataColumnCount + 2);
        List<Block> mergeRowIdFields = RowBlock.getRowFieldsFromBlock(mergeRowIdBlock);
        Block sourceRowIdBlock = mergeRowIdFields.get(4);

        long[] rowIdValues = new long[additionCount];
        long[] rowIdValidity = new long[Bitmap.wordsForBits(additionCount)];
        boolean foundNull = false;

        int additionIndex = 0;
        for (int position = 0; position < inputPage.getPositionCount(); position++) {
            byte operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    verify(additionIndex < additionCount, "INSERT row must be selected as an addition");
                    foundNull = true;
                    additionIndex++;
                }
                case UPDATE_INSERT_OPERATION_NUMBER -> {
                    verify(additionIndex < additionCount, "UPDATE_INSERT row must be selected as an addition");
                    if (sourceRowIdBlock.isNull(position)) {
                        foundNull = true;
                    }
                    else {
                        rowIdValues[additionIndex] = BIGINT.getLong(sourceRowIdBlock, position);
                        Bitmap.set(rowIdValidity, 0, additionIndex);
                    }
                    additionIndex++;
                }
                case DELETE_OPERATION_NUMBER, UPDATE_DELETE_OPERATION_NUMBER -> {
                    // This helper produces source row IDs only for additions (INSERT/UPDATE_INSERT).
                    // DELETE and UPDATE_DELETE rows are consumed by the deletion path, not this additions block.
                }
                case UPDATE_OPERATION_NUMBER -> throw new IllegalArgumentException("UPDATE must be represented as UPDATE_DELETE followed by UPDATE_INSERT in Iceberg");
                default -> throw new IllegalArgumentException("Invalid merge operation: " + operation);
            }
        }
        verify(additionIndex == additionCount, "Additions produced did not match planned additions");

        return new LongArrayBlock(additionCount, foundNull ? Optional.of(rowIdValidity) : Optional.empty(), rowIdValues);
    }

    private static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final IcebergFileFormat fileFormat;
        private final long fileSizeInBytes;
        private final long fileRecordCount;
        private final long dataSequenceNumber;
        private final OptionalLong fileFirstRowId;
        private final DeletionVector.Builder rowsToDelete = DeletionVector.builder();

        public FileDeletion(
                int partitionSpecId,
                String partitionDataJson,
                IcebergFileFormat fileFormat,
                long fileSizeInBytes,
                long fileRecordCount,
                long dataSequenceNumber,
                OptionalLong fileFirstRowId)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
            this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
            this.fileSizeInBytes = fileSizeInBytes;
            this.fileRecordCount = fileRecordCount;
            this.dataSequenceNumber = dataSequenceNumber;
            this.fileFirstRowId = requireNonNull(fileFirstRowId, "fileFirstRowId is null");
        }

        public int partitionSpecId()
        {
            return partitionSpecId;
        }

        public String partitionDataJson()
        {
            return partitionDataJson;
        }

        public IcebergFileFormat fileFormat()
        {
            return fileFormat;
        }

        public long fileSizeInBytes()
        {
            return fileSizeInBytes;
        }

        public long fileRecordCount()
        {
            return fileRecordCount;
        }

        public long dataSequenceNumber()
        {
            return dataSequenceNumber;
        }

        public OptionalLong fileFirstRowId()
        {
            return fileFirstRowId;
        }

        public DeletionVector.Builder rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
