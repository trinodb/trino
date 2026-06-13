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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.RollbackAction;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.plugin.iceberg.delete.DeletionVector;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMapping;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class CopyOnWriteFileRewriter
{
    private static final Logger log = Logger.get(CopyOnWriteFileRewriter.class);
    private final IcebergPageSourceProviderFactory pageSourceProviderFactory;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;

    @Inject
    public CopyOnWriteFileRewriter(
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            IcebergFileWriterFactory fileWriterFactory,
            IcebergFileSystemFactory fileSystemFactory,
            TypeManager typeManager)
    {
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /**
     * Computes the destination path for the rewritten file. Path is deterministic given the
     * session's query id plus a random UUID and must be computed before the writer is created
     * so the orchestrator can register it for abort-time cleanup even if the async task has not
     * yet started.
     */
    public static String computeNewFilePath(
            ConnectorSession session,
            IcebergFileFormat writeFileFormat,
            LocationProvider locationProvider,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData)
    {
        String fileName = writeFileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
        return partitionData
                .map(partition -> locationProvider.newDataLocation(partitionSpec, partition, fileName))
                .orElseGet(() -> locationProvider.newDataLocation(fileName));
    }

    public record RewriteMetrics(
            long originalRecordCount,
            long newRecordCount,
            long deletedRowCount,
            long originalFileSizeBytes,
            long newFileSizeBytes,
            long rewriteDurationMs) {}

    public record RewriteResult(
            Optional<DataFile> newFile,
            RollbackAction rollbackAction,
            RewriteMetrics metrics,
            Metrics newFileMetrics) {}

    /**
     * Reads {@code originalPath}, filters rows marked in {@code deletionVector},
     * and writes surviving rows to a new data file.
     *
     * @return a {@link RewriteResult} containing the new DataFile (empty if
     *         all rows were deleted), metrics, and a rollback action that
     *         deletes the newly written file on failure
     */
    public RewriteResult rewriteFile(
            ConnectorSession session,
            String originalPath,
            String newFilePath,
            long originalFileSize,
            DeletionVector deletionVector,
            Schema schema,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData,
            List<DeleteFile> preExistingDeletes,
            long sourceDataSequenceNumber,
            OptionalLong sourceFileFirstRowId,
            IcebergFileFormat sourceFileFormat,
            IcebergFileFormat writeFileFormat,
            MetricsConfig metricsConfig,
            Map<String, String> fileIoProperties,
            Map<String, String> tableProperties,
            Optional<NameMapping> nameMapping)
    {
        long startTimeMs = System.currentTimeMillis();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), requireNonNull(fileIoProperties, "fileIoProperties is null"));

        IcebergFileWriter writer = fileWriterFactory.createDataFileWriter(
                fileSystem,
                Location.of(newFilePath),
                schema,
                session,
                writeFileFormat,
                metricsConfig,
                tableProperties);

        long originalRecordCount = readFilterWrite(
                session,
                writer,
                originalPath,
                originalFileSize,
                deletionVector,
                schema,
                partitionSpec,
                partitionData,
                preExistingDeletes,
                sourceDataSequenceNumber,
                sourceFileFirstRowId,
                sourceFileFormat,
                fileIoProperties,
                nameMapping);

        return finalizeRewrite(
                writer,
                originalPath,
                newFilePath,
                partitionSpec,
                partitionData,
                writeFileFormat,
                originalRecordCount,
                originalFileSize,
                startTimeMs);
    }

    /**
     * Commits the writer and assembles the {@link RewriteResult}. Ensures no orphan files are
     * left on disk on any failure:
     * <ul>
     *   <li>If {@code writer.commit()} throws, {@code writer.rollback()} is invoked to remove the
     *       partially written file before rethrowing.</li>
     *   <li>If any post-commit step throws, the rollback {@link RollbackAction} is invoked to delete
     *       the committed file before rethrowing.</li>
     *   <li>If the output has zero rows, the rollback is invoked eagerly; a cleanup failure is
     *       surfaced as {@link UncheckedIOException} so the caller aborts the CoW commit.</li>
     * </ul>
     */
    static RewriteResult finalizeRewrite(
            IcebergFileWriter writer,
            String originalPath,
            String newFilePath,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData,
            IcebergFileFormat writeFileFormat,
            long originalRecordCount,
            long originalFileSize,
            long startTimeMs)
    {
        RollbackAction rollbackAction;
        try {
            rollbackAction = writer.commit();
        }
        catch (RuntimeException | Error e) {
            rollbackQuietly(writer, e);
            throw e;
        }

        try {
            IcebergFileWriter.FileMetrics fileMetrics = writer.getFileMetrics();
            Metrics fullMetrics = fileMetrics.metrics();
            long newRecordCount = fullMetrics.recordCount();
            long newFileSize = writer.getWrittenBytes();
            Optional<DataFile> newFile;
            if (newRecordCount > 0) {
                DataFiles.Builder newBuilder = DataFiles.builder(partitionSpec)
                        .withPath(newFilePath)
                        .withFormat(writeFileFormat.toIceberg())
                        .withFileSizeInBytes(newFileSize)
                        .withMetrics(fullMetrics);
                fileMetrics.splitOffsets().ifPresent(newBuilder::withSplitOffsets);
                partitionData.ifPresent(newBuilder::withPartition);
                newFile = Optional.of(newBuilder.build());
            }
            else {
                // All rows deleted: eagerly remove the empty output. A cleanup failure is fatal
                // for the whole CoW commit; orphan recovery is left to table orphan-file maintenance.
                try {
                    rollbackAction.run();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to delete empty CoW rewrite output file: " + newFilePath, e);
                }
                rollbackAction = () -> {};
                newFile = Optional.empty();
            }

            long durationMs = System.currentTimeMillis() - startTimeMs;
            long deletedRowCount = originalRecordCount - newRecordCount;
            RewriteMetrics metrics = new RewriteMetrics(
                    originalRecordCount,
                    newRecordCount,
                    deletedRowCount,
                    originalFileSize,
                    newFileSize,
                    durationMs);

            log.debug(
                    "CoW rewrite: %s -> %s (%d rows -> %d rows, %d deleted, %d bytes -> %d bytes, %d ms)",
                    originalPath,
                    newFile.isPresent() ? newFilePath : "<removed>",
                    originalRecordCount,
                    newRecordCount,
                    deletedRowCount,
                    originalFileSize,
                    newFileSize,
                    durationMs);

            return new RewriteResult(newFile, rollbackAction, metrics, fullMetrics);
        }
        catch (RuntimeException | Error e) {
            try {
                rollbackAction.run();
            }
            catch (IOException ioException) {
                e.addSuppressed(ioException);
            }
            throw e;
        }
    }

    private long readFilterWrite(
            ConnectorSession session,
            IcebergFileWriter writer,
            String originalPath,
            long originalFileSize,
            DeletionVector deletionVector,
            Schema schema,
            PartitionSpec partitionSpec,
            Optional<PartitionData> partitionData,
            List<DeleteFile> preExistingDeletes,
            long sourceDataSequenceNumber,
            OptionalLong sourceFileFirstRowId,
            IcebergFileFormat fileFormat,
            Map<String, String> fileIoProperties,
            Optional<NameMapping> nameMapping)
    {
        List<IcebergColumnHandle> dataColumns = schema.columns().stream()
                .map(column -> getColumnHandle(column, typeManager))
                .collect(toImmutableList());
        List<IcebergColumnHandle> readColumns = new ArrayList<>(dataColumns);
        readColumns.add(getColumnHandle(MetadataColumns.ROW_POSITION, typeManager));

        IcebergPageSourceProvider pageSourceProvider = pageSourceProviderFactory.createPageSourceProvider();

        long originalRecordCount = 0;
        try (ConnectorPageSource source = pageSourceProvider.createPageSource(
                session,
                readColumns,
                schema,
                partitionSpec,
                partitionData.orElseGet(() -> new PartitionData(new Object[0])),
                preExistingDeletes,
                DynamicFilter.EMPTY,
                TupleDomain.all(),
                TupleDomain.all(),
                originalPath,
                0,
                originalFileSize,
                originalFileSize,
                0,
                fileFormat,
                new IcebergTableCredentials(fileIoProperties, ImmutableList.of()),
                OptionalLong.of(sourceDataSequenceNumber),
                sourceFileFirstRowId,
                nameMapping,
                newSimpleAggregatedMemoryContext())) {
            int dataColumnCount = dataColumns.size();
            int[] dataChannels = new int[dataColumnCount];
            for (int channel = 0; channel < dataColumnCount; channel++) {
                dataChannels[channel] = channel;
            }
            while (!source.isFinished()) {
                SourcePage sourcePage = source.getNextSourcePage();
                if (sourcePage == null) {
                    continue;
                }
                Page page = sourcePage.getPage();
                Page dataPage = page.getColumns(dataChannels);
                int positionCount = dataPage.getPositionCount();
                originalRecordCount += positionCount;
                Block rowPositionBlock = page.getBlock(dataColumnCount);

                int[] kept = new int[positionCount];
                int keptCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    long rowPosition = BIGINT.getLong(rowPositionBlock, i);
                    if (!deletionVector.isRowDeleted(rowPosition)) {
                        kept[keptCount++] = i;
                    }
                }

                if (keptCount > 0) {
                    writer.appendRows(dataPage.getPositions(kept, 0, keptCount));
                }
            }
        }
        catch (IOException | RuntimeException e) {
            rollbackQuietly(writer, e);
            if (e instanceof IOException ioException) {
                throw new UncheckedIOException("Failed to rewrite data file: " + originalPath, ioException);
            }
            throw (RuntimeException) e;
        }
        catch (Error e) {
            rollbackQuietly(writer, e);
            throw e;
        }
        return originalRecordCount;
    }

    private static void rollbackQuietly(IcebergFileWriter writer, Throwable primary)
    {
        try {
            writer.rollback();
        }
        catch (RuntimeException rollbackFailure) {
            primary.addSuppressed(rollbackFailure);
        }
    }
}
