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
package io.trino.plugin.iceberg.delete;

import com.google.common.base.VerifyException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergPageSourceProviderFactory;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.NodeVersion;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.util.ContentFileUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_DATA_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getLocationProvider;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.puffin.StandardBlobTypes.DV_V1;

public class DefaultDeletionVectorWriter
        implements DeletionVectorWriter
{
    private static final String REFERENCED_DATA_FILE_KEY = "referenced-data-file";
    private static final String CARDINALITY_KEY = "cardinality";

    private final IcebergFileSystemFactory fileSystemFactory;
    // It is technically possible to read legacy position delete files directly, but would be annoying to maintain
    private final IcebergPageSourceProviderFactory pageSourceProviderFactory;
    private final IcebergColumnHandle deleteFilePathColumnHandle;
    private final IcebergColumnHandle deleteFilePositionColumnHandle;
    private final String trinoVersion;

    @Inject
    public DefaultDeletionVectorWriter(
            IcebergFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergPageSourceProviderFactory pageSourceProviderFactory,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.pageSourceProviderFactory = requireNonNull(pageSourceProviderFactory, "pageSourceProviderFactory is null");
        this.deleteFilePathColumnHandle = getColumnHandle(DELETE_FILE_PATH, typeManager);
        this.deleteFilePositionColumnHandle = getColumnHandle(DELETE_FILE_POS, typeManager);
        this.trinoVersion = nodeVersion.toString();
    }

    @Override
    public void writeDeletionVectors(
            ConnectorSession session,
            Table icebergTable,
            IcebergTableHandle table,
            List<DeletionVectorInfo> deletionVectorInfos,
            RowDelta rowDelta)
    {
        long snapshotId = table.getSnapshotId().orElseThrow(() -> new TrinoException(ICEBERG_BAD_DATA, "Missing base snapshot id for v3 deletion vector rewrite"));

        // deletion vector info may contain multiple entries for the same data file; merge them here
        Map<String, DeletionVector.Builder> deletionVectorBuilders = deletionVectorInfos.stream().collect(toMap(
                DeletionVectorInfo::dataFilePath,
                info -> DeletionVector.builder().deserialize(info.serializedDeletionVector()),
                DeletionVector.Builder::addAll));
        // Load any existing delete files for the affected data files
        ExistingDeletes existingDeletes = getExistingDeletesByMetadataOnly(icebergTable, snapshotId, deletionVectorBuilders.keySet());

        // merge existing deletion vectors into the new ones
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), icebergTable.io().properties());
        existingDeletes.deletionVectors().forEach((dataFilePath, deleteFile) -> {
            try (TrinoInput input = fileSystem.newInputFile(Location.of(deleteFile.location()), deleteFile.fileSizeInBytes()).newInput()) {
                Slice data = input.readFully(deleteFile.contentOffset(), toIntExact(deleteFile.contentSizeInBytes()));
                deletionVectorBuilders.get(dataFilePath).deserialize(data);
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_BAD_DATA, "Failed to read existing deletion vector file: " + deleteFile.location(), e);
            }
        });

        // merge existing legacy position delete files into the new DVs
        if (!existingDeletes.fileScopedDeletes().isEmpty() || !existingDeletes.partitionScopedDeletes().isEmpty()) {
            // determine which of the new deletion vectors need merging
            Map<String, DeletionVector.Builder> deletionVectorsWithLegacyDelete = deletionVectorBuilders.entrySet().stream()
                    .filter(entry -> existingDeletes.fileScopedDeletes().containsKey(entry.getKey()) || !existingDeletes.partitionScopedDeletes().isEmpty())
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!deletionVectorsWithLegacyDelete.isEmpty()) {
                // process the file-scoped delete files
                deletionVectorsWithLegacyDelete.forEach((dataFilePath, deletionVector) -> {
                    Collection<DeleteFile> deleteFiles = existingDeletes.fileScopedDeletes().get(dataFilePath);
                    for (DeleteFile deleteFile : deleteFiles) {
                        try (ConnectorPageSource connectorPageSource = openDeleteFilePageSource(session, deleteFile, fileSystem)) {
                            PositionDeleteReader.readSingleFilePositionDeletes(connectorPageSource, deletionVector::add);
                        }
                        catch (IOException e) {
                            throw new TrinoException(ICEBERG_BAD_DATA, "Failed to read position delete file: " + deleteFile.location(), e);
                        }
                    }
                });

                // process the partition-scoped delete files
                for (DeleteFile deleteFile : existingDeletes.partitionScopedDeletes()) {
                    try (ConnectorPageSource connectorPageSource = openDeleteFilePageSource(session, deleteFile, fileSystem)) {
                        PositionDeleteReader.readMultiFilePositionDeletes(connectorPageSource, (dataFilePath, position) -> {
                            DeletionVector.Builder deletionVector = deletionVectorsWithLegacyDelete.get(dataFilePath);
                            if (deletionVector != null) {
                                deletionVector.add(position);
                            }
                        });
                    }
                    catch (IOException e) {
                        throw new TrinoException(ICEBERG_BAD_DATA, "Failed to read position delete file: " + deleteFile.location(), e);
                    }
                }
            }
        }

        // finalize the deletion vectors
        Map<String, DeletionVector> deletionVectors = deletionVectorBuilders.entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), entry.getValue().build()))
                .collect(toMap(
                        Map.Entry::getKey,
                        // at this point there should not be an empty deletion vector
                        entry -> entry.getValue().orElseThrow(() -> new VerifyException("Delection vector is empty"))));

        // Write a single Puffin file containing all DVs; get blob offsets/lengths back.
        LocationProvider locationProvider = getLocationProvider(table.getSchemaTableName(), table.getTableLocation(), table.getStorageProperties());

        // write deletion vectors to a puffin file and delete files to the row delta
        writeDeletionVectorsPuffin(session, icebergTable, locationProvider, deletionVectorInfos, deletionVectors, trinoVersion)
                .forEach(rowDelta::addDeletes);

        // remove existing DVs and file-scoped position deletes
        existingDeletes.deletionVectors().values().forEach(rowDelta::removeDeletes);
        existingDeletes.fileScopedDeletes().values().forEach(rowDelta::removeDeletes);
    }

    private static ExistingDeletes getExistingDeletesByMetadataOnly(Table table, long snapshotId, Set<String> dataFilePaths)
    {
        Map<String, DeleteFile> deletionVectors = new HashMap<>();
        Multimap<String, DeleteFile> fileScopedDeletes = ArrayListMultimap.create();
        List<DeleteFile> partitionScopedDeletes = new ArrayList<>();

        FileIO io = table.io();
        Map<Integer, PartitionSpec> specsById = table.specs();
        for (ManifestFile manifest : table.snapshot(snapshotId).deleteManifests(io)) {
            try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
                for (DeleteFile deleteFile : reader) {
                    if (deleteFile.content() != FileContent.POSITION_DELETES) {
                        continue;
                    }

                    String referenced = deleteFile.referencedDataFile();
                    if (referenced == null) {
                        partitionScopedDeletes.add(deleteFile);
                    }
                    else if (dataFilePaths.contains(referenced)) {
                        // If there's a DV for a data file, legacy delete files are ignored.
                        if (isDeletionVector(deleteFile)) {
                            // multiple DVs for one data file is not allowed
                            if (deletionVectors.put(referenced, deleteFile) != null) {
                                throw new VerifyException("Multiple deletion vectors found for data file: " + referenced);
                            }
                            fileScopedDeletes.removeAll(referenced);
                        }
                        else if (!deletionVectors.containsKey(referenced)) {
                            fileScopedDeletes.put(referenced, deleteFile);
                        }
                    }
                }
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_BAD_DATA, "Failed to read delete manifest: " + manifest.path(), e);
            }
        }
        return new ExistingDeletes(deletionVectors, fileScopedDeletes, partitionScopedDeletes);
    }

    private record ExistingDeletes(
            Map<String, DeleteFile> deletionVectors,
            Multimap<String, DeleteFile> fileScopedDeletes,
            List<DeleteFile> partitionScopedDeletes) {}

    /// Writes one Puffin file containing one DV blob per data file, and returns the corresponding DeleteFile entries.
    private static List<DeleteFile> writeDeletionVectorsPuffin(
            ConnectorSession session,
            Table icebergTable,
            LocationProvider locationProvider,
            List<DeletionVectorInfo> deletionVectorInfos,
            Map<String, DeletionVector> deletionVectors,
            String trinoVersion)
    {
        if (deletionVectors.isEmpty()) {
            return List.of();
        }

        String fileName = "dv-" + session.getQueryId() + "-" + UUID.randomUUID() + ".puffin";
        String puffinPath = locationProvider.newDataLocation(fileName);

        FileIO fileIO = icebergTable.io();
        OutputFile outputFile = fileIO.newOutputFile(puffinPath);
        try {
            try (PuffinWriter writer = Puffin.write(outputFile).createdBy("Trino version " + trinoVersion).build()) {
                deletionVectors.forEach((referencedDataFile, deletionVector) ->
                        writer.add(createDeletionVectorBlob(referencedDataFile, deletionVector)));

                writer.finish();

                Map<String, DeletionVectorInfo> partitionInfo = deletionVectorInfos.stream()
                        .collect(toMap(
                                DeletionVectorInfo::dataFilePath,
                                Function.identity(),
                                (left, right) -> {
                                    verify(left.partitionSpec().equals(right.partitionSpec()), "Mismatched partition specs for data file: %s", left.dataFilePath());
                                    return left;
                                }));

                List<DeleteFile> deleteFiles = new ArrayList<>(deletionVectors.size());
                for (BlobMetadata meta : writer.writtenBlobsMetadata()) {
                    verify(DV_V1.equals(meta.type()), "Unexpected blob type written to deletion vector puffin file: %s", meta.type());

                    String referencedDataFile = meta.properties().get(REFERENCED_DATA_FILE_KEY);
                    verify(referencedDataFile != null, "DV blob missing '%s' property", REFERENCED_DATA_FILE_KEY);

                    DeletionVectorInfo deletionVectorInfo = partitionInfo.get(referencedDataFile);
                    verify(deletionVectorInfo != null, "No DeletionVectorInfo found for data file: %s", referencedDataFile);

                    long cardinality = Long.parseLong(meta.properties().get(CARDINALITY_KEY));

                    FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(deletionVectorInfo.partitionSpec())
                            .withPath(puffinPath)
                            .withFormat(PUFFIN)
                            .ofPositionDeletes()
                            .withFileSizeInBytes(writer.fileSize())
                            .withReferencedDataFile(referencedDataFile)
                            .withContentOffset(meta.offset())
                            .withContentSizeInBytes(meta.length())
                            .withRecordCount(cardinality);
                    deletionVectorInfo.partitionData().ifPresent(deleteBuilder::withPartition);
                    deleteFiles.add(deleteBuilder.build());
                }

                verify(deleteFiles.size() == deletionVectors.size());
                return deleteFiles;
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_WRITER_DATA_ERROR, "Failed to write deletion vectors puffin file: " + puffinPath, e);
            }
        }
        catch (Throwable t) {
            // Best-effort cleanup if we created the puffin file but failed before commit.
            Closables.closeAllSuppress(t, () -> fileIO.deleteFile(puffinPath));
            throw t;
        }
    }

    private ConnectorPageSource openDeleteFilePageSource(ConnectorSession session, DeleteFile deleteFile, TrinoFileSystem fileSystem)
    {
        return pageSourceProviderFactory.createPageSourceProvider().openDeleteFile(
                session,
                fileSystem,
                io.trino.plugin.iceberg.delete.DeleteFile.fromIceberg(deleteFile),
                List.of(deleteFilePathColumnHandle, deleteFilePositionColumnHandle),
                TupleDomain.all());
    }

    private static boolean isDeletionVector(DeleteFile deleteFile)
    {
        if (!ContentFileUtil.isDV(deleteFile)) {
            return false;
        }
        if (deleteFile.content() == FileContent.POSITION_DELETES
                && deleteFile.referencedDataFile() != null
                && deleteFile.contentOffset() != null
                && deleteFile.contentSizeInBytes() != null) {
            return true;
        }
        throw new TrinoException(ICEBERG_BAD_DATA, "Invalid deletion vector delete file: " + deleteFile.location());
    }

    private static Blob createDeletionVectorBlob(String referencedDataFile, DeletionVector deletionVector)
    {
        return new Blob(
                DV_V1,
                List.of(),
                -1L,
                -1L,
                deletionVector.serialize().toByteBuffer(),
                null,
                Map.of(REFERENCED_DATA_FILE_KEY, referencedDataFile, CARDINALITY_KEY, Long.toString(deletionVector.cardinality())));
    }
}
