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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergUtil.fileName;
import static io.trino.plugin.iceberg.IcebergUtil.loadAllManifestsFromManifestList;
import static io.trino.plugin.iceberg.IcebergUtil.loadAllManifestsFromSnapshot;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;

public final class RemoveOrphanFiles
{
    private static final Logger log = Logger.get(RemoveOrphanFiles.class);
    private static final int DELETE_BATCH_SIZE = 1000;

    private RemoveOrphanFiles() {}

    public static Map<String, Long> removeOrphanFiles(
            Table table,
            TrinoFileSystem fileSystem,
            ExecutorService icebergScanExecutor,
            ExecutorService icebergFileDeleteExecutor,
            SchemaTableName schemaTableName,
            Instant expiration)
    {
        Set<String> processedManifestFilePaths = new HashSet<>();
        // Similarly to issues like https://github.com/trinodb/trino/issues/13759, equivalent paths may have different String
        // representations due to things like double slashes. Using file names may result in retaining files which could be removed.
        // However, in practice Iceberg metadata and data files have UUIDs in their names which makes this unlikely.
        Set<String> validFileNames = Sets.newConcurrentHashSet();
        List<Future<?>> manifestScanFutures = new ArrayList<>();

        for (Snapshot snapshot : table.snapshots()) {
            String manifestListLocation = snapshot.manifestListLocation();
            List<ManifestFile> allManifests;
            if (manifestListLocation != null) {
                validFileNames.add(fileName(manifestListLocation));
                allManifests = loadAllManifestsFromManifestList(table, manifestListLocation);
            }
            else {
                // This is to maintain support for V1 tables which have embedded manifest lists
                allManifests = loadAllManifestsFromSnapshot(table, snapshot);
            }

            for (ManifestFile manifest : allManifests) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validFileNames.add(fileName(manifest.path()));
                manifestScanFutures.add(icebergScanExecutor.submit(() -> {
                    try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, table)) {
                        for (ContentFile<?> contentFile : manifestReader.select(ImmutableList.of("file_path"))) {
                            validFileNames.add(fileName(contentFile.location()));
                        }
                    }
                    catch (IOException | UncheckedIOException e) {
                        throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                    }
                    catch (NotFoundException e) {
                        throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path());
                    }
                }));
            }
        }

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        validFileNames.add("version-hint.text");

        try {
            manifestScanFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            manifestScanFutures.clear();
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            manifestScanFutures.forEach(future -> future.cancel(true));
        }
        ScanAndDeleteResult result = scanAndDeleteInvalidFiles(table, fileSystem, icebergFileDeleteExecutor, schemaTableName, expiration, validFileNames);
        log.info("remove_orphan_files for table %s processed %d manifest files, found %d active files, scanned %d files, deleted %d files (%d bytes)",
                schemaTableName,
                processedManifestFilePaths.size(),
                validFileNames.size() - 1, // excluding version-hint.text
                result.scannedFilesCount(),
                result.deletedFilesCount(),
                result.deletedBytes());
        return ImmutableMap.of(
                "processed_manifests_count", (long) processedManifestFilePaths.size(),
                "active_files_count", (long) validFileNames.size() - 1, // excluding version-hint.text
                "scanned_files_count", result.scannedFilesCount(),
                "deleted_files_count", result.deletedFilesCount(),
                "deleted_bytes", result.deletedBytes());
    }

    private static ScanAndDeleteResult scanAndDeleteInvalidFiles(
            Table table,
            TrinoFileSystem fileSystem,
            ExecutorService icebergFileDeleteExecutor,
            SchemaTableName schemaTableName,
            Instant expiration,
            Set<String> validFiles)
    {
        List<Future<?>> deleteFutures = new ArrayList<>();
        long scannedFilesCount = 0;
        long deletedFilesCount = 0;
        long deletedBytes = 0;
        try {
            List<Location> filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
            FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()));
            while (allFiles.hasNext()) {
                FileEntry entry = allFiles.next();
                scannedFilesCount++;
                if (entry.lastModified().isBefore(expiration) && !validFiles.contains(entry.location().fileName())) {
                    filesToDelete.add(entry.location());
                    deletedFilesCount++;
                    deletedBytes += entry.length();
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        List<Location> finalFilesToDelete = filesToDelete;
                        deleteFutures.add(icebergFileDeleteExecutor.submit(() -> deleteFiles(finalFilesToDelete, schemaTableName, fileSystem)));
                        filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
                    }
                }
                else {
                    log.debug("%s file retained while removing orphan files %s", entry.location(), schemaTableName.getTableName());
                }
            }
            if (!filesToDelete.isEmpty()) {
                log.debug("Deleting files while removing orphan files for table %s %s", schemaTableName, filesToDelete);
                fileSystem.deleteFiles(filesToDelete);
            }

            deleteFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            deleteFutures.clear();
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            deleteFutures.forEach(future -> future.cancel(true));
        }
        return new ScanAndDeleteResult(scannedFilesCount, deletedFilesCount, deletedBytes);
    }

    private static void deleteFiles(List<Location> files, SchemaTableName schemaTableName, TrinoFileSystem fileSystem)
    {
        log.debug("Deleting files while removing orphan files for table %s [%s]", schemaTableName, files);
        try {
            fileSystem.deleteFiles(files);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
    }

    private record ScanAndDeleteResult(long scannedFilesCount, long deletedFilesCount, long deletedBytes) {}
}
