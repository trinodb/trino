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

import io.airlift.log.Logger;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.PartitionStatisticsReader.MAXIMUM_CACHE_SIZE;
import static io.trino.plugin.iceberg.PartitionStatisticsReader.findPartitionStatisticsFile;
import static java.util.Objects.requireNonNull;

/**
 * Commits the partition statistics file of the snapshot a write adds together with the write, when the parent snapshot
 * has one, as extended statistics are updated on write. The file is computed from the metadata of each commit attempt:
 * a retry after a concurrent commit re-applies the write on the new parent, and continues from the file of that parent.
 * The statistics are advisory: when they cannot be computed, the write is committed without them.
 */
final class PartitionStatisticsUpdatingTableOperations
        implements TableOperations
{
    private static final Logger log = Logger.get(PartitionStatisticsUpdatingTableOperations.class);

    private final TableOperations delegate;
    private final String tableName;

    public PartitionStatisticsUpdatingTableOperations(TableOperations delegate, String tableName)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata)
    {
        Optional<PartitionStatisticsFile> file = writePartitionStatisticsFile(base, metadata);
        if (file.isEmpty()) {
            delegate.commit(base, metadata);
            return;
        }
        try {
            delegate.commit(base, TableMetadata.buildFrom(metadata)
                    .setPartitionStatistics(file.get())
                    .build());
        }
        catch (CommitFailedException e) {
            // Nothing was committed, and a retry computes the file again. After a commit whose state is unknown, the file
            // is kept: it may be referenced, and is an orphan otherwise, like an extended statistics file, which
            // remove_orphan_files deletes.
            deleteFile(file.get().path());
            throw e;
        }
    }

    private Optional<PartitionStatisticsFile> writePartitionStatisticsFile(TableMetadata base, TableMetadata metadata)
    {
        if (base == null) {
            log.debug("Not updating partition statistics of %s: the table is created by the commit", tableName);
            return Optional.empty();
        }
        try {
            Snapshot snapshot = metadata.currentSnapshot();
            Snapshot parent = base.currentSnapshot();
            // only a commit that adds a snapshot on top of the current one, not one that changes metadata only
            if (snapshot == null || parent == null || !Objects.equals(snapshot.parentId(), parent.snapshotId())) {
                return Optional.empty();
            }
            BaseTable table = new BaseTable(new UncommittedTableOperations(metadata, delegate), tableName);
            Optional<PartitionStatisticsFile> parentFile = findPartitionStatisticsFile(table, parent.snapshotId());
            if (parentFile.isEmpty()) {
                return Optional.empty();
            }
            // the library writes partition statistics files in the table's file format, and has no ORC implementation
            if (getFileFormat(table) == ORC) {
                log.debug("Not updating partition statistics of %s: tables with the ORC file format are not supported", tableName);
                return Optional.empty();
            }
            // The library computes the file from all manifests when it cannot read the file of the parent, which must not
            // happen inside the commit of a write: skip a missing file. A file larger than the reader's cache is never used
            // for row counts, as its rows take more memory than the file, so it is not worth keeping current either. A
            // corrupt file is not detected here, and is computed from all manifests once (InvalidStatsFileException).
            String parentPath = parentFile.get().path();
            if (parentFile.get().fileSizeInBytes() > MAXIMUM_CACHE_SIZE.toBytes()) {
                log.warn("Not updating partition statistics of %s: the file of the parent snapshot is larger than %s: %s", tableName, MAXIMUM_CACHE_SIZE, parentPath);
                return Optional.empty();
            }
            if (!delegate.io().newInputFile(parentPath).exists()) {
                log.warn("Not updating partition statistics of %s: the file of the parent snapshot does not exist: %s", tableName, parentPath);
                return Optional.empty();
            }
            return Optional.ofNullable(PartitionStatsHandler.computeAndWriteStatsFile(table, snapshot.snapshotId()));
        }
        catch (IOException | RuntimeException | LinkageError e) {
            // LinkageError: a class path problem in the library's file format code must not fail the write, as in the reader
            log.warn(e, "Failed to update partition statistics of %s", tableName);
            return Optional.empty();
        }
    }

    private void deleteFile(String path)
    {
        try {
            delegate.io().deleteFile(path);
        }
        catch (RuntimeException e) {
            log.debug(e, "Failed to delete partition statistics file %s", path);
        }
    }

    @Override
    public TableMetadata current()
    {
        return delegate.current();
    }

    @Override
    public TableMetadata refresh()
    {
        return delegate.refresh();
    }

    @Override
    public FileIO io()
    {
        return delegate.io();
    }

    @Override
    public EncryptionManager encryption()
    {
        return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName)
    {
        return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider()
    {
        return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata)
    {
        return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId()
    {
        return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup()
    {
        return delegate.requireStrictCleanup();
    }

    // The metadata of a commit attempt, for the library to compute the partition statistics file from. Unlike the table of
    // a transaction, it supports the partition statistics scan the incremental computation reads the parent's file with.
    private record UncommittedTableOperations(TableMetadata metadata, TableOperations delegate)
            implements TableOperations
    {
        @Override
        public TableMetadata current()
        {
            return metadata;
        }

        @Override
        public TableMetadata refresh()
        {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata)
        {
            throw new UnsupportedOperationException("Read-only table operations");
        }

        @Override
        public FileIO io()
        {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption()
        {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName)
        {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider()
        {
            return delegate.locationProvider();
        }
    }
}
