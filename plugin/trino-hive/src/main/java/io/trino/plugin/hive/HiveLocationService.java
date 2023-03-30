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
package io.trino.plugin.hive;

import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.trino.plugin.hive.HiveSessionProperties.isTemporaryStagingDirectoryEnabled;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static io.trino.plugin.hive.util.AcidTables.isTransactionalTable;
import static io.trino.plugin.hive.util.HiveWriteUtils.createTemporaryPath;
import static io.trino.plugin.hive.util.HiveWriteUtils.getTableDefaultLocation;
import static io.trino.plugin.hive.util.HiveWriteUtils.isHdfsEncrypted;
import static io.trino.plugin.hive.util.HiveWriteUtils.isS3FileSystem;
import static io.trino.plugin.hive.util.HiveWriteUtils.pathExists;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveLocationService
        implements LocationService
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HiveLocationService(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Path forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName)
    {
        HdfsContext context = new HdfsContext(session);
        Path targetPath = getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for table
        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }
        return targetPath;
    }

    @Override
    public LocationHandle forNewTableAsSelect(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName, Optional<Path> externalLocation)
    {
        HdfsContext context = new HdfsContext(session);
        Path targetPath = externalLocation.orElseGet(() -> getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName));

        // verify the target directory for the table
        if (pathExists(context, hdfsEnvironment, targetPath)) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        // TODO detect when existing table's location is a on a different file system than the temporary directory
        if (shouldUseTemporaryDirectory(session, context, targetPath, externalLocation)) {
            Path writePath = createTemporaryPath(session, context, hdfsEnvironment, targetPath);
            return new LocationHandle(targetPath, writePath, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_NEW_DIRECTORY);
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        HdfsContext context = new HdfsContext(session);
        Path targetPath = new Path(table.getStorage().getLocation());

        if (shouldUseTemporaryDirectory(session, context, targetPath, Optional.empty()) && !isTransactionalTable(table.getParameters())) {
            Path writePath = createTemporaryPath(session, context, hdfsEnvironment, targetPath);
            return new LocationHandle(targetPath, writePath, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
    }

    @Override
    public LocationHandle forOptimize(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        // For OPTIMIZE write result files directly to table directory; that is needed by the commit logic in HiveMetadata#finishTableExecute
        Path targetPath = new Path(table.getStorage().getLocation());
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
    }

    private boolean shouldUseTemporaryDirectory(ConnectorSession session, HdfsContext context, Path path, Optional<Path> externalLocation)
    {
        return isTemporaryStagingDirectoryEnabled(session)
                // skip using temporary directory for S3
                && !isS3FileSystem(context, hdfsEnvironment, path)
                // skip using temporary directory if destination is encrypted; it's not possible to move a file between encryption zones
                && !isHdfsEncrypted(context, hdfsEnvironment, path)
                // Skip using temporary directory if destination is external. Target may be on a different file system.
                && externalLocation.isEmpty();
    }

    @Override
    public WriteInfo getQueryWriteInfo(LocationHandle locationHandle)
    {
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getTableWriteInfo(LocationHandle locationHandle, boolean overwrite)
    {
        if (overwrite && locationHandle.getWriteMode() != STAGE_AND_MOVE_TO_TARGET_DIRECTORY) {
            throw new TrinoException(NOT_SUPPORTED, "Overwriting unpartitioned table not supported when writing directly to target directory");
        }
        return new WriteInfo(locationHandle.getTargetPath(), locationHandle.getWritePath(), locationHandle.getWriteMode());
    }

    @Override
    public WriteInfo getPartitionWriteInfo(LocationHandle locationHandle, Optional<Partition> partition, String partitionName)
    {
        if (partition.isPresent()) {
            // existing partition
            WriteMode writeMode = locationHandle.getWriteMode();
            Path targetPath = new Path(partition.get().getStorage().getLocation());
            Path writePath = getPartitionWritePath(locationHandle, partitionName, writeMode, targetPath);
            return new WriteInfo(targetPath, writePath, writeMode);
        }
        // new partition
        return new WriteInfo(
                new Path(locationHandle.getTargetPath(), partitionName),
                new Path(locationHandle.getWritePath(), partitionName),
                locationHandle.getWriteMode());
    }

    private Path getPartitionWritePath(LocationHandle locationHandle, String partitionName, WriteMode writeMode, Path targetPath)
    {
        switch (writeMode) {
            case STAGE_AND_MOVE_TO_TARGET_DIRECTORY:
                return new Path(locationHandle.getWritePath(), partitionName);
            case DIRECT_TO_TARGET_EXISTING_DIRECTORY:
                return targetPath;
            case DIRECT_TO_TARGET_NEW_DIRECTORY:
                throw new UnsupportedOperationException(format("inserting into existing partition is not supported for %s", writeMode));
        }
        throw new UnsupportedOperationException("Unexpected write mode: " + writeMode);
    }
}
