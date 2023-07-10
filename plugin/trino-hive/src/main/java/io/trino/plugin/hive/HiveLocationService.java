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

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import java.util.Optional;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
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
    private final boolean temporaryStagingDirectoryEnabled;
    private final String temporaryStagingDirectoryPath;

    @Inject
    public HiveLocationService(HdfsEnvironment hdfsEnvironment, HiveConfig hiveConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.temporaryStagingDirectoryEnabled = hiveConfig.isTemporaryStagingDirectoryEnabled();
        this.temporaryStagingDirectoryPath = hiveConfig.getTemporaryStagingDirectoryPath();
    }

    @Override
    public Location forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName)
    {
        HdfsContext context = new HdfsContext(session);
        Location targetPath = getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName);

        // verify the target directory for table
        if (pathExists(context, hdfsEnvironment, new Path(targetPath.toString()))) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }
        return targetPath;
    }

    @Override
    public LocationHandle forNewTableAsSelect(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName, Optional<Location> externalLocation)
    {
        HdfsContext context = new HdfsContext(session);
        Location targetPath = externalLocation.orElseGet(() -> getTableDefaultLocation(context, metastore, hdfsEnvironment, schemaName, tableName));

        // verify the target directory for the table
        if (pathExists(context, hdfsEnvironment, new Path(targetPath.toString()))) {
            throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for table '%s.%s' already exists: %s", schemaName, tableName, targetPath));
        }

        // TODO detect when existing table's location is a on a different file system than the temporary directory
        if (shouldUseTemporaryDirectory(context, new Path(targetPath.toString()), externalLocation.isPresent())) {
            Location writePath = createTemporaryPath(context, hdfsEnvironment, new Path(targetPath.toString()), temporaryStagingDirectoryPath);
            return new LocationHandle(targetPath, writePath, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_NEW_DIRECTORY);
    }

    @Override
    public LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        HdfsContext context = new HdfsContext(session);
        Location targetPath = Location.of(table.getStorage().getLocation());

        if (shouldUseTemporaryDirectory(context, new Path(targetPath.toString()), false) && !isTransactionalTable(table.getParameters())) {
            Location writePath = createTemporaryPath(context, hdfsEnvironment, new Path(targetPath.toString()), temporaryStagingDirectoryPath);
            return new LocationHandle(targetPath, writePath, STAGE_AND_MOVE_TO_TARGET_DIRECTORY);
        }
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
    }

    @Override
    public LocationHandle forOptimize(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table)
    {
        // For OPTIMIZE write result files directly to table directory; that is needed by the commit logic in HiveMetadata#finishTableExecute
        Location targetPath = Location.of(table.getStorage().getLocation());
        return new LocationHandle(targetPath, targetPath, DIRECT_TO_TARGET_EXISTING_DIRECTORY);
    }

    private boolean shouldUseTemporaryDirectory(HdfsContext context, Path path, boolean hasExternalLocation)
    {
        return temporaryStagingDirectoryEnabled
                // skip using temporary directory for S3
                && !isS3FileSystem(context, hdfsEnvironment, path)
                // skip using temporary directory if destination is encrypted; it's not possible to move a file between encryption zones
                && !isHdfsEncrypted(context, hdfsEnvironment, path)
                // Skip using temporary directory if destination is external. Target may be on a different file system.
                && !hasExternalLocation;
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
            Location targetPath = Location.of(partition.get().getStorage().getLocation());
            Location writePath = getPartitionWritePath(locationHandle, partitionName, writeMode, targetPath);
            return new WriteInfo(targetPath, writePath, writeMode);
        }
        // new partition
        return new WriteInfo(
                locationHandle.getTargetPath().appendPath(partitionName),
                locationHandle.getWritePath().appendPath(partitionName),
                locationHandle.getWriteMode());
    }

    private static Location getPartitionWritePath(LocationHandle locationHandle, String partitionName, WriteMode writeMode, Location targetPath)
    {
        return switch (writeMode) {
            case STAGE_AND_MOVE_TO_TARGET_DIRECTORY -> locationHandle.getWritePath().appendPath(partitionName);
            case DIRECT_TO_TARGET_EXISTING_DIRECTORY -> targetPath;
            case DIRECT_TO_TARGET_NEW_DIRECTORY -> throw new UnsupportedOperationException(format("inserting into existing partition is not supported for %s", writeMode));
        };
    }
}
