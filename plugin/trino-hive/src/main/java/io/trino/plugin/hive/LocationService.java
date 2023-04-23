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

import io.trino.filesystem.Location;
import io.trino.plugin.hive.LocationHandle.WriteMode;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface LocationService
{
    Location forNewTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName);

    LocationHandle forNewTableAsSelect(SemiTransactionalHiveMetastore metastore, ConnectorSession session, String schemaName, String tableName, Optional<Location> externalLocation);

    LocationHandle forExistingTable(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table);

    LocationHandle forOptimize(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table);

    /**
     * targetPath and writePath will be root directory of all partition and table paths
     * that may be returned by {@link #getTableWriteInfo(LocationHandle, boolean)} and {@link #getPartitionWriteInfo(LocationHandle, Optional, String)} method.
     */
    WriteInfo getQueryWriteInfo(LocationHandle locationHandle);

    WriteInfo getTableWriteInfo(LocationHandle locationHandle, boolean overwrite);

    /**
     * If {@code partition} is present, returns {@code WriteInfo} for appending existing partition;
     * otherwise, returns {@code WriteInfo} for writing new partition or overwriting existing partition.
     */
    WriteInfo getPartitionWriteInfo(LocationHandle locationHandle, Optional<Partition> partition, String partitionName);

    record WriteInfo(Location targetPath, Location writePath, WriteMode writeMode)
    {
        public WriteInfo
        {
            requireNonNull(targetPath, "targetPath is null");
            requireNonNull(writePath, "writePath is null");
            requireNonNull(writeMode, "writeMode is null");
        }

        /**
         * Target path for the partition, unpartitioned table, or the query.
         */
        @Override
        public Location targetPath()
        {
            return targetPath;
        }

        /**
         * Temporary path for writing to the partition, unpartitioned table or the query.
         * <p>
         * It may be the same as {@code targetPath}.
         */
        @Override
        public Location writePath()
        {
            return writePath;
        }
    }
}
