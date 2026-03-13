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
package io.trino.plugin.ducklake;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeDataFile;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Split manager for Ducklake connector.
 * Discovers data files from SQL catalog and creates splits for each Parquet file.
 */
public class DucklakeSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(DucklakeSplitManager.class);

    private final DucklakeCatalog catalog;
    private final DucklakeConfig config;

    @Inject
    public DucklakeSplitManager(DucklakeCatalog catalog, DucklakeConfig config)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DucklakeTableHandle tableHandle = (DucklakeTableHandle) table;

        log.debug("Getting splits for table %s at snapshot %d", tableHandle.tableName(), tableHandle.snapshotId());

        // Get all data files for this table at the snapshot
        List<DucklakeDataFile> dataFiles = catalog.getDataFiles(
                tableHandle.tableId(),
                tableHandle.snapshotId());

        log.debug("Found %d data files for table %s", dataFiles.size(), tableHandle.tableName());

        // TODO: Apply predicate pushdown via ducklake_file_column_stats

        // Convert data files to splits
        List<DucklakeSplit> splits = dataFiles.stream()
                .map(dataFile -> createSplit(dataFile))
                .collect(toImmutableList());

        log.debug("Created %d splits for table %s", splits.size(), tableHandle.tableName());

        return new FixedSplitSource(splits);
    }

    private DucklakeSplit createSplit(DucklakeDataFile dataFile)
    {
        // Resolve the full path for the data file
        String dataFilePath = resolveFilePath(dataFile.path(), dataFile.pathIsRelative());

        // Resolve delete file path if present
        Optional<String> deleteFilePath = dataFile.deleteFilePath()
                .map(path -> resolveFilePath(path, dataFile.deleteFilePathIsRelative().orElse(false)));

        return new DucklakeSplit(
                dataFilePath,
                deleteFilePath,
                dataFile.recordCount(),
                dataFile.fileSizeBytes(),
                dataFile.fileFormat());
    }

    private String resolveFilePath(String path, boolean isRelative)
    {
        if (!isRelative) {
            return path;
        }

        // Relative to data path from ducklake_metadata or config
        Optional<String> catalogDataPath = catalog.getDataPath();
        String basePath = catalogDataPath.orElseGet(() -> config.getDataPath());

        if (basePath == null) {
            throw new IllegalStateException("No data path configured for relative file paths");
        }

        // Use Location to properly join paths
        return Location.of(basePath).appendPath(path).toString();
    }
}
