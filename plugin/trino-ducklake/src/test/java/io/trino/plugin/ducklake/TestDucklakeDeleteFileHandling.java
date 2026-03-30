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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeDataFile;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeDeleteFileHandling
{
    private static Path sourceCatalogPath;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        sourceCatalogPath = Path.of("target/test-catalog/catalog.db");
        if (!Files.exists(sourceCatalogPath)) {
            synchronized (DucklakeCatalogGenerator.class) {
                if (!Files.exists(sourceCatalogPath)) {
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }
    }

    @Test
    public void testDeleteFileSuppressesRows()
            throws Exception
    {
        Path isolatedCatalogDir = Path.of("target/test-catalog-delete-" + UUID.randomUUID());
        copyDirectory(sourceCatalogPath.getParent(), isolatedCatalogDir);
        Path isolatedCatalogPath = isolatedCatalogDir.resolve("catalog.db");
        Path dataRoot = isolatedCatalogDir.resolve("data");
        updateCatalogDataPath(isolatedCatalogPath, dataRoot);

        DucklakeConfig config = new DucklakeConfig();
        config.setCatalogDatabaseUrl("jdbc:sqlite:" + isolatedCatalogPath.toAbsolutePath());
        config.setDataPath(dataRoot.toAbsolutePath().toString());
        config.setMaxCatalogConnections(5);

        DucklakeCatalog catalog = new SqliteDucklakeCatalog(config);
        try {
            DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);
            DucklakePageSourceProvider pageSourceProvider = new DucklakePageSourceProvider(
                    new LocalFileSystemFactory(Path.of("/")),
                    new FileFormatDataSourceStats(),
                    new ParquetReaderConfig().toParquetReaderOptions());

            long snapshotId = catalog.getCurrentSnapshotId();
            DucklakeTable table = getTable(catalog, "test_schema", "simple_table", snapshotId);
            DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
            long priceColumnId = getColumnId(catalog, table.tableId(), snapshotId, "price");
            DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

            DucklakeSplit splitBeforeDelete = getSplits(splitManager, tableHandle).getFirst();
            long baselineRows = countRows(pageSourceProvider, tableHandle, splitBeforeDelete, priceColumn);
            assertThat(baselineRows).isGreaterThan(1);

            DucklakeDataFile dataFile = findDataFileForSplit(catalog, table.tableId(), snapshotId, splitBeforeDelete);
            DeleteFilePath deleteFilePath = writeDeleteParquetFile(splitBeforeDelete.dataFilePath(), dataFile.rowIdStart());
            insertDeleteFileMetadata(
                    isolatedCatalogPath,
                    table.tableId(),
                    snapshotId,
                    dataFile.dataFileId(),
                    deleteFilePath.pathForCatalog(),
                    deleteFilePath.pathIsRelative(),
                    Files.size(deleteFilePath.absolutePath()));

            DucklakeSplit splitAfterDelete = getSplits(splitManager, tableHandle).stream()
                    .filter(split -> split.dataFilePath().equals(splitBeforeDelete.dataFilePath()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Expected split for data file: " + splitBeforeDelete.dataFilePath()));

            assertThat(splitAfterDelete.deleteFilePath()).isPresent();
            long rowsAfterDelete = countRows(pageSourceProvider, tableHandle, splitAfterDelete, priceColumn);
            assertThat(rowsAfterDelete).isEqualTo(baselineRows - 1);
        }
        finally {
            catalog.close();
            deleteDirectory(isolatedCatalogDir);
        }
    }

    private static List<DucklakeSplit> getSplits(DucklakeSplitManager splitManager, DucklakeTableHandle tableHandle)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue())) {
            ImmutableList.Builder<DucklakeSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                for (ConnectorSplit split : splitSource.getNextBatch(1000).get().getSplits()) {
                    splits.add((DucklakeSplit) split);
                }
            }
            return splits.build();
        }
    }

    private static long countRows(
            DucklakePageSourceProvider pageSourceProvider,
            DucklakeTableHandle tableHandle,
            DucklakeSplit split,
            DucklakeColumnHandle column)
            throws Exception
    {
        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null,
                SESSION,
                split,
                tableHandle,
                ImmutableList.of(column),
                DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                }
            }
        }
        return rows;
    }

    private static DucklakeDataFile findDataFileForSplit(DucklakeCatalog catalog, long tableId, long snapshotId, DucklakeSplit split)
    {
        return catalog.getDataFiles(tableId, snapshotId).stream()
                .filter(dataFile -> split.dataFilePath().endsWith(dataFile.path()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing data file metadata for split path: " + split.dataFilePath()));
    }

    private static DeleteFilePath writeDeleteParquetFile(String dataFilePath, long deletedRowId)
            throws Exception
    {
        Path sourcePath = Path.of(dataFilePath);
        Path sourceParent = sourcePath.toAbsolutePath().getParent();
        Path deleteFileName = Path.of("ducklake-delete-" + UUID.randomUUID() + ".parquet");

        if (sourceParent == null) {
            throw new IllegalArgumentException("Cannot resolve delete file parent for path: " + dataFilePath);
        }

        Path deleteAbsolutePath = sourceParent.resolve(deleteFileName);

        Files.createDirectories(deleteAbsolutePath.getParent());
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("COPY (SELECT " + deletedRowId + "::BIGINT AS row_id) TO '" +
                    escapeSql(deleteAbsolutePath.toAbsolutePath().toString()) +
                    "' (FORMAT PARQUET)");
        }

        return new DeleteFilePath(deleteAbsolutePath.toString(), false, deleteAbsolutePath);
    }

    private static void insertDeleteFileMetadata(
            Path catalogPath,
            long tableId,
            long snapshotId,
            long dataFileId,
            String deletePath,
            boolean pathIsRelative,
            long fileSizeBytes)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath.toAbsolutePath())) {
            long nextDeleteFileId;
            try (Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery("SELECT COALESCE(MAX(delete_file_id), 0) + 1 FROM ducklake_delete_file")) {
                rs.next();
                nextDeleteFileId = rs.getLong(1);
            }

            String sql = "INSERT INTO ducklake_delete_file (" +
                    "delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, " +
                    "delete_count, file_size_bytes, footer_size, encryption_key) " +
                    "VALUES (?, ?, ?, NULL, ?, ?, ?, 'parquet', ?, ?, ?, NULL)";

            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setLong(1, nextDeleteFileId);
                statement.setLong(2, tableId);
                statement.setLong(3, snapshotId);
                statement.setLong(4, dataFileId);
                statement.setString(5, deletePath);
                statement.setBoolean(6, pathIsRelative);
                statement.setLong(7, 1);
                statement.setLong(8, fileSizeBytes);
                statement.setLong(9, 0);
                statement.executeUpdate();
            }
        }
    }

    private static void updateCatalogDataPath(Path catalogPath, Path dataRoot)
            throws Exception
    {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + catalogPath.toAbsolutePath());
                PreparedStatement statement = conn.prepareStatement("UPDATE ducklake_metadata SET value = ? WHERE key = 'data_path'")) {
            statement.setString(1, dataRoot.toAbsolutePath().toString());
            statement.executeUpdate();
        }
    }

    private static String escapeSql(String value)
    {
        return value.replace("'", "''");
    }

    private static void copyDirectory(Path source, Path target)
            throws Exception
    {
        Files.walk(source).forEach(path -> {
            try {
                Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination);
                }
                else {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to copy " + path + " to " + target, e);
            }
        });
    }

    private static void deleteDirectory(Path directory)
            throws Exception
    {
        if (Files.exists(directory)) {
            Files.walk(directory)
                    .sorted((left, right) -> -left.compareTo(right))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        }
                        catch (Exception e) {
                            throw new RuntimeException("Failed to delete: " + path, e);
                        }
                    });
        }
    }

    private static DucklakeSchema getSchema(DucklakeCatalog catalog, String schemaName, long snapshotId)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private static DucklakeTable getTable(DucklakeCatalog catalog, String schemaName, String tableName, long snapshotId)
    {
        DucklakeSchema schema = getSchema(catalog, schemaName, snapshotId);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }

    private static long getColumnId(DucklakeCatalog catalog, long tableId, long snapshotId, String columnName)
    {
        return catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }

    private record DeleteFilePath(String pathForCatalog, boolean pathIsRelative, Path absolutePath) {}
}
