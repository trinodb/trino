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
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakePageSourceProvider
{
    private static Path catalogPath;

    private DucklakeCatalog catalog;
    private DucklakeSplitManager splitManager;
    private DucklakePageSourceProvider pageSourceProvider;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        catalogPath = Path.of("target/test-catalog/catalog.db");
        if (!Files.exists(catalogPath)) {
            synchronized (TestDucklakePageSourceProvider.class) {
                if (!Files.exists(catalogPath)) {
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }
    }

    @BeforeEach
    public void setUp()
    {
        DucklakeConfig config = new DucklakeConfig();
        config.setCatalogDatabaseUrl("jdbc:sqlite:" + catalogPath.toAbsolutePath());
        config.setDataPath(catalogPath.getParent().toAbsolutePath().toString());
        config.setMaxCatalogConnections(5);

        catalog = new SqliteDucklakeCatalog(config);
        splitManager = new DucklakeSplitManager(catalog, config);
        pageSourceProvider = new DucklakePageSourceProvider(
                new LocalFileSystemFactory(Path.of("/")),
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig().toParquetReaderOptions());
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testParquetPredicatePrunesRowGroups()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit baseSplit = getSplits(tableHandle).getFirst();

        DucklakeSplit matchingSplit = new DucklakeSplit(
                baseSplit.dataFilePath(),
                baseSplit.deleteFilePath(),
                baseSplit.rowIdStart(),
                baseSplit.recordCount(),
                baseSplit.fileSizeBytes(),
                baseSplit.fileFormat(),
                TupleDomain.withColumnDomains(Map.of(priceColumn, Domain.singleValue(DOUBLE, 30.0))));
        DucklakeSplit nonMatchingSplit = new DucklakeSplit(
                baseSplit.dataFilePath(),
                baseSplit.deleteFilePath(),
                baseSplit.rowIdStart(),
                baseSplit.recordCount(),
                baseSplit.fileSizeBytes(),
                baseSplit.fileFormat(),
                TupleDomain.withColumnDomains(Map.of(priceColumn, Domain.singleValue(DOUBLE, 1000.0))));

        long matchingRows = countRows(tableHandle, matchingSplit, priceColumn);
        long nonMatchingRows = countRows(tableHandle, nonMatchingSplit, priceColumn);

        assertThat(matchingRows).isGreaterThan(0);
        assertThat(nonMatchingRows).isEqualTo(0);
    }

    @Test
    public void testParquetPredicateNoneReturnsNoRows()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit baseSplit = getSplits(tableHandle).getFirst();
        DucklakeSplit noneSplit = new DucklakeSplit(
                baseSplit.dataFilePath(),
                baseSplit.deleteFilePath(),
                baseSplit.rowIdStart(),
                baseSplit.recordCount(),
                baseSplit.fileSizeBytes(),
                baseSplit.fileFormat(),
                TupleDomain.none());

        assertThat(countRows(tableHandle, noneSplit, priceColumn)).isEqualTo(0);
    }

    @Test
    public void testParquetPredicateForMissingColumnDoesNotFilterRows()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit baseSplit = getSplits(tableHandle).getFirst();
        DucklakeColumnHandle missingColumn = new DucklakeColumnHandle(-1, "missing_col", DOUBLE, true);
        DucklakeSplit missingColumnPredicateSplit = new DucklakeSplit(
                baseSplit.dataFilePath(),
                baseSplit.deleteFilePath(),
                baseSplit.rowIdStart(),
                baseSplit.recordCount(),
                baseSplit.fileSizeBytes(),
                baseSplit.fileFormat(),
                TupleDomain.withColumnDomains(Map.of(missingColumn, Domain.singleValue(DOUBLE, 1.0))));

        long allRows = countRows(tableHandle, baseSplit, priceColumn);
        long rowsWithMissingColumnPredicate = countRows(tableHandle, missingColumnPredicateSplit, priceColumn);

        assertThat(allRows).isGreaterThan(0);
        assertThat(rowsWithMissingColumnPredicate).isEqualTo(allRows);
    }

    @Test
    public void testParquetPageSourceSupportsFileUriPath()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit baseSplit = getSplits(tableHandle).getFirst();
        DucklakeSplit fileUriSplit = new DucklakeSplit(
                Path.of(baseSplit.dataFilePath()).toUri().toString(),
                baseSplit.deleteFilePath(),
                baseSplit.rowIdStart(),
                baseSplit.recordCount(),
                baseSplit.fileSizeBytes(),
                baseSplit.fileFormat(),
                baseSplit.fileStatisticsDomain());

        assertThat(countRows(tableHandle, fileUriSplit, priceColumn)).isGreaterThan(0);
    }

    private long countRows(DucklakeTableHandle tableHandle, DucklakeSplit split, DucklakeColumnHandle column)
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

    private List<DucklakeSplit> getSplits(DucklakeTableHandle tableHandle)
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

    private DucklakeSchema getSchema(String schemaName, long snapshotId)
    {
        return catalog.listSchemas(snapshotId).stream()
                .filter(schema -> schema.schemaName().equals(schemaName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing schema: " + schemaName));
    }

    private DucklakeTable getTable(String schemaName, String tableName, long snapshotId)
    {
        DucklakeSchema schema = getSchema(schemaName, snapshotId);
        return catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(table -> table.tableName().equals(tableName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing table: " + schemaName + "." + tableName));
    }

    private long getColumnId(long tableId, long snapshotId, String columnName)
    {
        return catalog.getTableColumns(tableId, snapshotId).stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }
}
