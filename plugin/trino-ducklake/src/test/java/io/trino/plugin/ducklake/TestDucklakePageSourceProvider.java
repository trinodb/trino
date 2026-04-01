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
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
            synchronized (DucklakeCatalogGenerator.class) {
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
                new ParquetReaderConfig().toParquetReaderOptions(),
                catalog);
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

    @Test
    public void testReadStructColumn()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "nested_table", table.tableId(), snapshotId);

        Type structType = RowType.from(List.of(
                new RowType.Field(Optional.of("key"), VARCHAR),
                new RowType.Field(Optional.of("value"), VARCHAR)));
        long metadataColumnId = getColumnId(table.tableId(), snapshotId, "metadata");
        DucklakeColumnHandle metadataColumn = new DucklakeColumnHandle(metadataColumnId, "metadata", structType, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();
        assertThat(countRows(tableHandle, split, metadataColumn)).isEqualTo(3);
    }

    @Test
    public void testReadMapColumn()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "nested_table", table.tableId(), snapshotId);

        TypeOperators typeOperators = new TypeOperators();
        Type mapType = new MapType(VARCHAR, INTEGER, typeOperators);
        long tagsColumnId = getColumnId(table.tableId(), snapshotId, "tags");
        DucklakeColumnHandle tagsColumn = new DucklakeColumnHandle(tagsColumnId, "tags", mapType, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();
        assertThat(countRows(tableHandle, split, tagsColumn)).isEqualTo(3);
    }

    @Test
    public void testReadNestedArrayColumn()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "nested_table", table.tableId(), snapshotId);

        Type nestedListType = new ArrayType(new ArrayType(INTEGER));
        long nestedListColumnId = getColumnId(table.tableId(), snapshotId, "nested_list");
        DucklakeColumnHandle nestedListColumn = new DucklakeColumnHandle(nestedListColumnId, "nested_list", nestedListType, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();
        assertThat(countRows(tableHandle, split, nestedListColumn)).isEqualTo(3);
    }

    @Test
    public void testReadComplexStructColumn()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "nested_table", table.tableId(), snapshotId);

        TypeOperators typeOperators = new TypeOperators();
        Type complexStructType = RowType.from(List.of(
                new RowType.Field(Optional.of("name"), VARCHAR),
                new RowType.Field(Optional.of("scores"), new ArrayType(INTEGER)),
                new RowType.Field(Optional.of("attrs"), new MapType(VARCHAR, VARCHAR, typeOperators))));
        long complexColumnId = getColumnId(table.tableId(), snapshotId, "complex_struct");
        DucklakeColumnHandle complexColumn = new DucklakeColumnHandle(complexColumnId, "complex_struct", complexStructType, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();
        assertThat(countRows(tableHandle, split, complexColumn)).isEqualTo(3);
    }

    @Test
    public void testDynamicFilterExcludesAllReturnsNoRows()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();

        // Dynamic filter that excludes all data (price = 999999.0, no such value)
        DynamicFilter exclusiveFilter = createDynamicFilter(
                priceColumn, Domain.singleValue(DOUBLE, 999999.0));

        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(priceColumn), exclusiveFilter)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                }
            }
        }
        // The fileStatisticsDomain intersected with the dynamic filter should yield NONE
        assertThat(rows).isEqualTo(0);
    }

    @Test
    public void testDynamicFilterIncludesAllReturnsAllRows()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();

        // Dynamic filter with a wide range that includes all data
        DynamicFilter wideFilter = createDynamicFilter(
                priceColumn, Domain.create(ValueSet.ofRanges(
                        io.trino.spi.predicate.Range.range(DOUBLE, 0.0, true, 100.0, true)), false));

        long baseRows = countRows(tableHandle, split, priceColumn);

        long filteredRows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(priceColumn), wideFilter)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    filteredRows += page.getPositionCount();
                }
            }
        }
        assertThat(filteredRows).isEqualTo(baseRows);
    }

    @Test
    public void testSchemaEvolutionMissingColumnReturnsNulls()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        // Request a column that does not exist in the Parquet file
        DucklakeColumnHandle missingColumn = new DucklakeColumnHandle(-1, "new_column", VARCHAR, true);
        DucklakeSplit split = getSplits(tableHandle).getFirst();

        // Should return all rows, with null values for the missing column
        long rows = countRows(tableHandle, split, missingColumn);
        assertThat(rows).isGreaterThan(0);

        // Verify all values are null
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(missingColumn), DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    Block block = page.getBlock(0);
                    for (int i = 0; i < block.getPositionCount(); i++) {
                        assertThat(block.isNull(i)).isTrue();
                    }
                }
            }
        }
    }

    @Test
    public void testSchemaEvolutionMixedColumns()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);
        DucklakeColumnHandle missingColumn = new DucklakeColumnHandle(-1, "new_column", VARCHAR, true);

        DucklakeSplit split = getSplits(tableHandle).getFirst();

        // Read both existing and missing columns together
        List<ColumnHandle> mixedColumns = ImmutableList.of(priceColumn, missingColumn);
        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, mixedColumns, DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                    // price column (index 0) should have non-null values
                    Block priceBlock = page.getBlock(0);
                    // missing column (index 1) should be all nulls
                    Block missingBlock = page.getBlock(1);
                    for (int i = 0; i < page.getPositionCount(); i++) {
                        assertThat(priceBlock.isNull(i)).isFalse();
                        assertThat(missingBlock.isNull(i)).isTrue();
                    }
                }
            }
        }
        assertThat(rows).isGreaterThan(0);
    }

    @Test
    public void testInlinedPageSourceReturnsCorrectRows()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "inlined_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "inlined_table", table.tableId(), snapshotId);

        // Get the inlined split
        ConnectorSplit split = getAnySplits(tableHandle).getFirst();
        assertThat(split).isInstanceOf(DucklakeInlinedSplit.class);

        long idColumnId = getColumnId(table.tableId(), snapshotId, "id");
        long nameColumnId = getColumnId(table.tableId(), snapshotId, "name");
        long valueColumnId = getColumnId(table.tableId(), snapshotId, "value");
        DucklakeColumnHandle idColumn = new DucklakeColumnHandle(idColumnId, "id", INTEGER, true);
        DucklakeColumnHandle nameColumn = new DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true);
        DucklakeColumnHandle valueColumn = new DucklakeColumnHandle(valueColumnId, "value", DOUBLE, true);

        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(idColumn, nameColumn, valueColumn), DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                }
            }
        }
        assertThat(rows).isEqualTo(3);
    }

    @Test
    public void testInlinedPageSourceColumnProjection()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "inlined_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "inlined_table", table.tableId(), snapshotId);

        ConnectorSplit split = getAnySplits(tableHandle).getFirst();
        assertThat(split).isInstanceOf(DucklakeInlinedSplit.class);

        // Request only the name column
        long nameColumnId = getColumnId(table.tableId(), snapshotId, "name");
        DucklakeColumnHandle nameColumn = new DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true);

        long rows = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                null, SESSION, split, tableHandle, ImmutableList.of(nameColumn), DynamicFilter.EMPTY)) {
            while (!pageSource.isFinished()) {
                var page = pageSource.getNextSourcePage();
                if (page != null) {
                    rows += page.getPositionCount();
                    // Verify we only get one column
                    assertThat(page.getChannelCount()).isEqualTo(1);
                }
            }
        }
        assertThat(rows).isEqualTo(3);
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
        return getAnySplits(tableHandle).stream()
                .map(DucklakeSplit.class::cast)
                .collect(toImmutableList());
    }

    private List<ConnectorSplit> getAnySplits(DucklakeTableHandle tableHandle)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null,
                SESSION,
                tableHandle,
                DynamicFilter.EMPTY,
                Constraint.alwaysTrue())) {
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splits.addAll(splitSource.getNextBatch(1000).get().getSplits());
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

    private static DynamicFilter createDynamicFilter(DucklakeColumnHandle column, Domain domain)
    {
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                Map.of(column, domain));
        return new DynamicFilter()
        {
            @Override
            public java.util.Set<ColumnHandle> getColumnsCovered()
            {
                return java.util.Set.of(column);
            }

            @Override
            public java.util.concurrent.CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }
}
