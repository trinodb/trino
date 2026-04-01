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

import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeColumn;
import io.trino.plugin.ducklake.catalog.DucklakeColumnStats;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Ducklake catalog reading from DuckDB-generated SQLite database.
 */
public class TestDucklakeCatalog
{
    private static Path catalogPath;
    private DucklakeCatalog catalog;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        // Look for test catalog
        catalogPath = Path.of("target/test-catalog/catalog.db");

        // Generate test catalog once for all tests
        if (!Files.exists(catalogPath)) {
            synchronized (DucklakeCatalogGenerator.class) {
                if (!Files.exists(catalogPath)) {
                    System.out.println("Test catalog not found, generating with DuckDB...");
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }
    }

    @BeforeEach
    public void setUp()
    {
        // Create catalog instance for each test
        DucklakeConfig config = new DucklakeConfig();
        config.setCatalogDatabaseUrl("jdbc:sqlite:" + catalogPath.toAbsolutePath());
        config.setDataPath(catalogPath.getParent().toAbsolutePath().toString());
        config.setMaxCatalogConnections(5);

        catalog = new SqliteDucklakeCatalog(config);
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testGetCurrentSnapshot()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        assertThat(snapshotId).isGreaterThan(0);
        assertThat(catalog.listSchemas(snapshotId))
                .extracting(DucklakeSchema::schemaName)
                .contains("test_schema");
    }

    @Test
    public void testListSchemas()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        var schemas = catalog.listSchemas(snapshotId);

        assertThat(schemas)
                .isNotEmpty()
                .anySatisfy(schema ->
                        assertThat(schema.schemaName()).isEqualTo("test_schema"));
    }

    @Test
    public void testListTables()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeSchema testSchema = getSchema("test_schema", snapshotId);
        List<DucklakeTable> tables = catalog.listTables(testSchema.schemaId(), snapshotId);

        assertThat(tables)
                .isNotEmpty()
                .hasSize(16)
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("simple_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("partitioned_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("temporal_partitioned_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("array_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("nested_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("wide_types_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("nullable_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("empty_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("schema_evolution_table"))
                .anySatisfy(table ->
                        assertThat(table.tableName()).isEqualTo("aggregation_table"));
    }

    @Test
    public void testGetTable()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        var retrievedTable = catalog.getTableById(table.tableId(), snapshotId);

        assertThat(retrievedTable)
                .isPresent()
                .get()
                .satisfies(t -> {
                    assertThat(t.tableId()).isEqualTo(table.tableId());
                    assertThat(t.tableName()).isEqualTo(table.tableName());
                });
    }

    @Test
    public void testGetDataFiles()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        var dataFiles = catalog.getDataFiles(table.tableId(), snapshotId);

        assertThat(dataFiles)
                .isNotEmpty()
                .allSatisfy(file -> {
                    assertThat(file.path()).isNotBlank();
                    assertThat(file.fileFormat()).isEqualTo("parquet");
                    assertThat(file.recordCount()).isGreaterThan(0);
                    assertThat(file.fileSizeBytes()).isGreaterThan(0);
                });
    }

    @Test
    public void testGetTableColumnsResolvesListType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "array_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        assertThat(columns)
                .extracting(DucklakeColumn::columnName)
                .containsExactly("id", "product_name", "tags", "quantity");

        DucklakeColumn tagsColumn = columns.stream()
                .filter(column -> column.columnName().equals("tags"))
                .findFirst()
                .orElseThrow();
        assertThat(tagsColumn.columnType()).isEqualTo("list<varchar>");
    }

    @Test
    public void testGetTableColumnsResolvesStructType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        assertThat(columns)
                .extracting(DucklakeColumn::columnName)
                .containsExactly("id", "metadata", "tags", "nested_list", "complex_struct");

        DucklakeColumn metadataColumn = columns.stream()
                .filter(column -> column.columnName().equals("metadata"))
                .findFirst()
                .orElseThrow();
        assertThat(metadataColumn.columnType()).isEqualTo("struct<key:varchar,value:varchar>");
    }

    @Test
    public void testGetTableColumnsResolvesMapType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn tagsColumn = columns.stream()
                .filter(column -> column.columnName().equals("tags"))
                .findFirst()
                .orElseThrow();
        assertThat(tagsColumn.columnType()).isEqualTo("map<varchar,int32>");
    }

    @Test
    public void testGetTableColumnsResolvesNestedListType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn nestedListColumn = columns.stream()
                .filter(column -> column.columnName().equals("nested_list"))
                .findFirst()
                .orElseThrow();
        assertThat(nestedListColumn.columnType()).isEqualTo("list<list<int32>>");
    }

    @Test
    public void testGetTableColumnsResolvesComplexStructType()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nested_table", snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        DucklakeColumn complexColumn = columns.stream()
                .filter(column -> column.columnName().equals("complex_struct"))
                .findFirst()
                .orElseThrow();
        // struct<name:varchar,scores:list<int32>,attrs:map<varchar,varchar>>
        assertThat(complexColumn.columnType())
                .startsWith("struct<")
                .contains("name:varchar")
                .contains("scores:list<int32>")
                .contains("attrs:map<varchar,varchar>");
    }

    @Test
    public void testGetDataFileIdsForPredicate()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        long priceColumnId = getColumnId(columns, "price");
        long createdDateColumnId = getColumnId(columns, "created_date");

        assertThat(catalog.getDataFileIdsForPredicate(table.tableId(), priceColumnId, snapshotId, 30.0, 30.0))
                .isNotEmpty();
        assertThat(catalog.getDataFileIdsForPredicate(table.tableId(), priceColumnId, snapshotId, 1000.0, 1000.0))
                .isEmpty();

        assertThat(catalog.getDataFileIdsForPredicate(table.tableId(), createdDateColumnId, snapshotId, "2024-02-01", "2024-02-01"))
                .isNotEmpty();
        assertThat(catalog.getDataFileIdsForPredicate(table.tableId(), createdDateColumnId, snapshotId, "2025-01-01", "2025-01-01"))
                .isEmpty();
    }

    @Test
    public void testGetColumnStatsReturnsTypedMinMax()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);

        // Build column type map
        Map<Long, String> columnTypes = columns.stream()
                .collect(java.util.stream.Collectors.toMap(DucklakeColumn::columnId, DucklakeColumn::columnType));

        List<DucklakeColumnStats> statsList = catalog.getColumnStats(table.tableId(), snapshotId, columnTypes);
        assertThat(statsList).isNotEmpty();

        // Verify price column (DOUBLE) has typed min/max
        long priceColumnId = getColumnId(columns, "price");
        DucklakeColumnStats priceStats = statsList.stream()
                .filter(s -> s.columnId() == priceColumnId)
                .findFirst().orElseThrow();
        assertThat(priceStats.minValue()).isPresent();
        assertThat(priceStats.maxValue()).isPresent();
        // price values: 19.99, 29.99, 39.99, 49.99, 59.99
        assertThat(Double.parseDouble(priceStats.minValue().get())).isLessThanOrEqualTo(19.99);
        assertThat(Double.parseDouble(priceStats.maxValue().get())).isGreaterThanOrEqualTo(59.99);

        // Verify id column (INTEGER) has typed min/max
        long idColumnId = getColumnId(columns, "id");
        DucklakeColumnStats idStats = statsList.stream()
                .filter(s -> s.columnId() == idColumnId)
                .findFirst().orElseThrow();
        assertThat(idStats.minValue()).isPresent();
        assertThat(idStats.maxValue()).isPresent();
        assertThat(Long.parseLong(idStats.minValue().get())).isEqualTo(1);
        assertThat(Long.parseLong(idStats.maxValue().get())).isEqualTo(5);

        // Verify value counts
        assertThat(priceStats.totalValueCount()).isEqualTo(5);
        assertThat(priceStats.totalNullCount()).isEqualTo(0);
    }

    @Test
    public void testGetTableStatisticsRowCountAndRanges()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        // Row count should be 5
        assertThat(stats.getRowCount().getValue()).isEqualTo(5.0);

        // Should have column statistics
        assertThat(stats.getColumnStatistics()).isNotEmpty();
    }

    @Test
    public void testGetTableStatisticsTracksNullFractionsAndRanges()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "nullable_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "nullable_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        // nullable_table has 4 rows
        assertThat(stats.getRowCount().getValue()).isEqualTo(4.0);

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle);

        ColumnStatistics idStats = stats.getColumnStatistics().get(handles.get("id"));
        ColumnStatistics nameStats = stats.getColumnStatistics().get(handles.get("name"));
        ColumnStatistics priceStats = stats.getColumnStatistics().get(handles.get("price"));
        assertThat(idStats.getNullsFraction().getValue()).isEqualTo(0.25);
        assertThat(nameStats.getNullsFraction().getValue()).isEqualTo(0.5);
        assertThat(priceStats.getNullsFraction().getValue()).isEqualTo(0.5);

        // non-null prices are 10.0 and 20.0
        DoubleRange priceRange = priceStats.getRange().orElseThrow();
        assertThat(priceRange.getMin()).isEqualTo(10.0);
        assertThat(priceRange.getMax()).isEqualTo(20.0);
    }

    @Test
    public void testGetTableStatisticsTracksDateRange()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "simple_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        Map<String, ColumnHandle> handles = metadata.getColumnHandles(
                io.trino.testing.connector.TestingConnectorSession.SESSION,
                tableHandle);
        ColumnStatistics dateStats = stats.getColumnStatistics().get(handles.get("created_date"));
        DoubleRange dateRange = dateStats.getRange().orElseThrow();

        double expectedMin = LocalDate.parse("2024-01-05").toEpochDay();
        double expectedMax = LocalDate.parse("2024-03-10").toEpochDay();
        assertThat(dateRange.getMin()).isEqualTo(expectedMin);
        assertThat(dateRange.getMax()).isEqualTo(expectedMax);
    }

    @Test
    public void testGetTableStatisticsForEmptyTable()
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "empty_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "empty_table", table.tableId(), snapshotId);

        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(
                io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);
        io.trino.spi.statistics.TableStatistics stats = metadata.getTableStatistics(
                io.trino.testing.connector.TestingConnectorSession.SESSION, tableHandle);

        // Empty table may have unknown row-count estimate if stats are absent.
        assertThat(stats.getRowCount().isUnknown()).isTrue();
        assertThat(stats.getColumnStatistics()).isEmpty();
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

    private long getColumnId(List<DucklakeColumn> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.columnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing column: " + columnName))
                .columnId();
    }
}
