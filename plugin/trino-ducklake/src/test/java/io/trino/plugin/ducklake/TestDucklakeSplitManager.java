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
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.spi.connector.ColumnHandle;
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
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakeSplitManager
{
    private static Path catalogPath;

    private DucklakeCatalog catalog;
    private DucklakeSplitManager splitManager;

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
    }

    @AfterEach
    public void tearDown()
    {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testGetSplitsWithoutPredicate()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<DucklakeSplit> splits = getSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(catalog.getDataFiles(table.tableId(), snapshotId).size());
    }

    @Test
    public void testGetSplitsPrunesByNumericStats()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");

        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);
        Constraint matchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 30.0))));
        Constraint nonMatchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 1000.0))));

        List<DucklakeSplit> matchingSplits = getSplits(tableHandle, matchingConstraint);
        List<DucklakeSplit> nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint);

        assertThat(matchingSplits).isNotEmpty();
        assertThat(nonMatchingSplits).isEmpty();
    }

    @Test
    public void testGetSplitsPrunesByDateStats()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long createdDateColumnId = getColumnId(table.tableId(), snapshotId, "created_date");

        DucklakeColumnHandle createdDateColumn = new DucklakeColumnHandle(createdDateColumnId, "created_date", DATE, true);
        long matchingDate = LocalDate.parse("2024-02-01").toEpochDay();
        long nonMatchingDate = LocalDate.parse("2025-01-01").toEpochDay();

        Constraint matchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) createdDateColumn, Domain.singleValue(DATE, matchingDate))));
        Constraint nonMatchingConstraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) createdDateColumn, Domain.singleValue(DATE, nonMatchingDate))));

        List<DucklakeSplit> matchingSplits = getSplits(tableHandle, matchingConstraint);
        List<DucklakeSplit> nonMatchingSplits = getSplits(tableHandle, nonMatchingConstraint);

        assertThat(matchingSplits).isNotEmpty();
        assertThat(nonMatchingSplits).isEmpty();
    }

    @Test
    public void testGetSplitsCarryFileStatisticsDomain()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);
        long priceColumnId = getColumnId(table.tableId(), snapshotId, "price");
        DucklakeColumnHandle priceColumn = new DucklakeColumnHandle(priceColumnId, "price", DOUBLE, true);
        TupleDomain<DucklakeColumnHandle> expectedDomain = TupleDomain.withColumnDomains(Map.of(priceColumn, Domain.singleValue(DOUBLE, 30.0)));
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) priceColumn, Domain.singleValue(DOUBLE, 30.0))));

        List<DucklakeSplit> splits = getSplits(tableHandle, constraint);

        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(split -> assertThat(split.fileStatisticsDomain()).isEqualTo(expectedDomain));
    }

    @Test
    public void testGetSplitsWithAlwaysTrueConstraintCarryAllDomain()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<DucklakeSplit> splits = getSplits(tableHandle, Constraint.alwaysTrue());

        assertThat(splits).isNotEmpty();
        assertThat(splits)
                .allSatisfy(split -> assertThat(split.fileStatisticsDomain().isAll()).isTrue());
    }

    @Test
    public void testGetSplitsReturnsInlinedSplitForInlinedTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "inlined_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "inlined_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst()).isInstanceOf(DucklakeInlinedSplit.class);

        DucklakeInlinedSplit inlinedSplit = (DucklakeInlinedSplit) splits.getFirst();
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId());
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId);
    }

    @Test
    public void testGetSplitsReturnsFileSplitsForFlushedTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "simple_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "simple_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).isNotEmpty();
        assertThat(splits).allSatisfy(split -> assertThat(split).isInstanceOf(DucklakeSplit.class));
    }

    @Test
    public void testGetSplitsReturnsEmptyForEmptyTable()
            throws Exception
    {
        long snapshotId = catalog.getCurrentSnapshotId();
        DucklakeTable table = getTable("test_schema", "empty_table", snapshotId);
        DucklakeTableHandle tableHandle = new DucklakeTableHandle("test_schema", "empty_table", table.tableId(), snapshotId);

        List<ConnectorSplit> splits = getRawSplits(tableHandle, Constraint.alwaysTrue());
        assertThat(splits).hasSize(1);
        assertThat(splits.getFirst()).isInstanceOf(DucklakeInlinedSplit.class);

        DucklakeInlinedSplit inlinedSplit = (DucklakeInlinedSplit) splits.getFirst();
        assertThat(inlinedSplit.tableId()).isEqualTo(table.tableId());
        assertThat(inlinedSplit.snapshotId()).isEqualTo(snapshotId);

        List<DucklakeColumn> columns = catalog.getTableColumns(table.tableId(), snapshotId);
        List<List<Object>> rows = catalog.readInlinedData(
                inlinedSplit.tableId(),
                inlinedSplit.schemaVersion(),
                inlinedSplit.snapshotId(),
                columns);
        assertThat(rows).isEmpty();
    }

    private List<DucklakeSplit> getSplits(DucklakeTableHandle tableHandle, Constraint constraint)
            throws Exception
    {
        return getRawSplits(tableHandle, constraint).stream()
                .map(DucklakeSplit.class::cast)
                .collect(toImmutableList());
    }

    private List<ConnectorSplit> getRawSplits(DucklakeTableHandle tableHandle, Constraint constraint)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null,
                null,
                tableHandle,
                DynamicFilter.EMPTY,
                constraint)) {
            com.google.common.collect.ImmutableList.Builder<ConnectorSplit> splits = com.google.common.collect.ImmutableList.builder();
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
}
