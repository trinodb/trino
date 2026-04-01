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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.trino.plugin.ducklake.catalog.DucklakeCatalog;
import io.trino.plugin.ducklake.catalog.DucklakeFilePartitionValue;
import io.trino.plugin.ducklake.catalog.DucklakePartitionSpec;
import io.trino.plugin.ducklake.catalog.DucklakePartitionTransform;
import io.trino.plugin.ducklake.catalog.DucklakeSchema;
import io.trino.plugin.ducklake.catalog.DucklakeTable;
import io.trino.plugin.ducklake.catalog.SqliteDucklakeCatalog;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.connector.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDucklakePartitionPruning
{
    private static DucklakeCatalog catalog;
    private static DucklakeConfig config;
    private static long snapshotId;
    private static DucklakeTable partitionedTable;
    private static DucklakeTable temporalPartitionedTable;
    private static DucklakeTable dailyPartitionedTable;

    @BeforeAll
    public static void setUpClass()
            throws Exception
    {
        Path catalogPath = Path.of("target/test-catalog/catalog.db");
        if (!Files.exists(catalogPath)) {
            synchronized (DucklakeCatalogGenerator.class) {
                if (!Files.exists(catalogPath)) {
                    DucklakeCatalogGenerator.generateTestCatalog();
                }
            }
        }

        config = new DucklakeConfig();
        config.setCatalogDatabaseUrl("jdbc:sqlite:" + catalogPath.toAbsolutePath());
        config.setDataPath(catalogPath.getParent().resolve("data").toAbsolutePath().toString());
        config.setMaxCatalogConnections(5);

        catalog = new SqliteDucklakeCatalog(config);
        snapshotId = catalog.getCurrentSnapshotId();

        DucklakeSchema schema = catalog.listSchemas(snapshotId).stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst().orElseThrow();
        partitionedTable = catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(t -> t.tableName().equals("partitioned_table"))
                .findFirst().orElseThrow();
        temporalPartitionedTable = catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(t -> t.tableName().equals("temporal_partitioned_table"))
                .findFirst().orElseThrow();
        dailyPartitionedTable = catalog.listTables(schema.schemaId(), snapshotId).stream()
                .filter(t -> t.tableName().equals("daily_partitioned_table"))
                .findFirst().orElseThrow();
    }

    @Test
    public void testPartitionSpecsReturned()
    {
        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                partitionedTable.tableId(), snapshotId);

        assertThat(specs).isNotEmpty();
        assertThat(specs.get(0).fields()).hasSize(1);
        assertThat(specs.get(0).fields().get(0).transform()).isEqualTo(DucklakePartitionTransform.IDENTITY);
    }

    @Test
    public void testFilePartitionValuesReturned()
    {
        Map<Long, List<DucklakeFilePartitionValue>> values = catalog.getFilePartitionValues(
                partitionedTable.tableId(), snapshotId);

        assertThat(values).isNotEmpty();
        // Each file should have a partition value for region
        for (List<DucklakeFilePartitionValue> fileValues : values.values()) {
            assertThat(fileValues).isNotEmpty();
            assertThat(fileValues.get(0).partitionKeyIndex()).isEqualTo(0);
        }
    }

    @Test
    public void testApplyFilterClassifiesEnforcedPredicate()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId);

        long regionColumnId = catalog.getTableColumns(partitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("region"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle regionColumn = new DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true);

        // Apply filter with region = 'US'
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) regionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        DucklakeTableHandle newHandle = (DucklakeTableHandle) result.get().getHandle();

        // Region predicate should be enforced (partition column with identity transform)
        assertThat(newHandle.enforcedPredicate().isAll()).isFalse();
        assertThat(newHandle.enforcedPredicate().getDomains().orElseThrow()).containsKey(regionColumn);

        // Remaining filter should NOT include region (it's enforced)
        TupleDomain<ColumnHandle> remaining = result.get().getRemainingFilter();
        assertThat(remaining.isAll()).isTrue();
    }

    @Test
    public void testApplyFilterNonPartitionColumnIsUnenforced()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId);

        long amountColumnId = catalog.getTableColumns(partitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("amount"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle amountColumn = new DucklakeColumnHandle(amountColumnId, "amount", DOUBLE, true);

        // Apply filter with amount > 100
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) amountColumn, Domain.create(
                        io.trino.spi.predicate.ValueSet.ofRanges(
                                io.trino.spi.predicate.Range.greaterThan(DOUBLE, 100.0)),
                        false))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        DucklakeTableHandle newHandle = (DucklakeTableHandle) result.get().getHandle();

        // Amount should be unenforced (not a partition column)
        assertThat(newHandle.enforcedPredicate().isAll()).isTrue();
        assertThat(newHandle.unenforcedPredicate().isAll()).isFalse();

        // Remaining filter should include amount
        assertThat(result.get().getRemainingFilter().isAll()).isFalse();
    }

    @Test
    public void testApplyFilterAllConstraintReturnsEmpty()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, Constraint.alwaysTrue());

        assertThat(result).isEmpty();
    }

    @Test
    public void testApplyFilterNoneConstraintProducesUnsatisfiableHandle()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId);

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, new Constraint(TupleDomain.none()));

        assertThat(result).isPresent();
        DucklakeTableHandle newHandle = (DucklakeTableHandle) result.get().getHandle();
        assertThat(newHandle.enforcedPredicate().isNone()).isTrue();
    }

    @Test
    public void testApplyFilterIsIdempotentForSamePredicate()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId);

        long regionColumnId = catalog.getTableColumns(partitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("region"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle regionColumn = new DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true);
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) regionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("US")))));

        DucklakeTableHandle filteredHandle = (DucklakeTableHandle) metadata.applyFilter(SESSION, tableHandle, constraint)
                .orElseThrow()
                .getHandle();

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> secondApply =
                metadata.applyFilter(SESSION, filteredHandle, constraint);
        assertThat(secondApply).isEmpty();
    }

    @Test
    public void testApplyFilterIgnoresComplexTypePredicate()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTable nestedTable = catalog.listSchemas(snapshotId).stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst()
                .flatMap(schema -> catalog.listTables(schema.schemaId(), snapshotId).stream()
                        .filter(t -> t.tableName().equals("nested_table"))
                        .findFirst())
                .orElseThrow();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "nested_table", nestedTable.tableId(), snapshotId);

        long metadataColumnId = catalog.getTableColumns(nestedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("metadata"))
                .findFirst().orElseThrow().columnId();
        RowType metadataType = RowType.from(List.of(
                new RowType.Field(Optional.of("key"), VARCHAR),
                new RowType.Field(Optional.of("value"), VARCHAR)));
        DucklakeColumnHandle metadataColumn = new DucklakeColumnHandle(metadataColumnId, "metadata", metadataType, true);

        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) metadataColumn, Domain.onlyNull(metadataType))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        // Complex types are not pushed down into Ducklake predicates.
        assertThat(result).isEmpty();
    }

    @Test
    public void testSplitManagerPrunesByPartitionValue()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long regionColumnId = catalog.getTableColumns(partitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("region"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle regionColumn = new DucklakeColumnHandle(regionColumnId, "region", VARCHAR, true);

        // Create table handle with enforced predicate for region = 'EU'
        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "partitioned_table",
                partitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        regionColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("EU")))));

        // Get all splits
        List<DucklakeSplit> allSplits = getSplits(splitManager, new DucklakeTableHandle(
                "test_schema", "partitioned_table", partitionedTable.tableId(), snapshotId));

        // Get pruned splits
        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // Pruned splits should be fewer than all splits
        assertThat(prunedSplits.size()).isLessThan(allSplits.size());
        assertThat(prunedSplits).isNotEmpty();
    }

    @Test
    public void testNonPartitionedTableAllPredicatesUnenforced()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        // Use simple_table which is NOT partitioned
        DucklakeTable simpleTable = catalog.listSchemas(snapshotId).stream()
                .filter(s -> s.schemaName().equals("test_schema"))
                .findFirst()
                .flatMap(schema -> catalog.listTables(schema.schemaId(), snapshotId).stream()
                        .filter(t -> t.tableName().equals("simple_table"))
                        .findFirst())
                .orElseThrow();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "simple_table", simpleTable.tableId(), snapshotId);

        long nameColumnId = catalog.getTableColumns(simpleTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("name"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle nameColumn = new DucklakeColumnHandle(nameColumnId, "name", VARCHAR, true);

        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) nameColumn, Domain.singleValue(VARCHAR, Slices.utf8Slice("Product A")))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        DucklakeTableHandle newHandle = (DucklakeTableHandle) result.get().getHandle();

        // No partition specs -> all predicates should be unenforced
        assertThat(newHandle.enforcedPredicate().isAll()).isTrue();
        assertThat(newHandle.unenforcedPredicate().isAll()).isFalse();
    }

    @Test
    public void testTemporalPartitionSpecHasYearAndMonthTransforms()
    {
        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                temporalPartitionedTable.tableId(), snapshotId);

        assertThat(specs).isNotEmpty();
        DucklakePartitionSpec spec = specs.get(0);
        assertThat(spec.fields()).hasSize(2);
        assertThat(spec.fields().get(0).transform()).isEqualTo(DucklakePartitionTransform.YEAR);
        assertThat(spec.fields().get(1).transform()).isEqualTo(DucklakePartitionTransform.MONTH);
    }

    @Test
    public void testTemporalPartitionFileValuesPresent()
    {
        Map<Long, List<DucklakeFilePartitionValue>> values = catalog.getFilePartitionValues(
                temporalPartitionedTable.tableId(), snapshotId);

        assertThat(values).isNotEmpty();
        // Each file should have 2 partition values (year + month)
        for (List<DucklakeFilePartitionValue> fileValues : values.values()) {
            assertThat(fileValues).hasSize(2);
        }
    }

    @Test
    public void testApplyFilterClassifiesTemporalPartitionAsEnforced()
    {
        DucklakeTypeConverter typeConverter = new DucklakeTypeConverter(TESTING_TYPE_MANAGER);
        DucklakeMetadata metadata = new DucklakeMetadata(catalog, typeConverter);

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId(), snapshotId);

        long eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Apply filter with event_date = DATE '2023-06-10'
        long daysSinceEpoch = java.time.LocalDate.of(2023, 6, 10).toEpochDay();
        Constraint constraint = new Constraint(TupleDomain.withColumnDomains(
                ImmutableMap.of((ColumnHandle) eventDateColumn,
                        Domain.singleValue(io.trino.spi.type.DateType.DATE, daysSinceEpoch))));

        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(SESSION, tableHandle, constraint);

        assertThat(result).isPresent();
        DucklakeTableHandle newHandle = (DucklakeTableHandle) result.get().getHandle();

        // event_date has temporal partition transforms (year, month) -> should be enforced
        assertThat(newHandle.enforcedPredicate().isAll()).isFalse();
        assertThat(newHandle.enforcedPredicate().getDomains().orElseThrow()).containsKey(eventDateColumn);

        // Remaining filter should NOT include event_date (it's enforced)
        assertThat(result.get().getRemainingFilter().isAll()).isTrue();
    }

    @Test
    public void testSplitManagerPrunesByTemporalPartitionValue()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Predicate: event_date in January 2023 (days 19358..19388)
        long jan1 = java.time.LocalDate.of(2023, 1, 1).toEpochDay();
        long jan31 = java.time.LocalDate.of(2023, 1, 31).toEpochDay();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        eventDateColumn, Domain.create(
                                io.trino.spi.predicate.ValueSet.ofRanges(
                                        io.trino.spi.predicate.Range.range(
                                                io.trino.spi.type.DateType.DATE, jan1, true, jan31, true)),
                                false))));

        // Get all splits (no predicate)
        List<DucklakeSplit> allSplits = getSplits(splitManager, new DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId(), snapshotId));

        // Get pruned splits (only Jan 2023)
        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // Should have 3 files total (Jan 2023, Jun 2023, Mar 2024) but only 1 after pruning
        assertThat(allSplits.size()).isGreaterThan(prunedSplits.size());
        assertThat(prunedSplits).isNotEmpty();
    }

    @Test
    public void testTemporalPruningByYearOnly()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long eventDateColumnId = catalog.getTableColumns(temporalPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Predicate: event_date in all of 2024
        long year2024Start = java.time.LocalDate.of(2024, 1, 1).toEpochDay();
        long year2024End = java.time.LocalDate.of(2024, 12, 31).toEpochDay();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        eventDateColumn, Domain.create(
                                io.trino.spi.predicate.ValueSet.ofRanges(
                                        io.trino.spi.predicate.Range.range(
                                                io.trino.spi.type.DateType.DATE, year2024Start, true, year2024End, true)),
                                false))));

        List<DucklakeSplit> allSplits = getSplits(splitManager, new DucklakeTableHandle(
                "test_schema", "temporal_partitioned_table",
                temporalPartitionedTable.tableId(), snapshotId));

        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // 2023 files should be pruned, only 2024 files remain
        assertThat(allSplits.size()).isGreaterThan(prunedSplits.size());
        assertThat(prunedSplits).isNotEmpty();
    }

    @Test
    public void testDailyPartitionSpecHasThreeTransforms()
    {
        List<DucklakePartitionSpec> specs = catalog.getPartitionSpecs(
                dailyPartitionedTable.tableId(), snapshotId);

        assertThat(specs).isNotEmpty();
        DucklakePartitionSpec spec = specs.get(0);
        assertThat(spec.fields()).hasSize(3);
        assertThat(spec.fields().get(0).transform()).isEqualTo(DucklakePartitionTransform.YEAR);
        assertThat(spec.fields().get(1).transform()).isEqualTo(DucklakePartitionTransform.MONTH);
        assertThat(spec.fields().get(2).transform()).isEqualTo(DucklakePartitionTransform.DAY);
    }

    @Test
    public void testDailyPartitionFileValuesPresent()
    {
        Map<Long, List<DucklakeFilePartitionValue>> values = catalog.getFilePartitionValues(
                dailyPartitionedTable.tableId(), snapshotId);

        assertThat(values).isNotEmpty();
        // Each file should have 3 partition values (year + month + day)
        for (List<DucklakeFilePartitionValue> fileValues : values.values()) {
            assertThat(fileValues).hasSize(3);
        }
    }

    @Test
    public void testDailyPrunesToSingleDay()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Predicate: event_date = '2023-06-15' (should match only that day's file)
        long june15 = java.time.LocalDate.of(2023, 6, 15).toEpochDay();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        eventDateColumn, Domain.singleValue(io.trino.spi.type.DateType.DATE, june15))));

        List<DucklakeSplit> allSplits = getSplits(splitManager, new DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId(), snapshotId));

        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // 4 files total (Jun 15, Jun 20, Jul 1, Jan 10 2024), only Jun 15 should survive
        assertThat(allSplits).hasSize(4);
        assertThat(prunedSplits).hasSize(1);
    }

    @Test
    public void testDailyPrunesWithinSameMonth()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Predicate: event_date between Jun 14 and Jun 16 (should match only Jun 15, not Jun 20)
        long june14 = java.time.LocalDate.of(2023, 6, 14).toEpochDay();
        long june16 = java.time.LocalDate.of(2023, 6, 16).toEpochDay();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        eventDateColumn, Domain.create(
                                io.trino.spi.predicate.ValueSet.ofRanges(
                                        io.trino.spi.predicate.Range.range(
                                                io.trino.spi.type.DateType.DATE, june14, true, june16, true)),
                                false))));

        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // Jun 20, Jul 1, and Jan 2024 should all be pruned. Only Jun 15 survives.
        assertThat(prunedSplits).hasSize(1);
    }

    @Test
    public void testDailyPrunesByMonthAcrossDays()
            throws Exception
    {
        DucklakeSplitManager splitManager = new DucklakeSplitManager(catalog, config);

        long eventDateColumnId = catalog.getTableColumns(dailyPartitionedTable.tableId(), snapshotId).stream()
                .filter(c -> c.columnName().equals("event_date"))
                .findFirst().orElseThrow().columnId();
        DucklakeColumnHandle eventDateColumn = new DucklakeColumnHandle(
                eventDateColumnId, "event_date", io.trino.spi.type.DateType.DATE, true);

        // Predicate: all of June 2023 (should match Jun 15 and Jun 20 but not Jul 1 or Jan 2024)
        long june1 = java.time.LocalDate.of(2023, 6, 1).toEpochDay();
        long june30 = java.time.LocalDate.of(2023, 6, 30).toEpochDay();

        DucklakeTableHandle tableHandle = new DucklakeTableHandle(
                "test_schema", "daily_partitioned_table",
                dailyPartitionedTable.tableId(), snapshotId,
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        eventDateColumn, Domain.create(
                                io.trino.spi.predicate.ValueSet.ofRanges(
                                        io.trino.spi.predicate.Range.range(
                                                io.trino.spi.type.DateType.DATE, june1, true, june30, true)),
                                false))));

        List<DucklakeSplit> prunedSplits = getSplits(splitManager, tableHandle);

        // Jun 15 and Jun 20 survive; Jul 1 and Jan 2024 are pruned
        assertThat(prunedSplits).hasSize(2);
    }

    private static List<DucklakeSplit> getSplits(DucklakeSplitManager splitManager, DucklakeTableHandle tableHandle)
            throws Exception
    {
        try (ConnectorSplitSource splitSource = splitManager.getSplits(
                null, SESSION, tableHandle, DynamicFilter.EMPTY, Constraint.alwaysTrue())) {
            ImmutableList.Builder<DucklakeSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                for (ConnectorSplit split : splitSource.getNextBatch(1000).get().getSplits()) {
                    splits.add((DucklakeSplit) split);
                }
            }
            return splits.build();
        }
    }
}
