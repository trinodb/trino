/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.TableChanges.BucketChanges;
import io.starburst.schema.discovery.TableChanges.PartitionValueChanges;
import io.starburst.schema.discovery.TableChanges.TableColumnChanges;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.TableChangesBuilder;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.SlashEndedPath;
import io.starburst.schema.discovery.models.TableFormat;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;

public class DiscoveredTablesDiffGenerator
{
    private DiscoveredTablesDiffGenerator() {}

    public enum DiffType
    {
        CREATE, CHANGE, DROP, NOT_MODIFIED, RECREATE
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "diffType")
    @JsonSubTypes({
            @JsonSubTypes.Type(name = "CREATE", value = NewTable.class),
            @JsonSubTypes.Type(name = "CHANGE", value = UpdatedTable.class),
            @JsonSubTypes.Type(name = "DROP", value = DroppedTable.class),
            @JsonSubTypes.Type(name = "NOT_MODIFIED", value = UnchangedTable.class),
            @JsonSubTypes.Type(name = "RECREATE", value = RecreatedTable.class)
    })
    public sealed interface DiffTable
    {
        @JsonProperty("tableName")
        TableName tableName();

        @JsonProperty("path")
        SlashEndedPath path();

        @JsonProperty("format")
        TableFormat format();

        @JsonProperty("diffType")
        DiffType diffType();

        @JsonProperty("columns")
        List<? extends DiffColumn> columns();

        @JsonProperty("partitionColumns")
        List<? extends DiffColumn> partitionColumns();

        @JsonProperty("partitionValues")
        List<? extends DiffPartitionValue> partitionValues();

        @JsonProperty("partitionProjections")
        List<ColumnPartitionProjection> partitionProjections();
    }

    public record NewTable(TableName tableName, SlashEndedPath path, TableFormat format, List<NewColumn> columns, List<NewColumn> partitionColumns,
                           List<NewPartitionValue> partitionValues, List<ColumnPartitionProjection> partitionProjections)
            implements DiffTable
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.CREATE;
        }
    }

    public record UpdatedTable(TableName tableName, SlashEndedPath path, TableFormat format, List<DiffColumn> columns, List<DiffColumn> partitionColumns,
                               List<DiffPartitionValue> partitionValues, List<ColumnPartitionProjection> partitionProjections)
            implements DiffTable
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.CHANGE;
        }
    }

    public record DroppedTable(TableName tableName, SlashEndedPath path, TableFormat format, List<DroppedColumn> columns, List<DroppedColumn> partitionColumns,
                               List<DroppedPartitionValue> partitionValues, List<ColumnPartitionProjection> partitionProjections)
            implements DiffTable
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.DROP;
        }
    }

    public record RecreatedTable(TableName tableName, SlashEndedPath path, TableFormat format, List<DiffColumn> columns, List<DiffColumn> partitionColumns,
                                 List<DiffPartitionValue> partitionValues, List<ColumnPartitionProjection> partitionProjections)
            implements DiffTable
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.RECREATE;
        }
    }

    public record UnchangedTable(TableName tableName, SlashEndedPath path, TableFormat format, List<UnchangedColumn> columns, List<UnchangedColumn> partitionColumns,
                                 List<UnchangedPartitionValue> partitionValues, List<ColumnPartitionProjection> partitionProjections)
            implements DiffTable
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.NOT_MODIFIED;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "diffType")
    @JsonSubTypes({
            @JsonSubTypes.Type(name = "CREATE", value = NewColumn.class),
            @JsonSubTypes.Type(name = "CHANGE", value = UpdatedColumn.class),
            @JsonSubTypes.Type(name = "DROP", value = DroppedColumn.class),
            @JsonSubTypes.Type(name = "NOT_MODIFIED", value = UnchangedColumn.class)
    })
    public sealed interface DiffColumn
    {
        @JsonProperty("name")
        LowerCaseString name();

        @JsonProperty("type")
        HiveType type();

        @JsonProperty("sampleValue")
        Optional<String> sampleValue();

        @JsonProperty("diffType")
        DiffType diffType();
    }

    public record NewColumn(LowerCaseString name, HiveType type, Optional<String> sampleValue)
            implements DiffColumn
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.CREATE;
        }
    }

    public record UpdatedColumn(LowerCaseString name, HiveType type, LowerCaseString prevName, Optional<String> sampleValue)
            implements DiffColumn
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.CHANGE;
        }
    }

    public record DroppedColumn(LowerCaseString name, HiveType type, Optional<String> sampleValue)
            implements DiffColumn
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.DROP;
        }
    }

    public record UnchangedColumn(LowerCaseString name, HiveType type, Optional<String> sampleValue)
            implements DiffColumn
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.NOT_MODIFIED;
        }

        static List<UnchangedColumn> listFrom(List<Column> columns)
        {
            return columns.stream().map(col -> new UnchangedColumn(col.name(), col.type(), col.sampleValue())).collect(toImmutableList());
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "diffType")
    @JsonSubTypes({
            @JsonSubTypes.Type(name = "CREATE", value = NewPartitionValue.class),
            @JsonSubTypes.Type(name = "DROP", value = DroppedPartitionValue.class),
            @JsonSubTypes.Type(name = "NOT_MODIFIED", value = UnchangedPartitionValue.class)
    })
    public sealed interface DiffPartitionValue
    {
        @JsonProperty("path")
        SlashEndedPath path();

        @JsonProperty("values")
        Map<LowerCaseString, String> values();

        @JsonProperty("diffType")
        DiffType diffType();
    }

    public record NewPartitionValue(SlashEndedPath path, Map<LowerCaseString, String> values)
            implements DiffPartitionValue
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.CREATE;
        }
    }

    public record DroppedPartitionValue(SlashEndedPath path, Map<LowerCaseString, String> values)
            implements DiffPartitionValue
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.DROP;
        }
    }

    public record UnchangedPartitionValue(SlashEndedPath path, Map<LowerCaseString, String> values)
            implements DiffPartitionValue
    {
        @Override
        public DiffType diffType()
        {
            return DiffType.NOT_MODIFIED;
        }

        static List<UnchangedPartitionValue> listFrom(List<DiscoveredPartitionValues> partitionValues)
        {
            return partitionValues.stream().map(pValue -> new UnchangedPartitionValue(pValue.path(), pValue.values())).collect(toImmutableList());
        }
    }

    public record ColumnPartitionProjection(LowerCaseString columnName, InferredPartitionProjection partitionProjection)
    {}

    public static List<DiffTable> generateDiff(String rootPath, List<DiscoveredTable> previousTables, List<DiscoveredTable> currentTables)
    {
        TableChangesBuilder tableChangesBuilder = new TableChangesBuilder(ensureEndsWithSlash(rootPath));
        previousTables.forEach(tableChangesBuilder::addPreviousTable);
        currentTables.forEach(tableChangesBuilder::addCurrentTable);
        TableChanges tableChanges = tableChangesBuilder.build();

        ImmutableList.Builder<DiffTable> results = ImmutableList.builder();

        buildNewTablesDiff(tableChanges, results);
        buildDroppedTablesDiff(previousTables, tableChanges, results);

        buildUpdatedAndNotModifiedTablesDiff(previousTables, currentTables, tableChanges, results);

        buildPartitionProjectionRecreatedTables(previousTables, tableChanges, results);

        return results.build();
    }

    private static void buildPartitionProjectionRecreatedTables(List<DiscoveredTable> previousTables, TableChanges tableChanges, ImmutableList.Builder<DiffTable> results)
    {
        tableChanges.tablesToRecreateForProjectionChanges().forEach(recreatedTable -> {
            Optional<DiscoveredTable> previousTableMaybe = previousTables.stream().filter(prevTable -> prevTable.tableName().equals(recreatedTable.tableName())).findFirst();
            previousTableMaybe.ifPresent(previousTable -> {
                results.add(new RecreatedTable(
                        recreatedTable.tableName(),
                        recreatedTable.path(),
                        recreatedTable.format(),
                        recreatedTable.columns().columns().stream().map(c -> new NewColumn(c.name(), c.type(), c.sampleValue())).collect(toImmutableList()),
                        recreatedTable.discoveredPartitions().columns().stream().map(c -> new NewColumn(c.name(), c.type(), c.sampleValue())).collect(toImmutableList()),
                        recreatedTable.discoveredPartitions().values().stream().map(v -> new NewPartitionValue(v.path(), v.values())).collect(toImmutableList()),
                        buildColumnPartitionProjection(previousTable)));
            });
        });
    }

    private static void buildUpdatedAndNotModifiedTablesDiff(List<DiscoveredTable> previousTables, List<DiscoveredTable> currentTables, TableChanges tableChanges, ImmutableList.Builder<DiffTable> results)
    {
        Set<TableName> potentiallyModifiedTableNames = ImmutableSet.<TableName>builder()
                .addAll(tableChanges.columnChanges().keySet())
                .addAll(tableChanges.bucketChanges().keySet())
                .addAll(tableChanges.partitionColumnChanges().keySet())
                .addAll(tableChanges.partitionValueChanges().keySet())
                .build();
        potentiallyModifiedTableNames = Sets.difference(
                potentiallyModifiedTableNames,
                tableChanges.tablesToRecreateForProjectionChanges().stream().map(DiscoveredTable::tableName).collect(toImmutableSet()));

        potentiallyModifiedTableNames.forEach(potentiallyModifiedTableName -> {
            TableColumnChanges columnChanges = tableChanges.columnChanges().getOrDefault(potentiallyModifiedTableName, new TableColumnChanges(ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
            TableColumnChanges partitionColumnChanges = tableChanges.partitionColumnChanges().getOrDefault(potentiallyModifiedTableName, new TableColumnChanges(ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
            BucketChanges bucketChanges = tableChanges.bucketChanges().getOrDefault(potentiallyModifiedTableName, new BucketChanges(ImmutableList.of(), ImmutableList.of()));
            PartitionValueChanges partitionValueChanges = tableChanges.partitionValueChanges().getOrDefault(potentiallyModifiedTableName, new PartitionValueChanges(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));

            if (isUnchanged(columnChanges, partitionColumnChanges, bucketChanges, partitionValueChanges)) {
                Optional<DiscoveredTable> unmodifiedTableMaybe = currentTables.stream().filter(currentTable -> currentTable.tableName().equals(potentiallyModifiedTableName)).findFirst();
                unmodifiedTableMaybe.ifPresent(unmodifiedTable -> results.add(new UnchangedTable(
                        unmodifiedTable.tableName(),
                        unmodifiedTable.path(),
                        unmodifiedTable.format(),
                        UnchangedColumn.listFrom(unmodifiedTable.columns().columns()),
                        UnchangedColumn.listFrom(unmodifiedTable.discoveredPartitions().columns()),
                        UnchangedPartitionValue.listFrom(unmodifiedTable.discoveredPartitions().values()),
                        buildColumnPartitionProjection(unmodifiedTable))));
            }
            else {
                // we know table has been modified somehow, add all details both modified & unmodified
                Optional<DiscoveredTable> modifiedTableMaybe = currentTables.stream().filter(currentTable -> currentTable.tableName().equals(potentiallyModifiedTableName)).findFirst();
                Optional<DiscoveredTable> previousTableMaybe = previousTables.stream().filter(prevTable -> prevTable.tableName().equals(potentiallyModifiedTableName)).findFirst();

                if (modifiedTableMaybe.isPresent() && previousTableMaybe.isPresent()) {
                    DiscoveredTable modifiedTable = modifiedTableMaybe.get();
                    DiscoveredTable previousTable = previousTableMaybe.get();

                    List<DiffColumn> diffColumns = buildModifiedTableColumns(columnChanges, modifiedTable, previousTable, table -> table.columns().columns());
                    List<DiffColumn> diffPartitionColumns = buildModifiedTableColumns(partitionColumnChanges, modifiedTable, previousTable, table -> table.discoveredPartitions().columns());
                    List<DiffPartitionValue> diffPartitionValues = buildModifiedPartitionValues(partitionValueChanges, modifiedTable, previousTable);
                    results.add(new UpdatedTable(modifiedTable.tableName(),
                            modifiedTable.path(),
                            modifiedTable.format(),
                            diffColumns,
                            diffPartitionColumns,
                            diffPartitionValues,
                            buildColumnPartitionProjection(modifiedTable)));
                }
            }
        });
    }

    private static List<DiffPartitionValue> buildModifiedPartitionValues(PartitionValueChanges partitionValueChanges, DiscoveredTable modifiedTable, DiscoveredTable previousTable)
    {
        ImmutableList.Builder<DiffPartitionValue> partitionValueDiffBuilder = ImmutableList.builder();

        partitionValueChanges.addedPartitionValues().stream()
                .map(addedPartitionValue -> new NewPartitionValue(addedPartitionValue.path(), addedPartitionValue.values()))
                .forEach(partitionValueDiffBuilder::add);

        partitionValueChanges.droppedPartitionValues().stream()
                .map(droppedPartitionValue -> new DroppedPartitionValue(droppedPartitionValue.path(), droppedPartitionValue.values()))
                .forEach(partitionValueDiffBuilder::add);

        intersection(ImmutableSet.copyOf(modifiedTable.discoveredPartitions().values()),
                ImmutableSet.copyOf(previousTable.discoveredPartitions().values()))
                .stream()
                .map(unchangedPartitionValue -> new UnchangedPartitionValue(unchangedPartitionValue.path(), unchangedPartitionValue.values()))
                .forEach(partitionValueDiffBuilder::add);

        return partitionValueDiffBuilder.build();
    }

    private static List<DiffColumn> buildModifiedTableColumns(TableColumnChanges columnChanges, DiscoveredTable modifiedTable, DiscoveredTable previousTable, Function<DiscoveredTable, List<Column>> columnsAccessor)
    {
        ImmutableList.Builder<DiffColumn> columnDiffBuilder = ImmutableList.builder();
        Set<LowerCaseString> checkedColumnNames = new HashSet<>();

        columnChanges.addedColumns().forEach(addedColumn -> {
            checkedColumnNames.add(addedColumn.name());
            columnDiffBuilder.add(new NewColumn(addedColumn.name(), addedColumn.type(), addedColumn.sampleValue()));
        });

        columnChanges.droppedColumns().forEach(droppedColumn -> {
            checkedColumnNames.add(droppedColumn);
            columnsAccessor.apply(previousTable).stream()
                    .filter(previousTableColumn -> previousTableColumn.name().equals(droppedColumn))
                    .forEach(missingColumn -> columnDiffBuilder.add(new DroppedColumn(missingColumn.name(), missingColumn.type(), missingColumn.sampleValue())));
        });

        columnChanges.columnRenames().forEach(columnRename -> {
            checkedColumnNames.add(columnRename.newName());
            columnsAccessor.apply(previousTable).stream()
                    .filter(previousTableColumn -> previousTableColumn.name().equals(columnRename.oldName()))
                    .forEach(columnBeforeRename -> columnDiffBuilder.add(new UpdatedColumn(
                            columnRename.newName(),
                            columnBeforeRename.type(),
                            columnBeforeRename.name(),
                            columnBeforeRename.sampleValue())));
        });

        columnsAccessor.apply(modifiedTable).stream()
                .filter(currentColumn -> !checkedColumnNames.contains(currentColumn.name()))
                .forEach(unmodifiedColumn -> columnDiffBuilder.add(new UnchangedColumn(unmodifiedColumn.name(), unmodifiedColumn.type(), unmodifiedColumn.sampleValue())));

        return columnDiffBuilder.build();
    }

    private static boolean isUnchanged(TableColumnChanges columnChanges, TableColumnChanges partitionColumnChanges, BucketChanges bucketChanges, PartitionValueChanges partitionValueChanges)
    {
        return columnChanges.addedColumns().isEmpty() && columnChanges.droppedColumns().isEmpty() && columnChanges.columnRenames().isEmpty() &&
               partitionColumnChanges.addedColumns().isEmpty() && partitionColumnChanges.droppedColumns().isEmpty() && partitionColumnChanges.columnRenames().isEmpty() &&
               bucketChanges.addedBuckets().isEmpty() && bucketChanges.droppedBuckets().isEmpty() &&
               partitionValueChanges.addedPartitionValues().isEmpty() && partitionValueChanges.droppedPartitionValues().isEmpty();
    }

    private static void buildDroppedTablesDiff(List<DiscoveredTable> previousTables, TableChanges tableChanges, ImmutableList.Builder<DiffTable> results)
    {
        tableChanges.droppedTables().forEach(droppedTable -> {
            Optional<DiscoveredTable> droppedTableDetailsMaybe = previousTables.stream()
                    .filter(DiscoveredTable::valid)
                    .filter(prevTable -> prevTable.tableName().equals(droppedTable.tableName()))
                    .findFirst();
            droppedTableDetailsMaybe.ifPresent(droppedTableDetails -> results.add(new DroppedTable(
                    droppedTableDetails.tableName(),
                    droppedTableDetails.path(),
                    droppedTableDetails.format(),
                    droppedTableDetails.columns().columns().stream().map(droppedColumn ->
                            new DroppedColumn(droppedColumn.name(), droppedColumn.type(), droppedColumn.sampleValue())).collect(toImmutableList()),
                    droppedTableDetails.discoveredPartitions().columns().stream().map(droppedPartitionColumn ->
                            new DroppedColumn(droppedPartitionColumn.name(), droppedPartitionColumn.type(), droppedPartitionColumn.sampleValue())).collect(toImmutableList()),
                    droppedTableDetails.discoveredPartitions().values().stream().map(droppedPartitionValue ->
                            new DroppedPartitionValue(droppedPartitionValue.path(), droppedPartitionValue.values())).collect(toImmutableList()),
                    buildColumnPartitionProjection(droppedTableDetails))));
        });
    }

    private static void buildNewTablesDiff(TableChanges tableChanges, ImmutableList.Builder<DiffTable> results)
    {
        tableChanges.addedTables().forEach(addedTable -> results.add(new NewTable(
                addedTable.tableName(),
                addedTable.path(),
                addedTable.format(),
                addedTable.columns().columns().stream().map(newColumn ->
                        new NewColumn(newColumn.name(), newColumn.type(), newColumn.sampleValue())).collect(toImmutableList()),
                addedTable.discoveredPartitions().columns().stream().map(newPartitionColumn ->
                        new NewColumn(newPartitionColumn.name(), newPartitionColumn.type(), newPartitionColumn.sampleValue())).collect(toImmutableList()),
                addedTable.discoveredPartitions().values().stream().map(addedPartitionValue ->
                        new NewPartitionValue(addedPartitionValue.path(), addedPartitionValue.values())).collect(toImmutableList()),
                buildColumnPartitionProjection(addedTable))));
    }

    private static List<ColumnPartitionProjection> buildColumnPartitionProjection(DiscoveredTable table)
    {
        if (!table.hasAnyProjectedPartition()) {
            return ImmutableList.of();
        }
        return table.discoveredPartitions().columns().stream()
                .map(c -> new ColumnPartitionProjection(c.name(), table.discoveredPartitions().columnProjections().get(c.name())))
                .collect(toImmutableList());
    }
}
