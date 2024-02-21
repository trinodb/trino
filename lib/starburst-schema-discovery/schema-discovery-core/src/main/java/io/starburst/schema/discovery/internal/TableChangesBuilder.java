/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.TableChanges.BucketChanges;
import io.starburst.schema.discovery.TableChanges.PartitionValueChanges;
import io.starburst.schema.discovery.TableChanges.TableColumnChanges;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredPartitions.IntegerProjectionMinMaxRange;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.SlashEndedPath;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class TableChangesBuilder
{
    private final Errors errors = new Errors();
    private final Map<TableName, DiscoveredTable> previousTables = new HashMap<>();
    private final Map<TableName, DiscoveredTable> currentTables = new HashMap<>();
    private final SlashEndedPath rootPath;

    public TableChangesBuilder(SlashEndedPath rootPath)
    {
        this.rootPath = requireNonNull(rootPath, "rootPath is null");
    }

    public void addPreviousTable(DiscoveredTable table)
    {
        TableName tableName = table.tableName();
        checkArgument(!previousTables.containsKey(tableName), "Previous table has already been added: " + tableName);
        previousTables.put(tableName, table);
    }

    public void addCurrentTable(DiscoveredTable table)
    {
        TableName tableName = table.tableName();
        checkArgument(!currentTables.containsKey(tableName), "Current table has already been added: " + tableName);
        currentTables.put(tableName, table);
    }

    public TableChanges build()
    {
        SetView<LowerCaseString> schemaToAdd = Sets.difference(schemaNames(currentTables), schemaNames(previousTables));
        Map<TableName, DiscoveredTable> tablesToDrop = difference(previousTables, currentTables);
        Map<TableName, DiscoveredTable> tablesToAdd = difference(currentTables, previousTables);

        Set<TableName> remainingTables = new HashSet<>(previousTables.keySet());
        remainingTables.retainAll(currentTables.keySet());
        remainingTables = filterConflicting(remainingTables);

        Map<TableName, DiscoveredTable> tablesToRecreateForProjectionChanges = buildProjectedPartitionChanges(remainingTables);
        remainingTables = Sets.difference(remainingTables, tablesToRecreateForProjectionChanges.keySet());

        Map<TableName, BucketChanges> bucketChanges = buildBucketChanges(remainingTables);
        Map<TableName, TableColumnChanges> columnChanges = buildColumnChanges(remainingTables);
        Map<TableName, TableColumnChanges> partitionColumnChanges = buildPartitionColumnChangesForNonProjected(remainingTables);
        Map<TableName, PartitionValueChanges> partitionValueChanges = buildPartitionValueChangesForNonProjected(remainingTables);

        return new TableChanges(
                rootPath,
                schemaToAdd,
                tablesToDrop.values().stream().filter(DiscoveredTable::valid).collect(toImmutableList()),
                ImmutableList.copyOf(tablesToAdd.values()),
                ImmutableList.copyOf(tablesToRecreateForProjectionChanges.values()),
                columnChanges,
                partitionColumnChanges,
                partitionValueChanges,
                bucketChanges,
                errors.buildPathErrors());
    }

    private Set<LowerCaseString> schemaNames(Map<TableName, DiscoveredTable> tables)
    {
        return tables.values().stream().flatMap(table -> table.tableName().schemaName().stream()).collect(toImmutableSet());
    }

    private Map<TableName, PartitionValueChanges> buildPartitionValueChangesForNonProjected(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tableName -> !currentTables.get(tableName).hasAnyProjectedPartition())
                .flatMap(tableName -> {
                    DiscoveredPartitions previousPartitions = previousTables.get(tableName).discoveredPartitions();
                    DiscoveredPartitions currentPartitions = currentTables.get(tableName).discoveredPartitions();
                    ImmutableSet<DiscoveredPartitionValues> previousPartitionValues = ImmutableSet.copyOf(previousPartitions.values());
                    ImmutableSet<DiscoveredPartitionValues> currentPartitionValues = ImmutableSet.copyOf(currentPartitions.values());
                    Set<DiscoveredPartitionValues> droppedPartitionValues = Sets.difference(previousPartitionValues, currentPartitionValues);
                    Set<DiscoveredPartitionValues> addedPartitionValues = Sets.difference(currentPartitionValues, previousPartitionValues);
                    if (droppedPartitionValues.isEmpty() && addedPartitionValues.isEmpty()) {
                        return Optional.<Entry<TableName, PartitionValueChanges>>empty().stream();
                    }
                    SimpleEntry<TableName, PartitionValueChanges> entry = new SimpleEntry<>(tableName, new PartitionValueChanges(previousPartitions.columns(), currentPartitions.columns(), ImmutableList.copyOf(droppedPartitionValues), ImmutableList.copyOf(addedPartitionValues)));
                    return Optional.of(entry).stream();
                })
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
    }

    private Map<TableName, BucketChanges> buildBucketChanges(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .flatMap(tableName -> {
                    ImmutableSet<LowerCaseString> previousBuckets = ImmutableSet.copyOf(previousTables.get(tableName).buckets());
                    ImmutableSet<LowerCaseString> currentBuckets = ImmutableSet.copyOf(currentTables.get(tableName).buckets());
                    Sets.SetView<LowerCaseString> droppedBuckets = Sets.difference(previousBuckets, currentBuckets);
                    Sets.SetView<LowerCaseString> addedBuckets = Sets.difference(currentBuckets, previousBuckets);
                    if (droppedBuckets.isEmpty() && addedBuckets.isEmpty()) {
                        return Optional.<Map.Entry<TableName, BucketChanges>>empty().stream();
                    }
                    AbstractMap.SimpleEntry<TableName, BucketChanges> entry = new AbstractMap.SimpleEntry<>(tableName, new BucketChanges(droppedBuckets, addedBuckets));
                    return Optional.of(entry).stream();
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<TableName, DiscoveredTable> buildProjectedPartitionChanges(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tableName -> currentTables.get(tableName).hasAnyProjectedPartition())
                .flatMap(tableName -> {
                    DiscoveredPartitions previousPartitions = previousTables.get(tableName).discoveredPartitions();
                    DiscoveredPartitions currentPartitions = currentTables.get(tableName).discoveredPartitions();

                    if (!previousPartitions.columns().equals(currentPartitions.columns()) ||
                            !previousPartitions.columnProjections().equals(currentPartitions.columnProjections())) {
                        return Optional.of(getTableAsEntry(tableName)).stream();
                    }

                    boolean areAllProjectionsInMatch = currentPartitions.columns().stream()
                            .allMatch(column -> {
                                InferredPartitionProjection previousProjection = previousPartitions.columnProjections().get(column.name());
                                InferredPartitionProjection currentProjection = currentPartitions.columnProjections().get(column.name());
                                if (!currentProjection.equals(previousProjection)) {
                                    return false;
                                }
                                return switch (currentProjection.projectionType()) {
                                    case INTEGER -> {
                                        IntegerProjectionMinMaxRange previousIntegerProjection = currentPartitions.computeIntegerProjectionRange(column.name());
                                        IntegerProjectionMinMaxRange currentIntegerProjection = previousPartitions.computeIntegerProjectionRange(column.name());
                                        yield currentIntegerProjection.equals(previousIntegerProjection);
                                    }
                                    case ENUM -> {
                                        String previousEnumProjection = currentPartitions.computeEnumProjectionPossibleValues(column.name());
                                        String currentEnumProjection = previousPartitions.computeEnumProjectionPossibleValues(column.name());
                                        yield currentEnumProjection.equals(previousEnumProjection);
                                    }
                                    case DATE -> throw new RuntimeException("Date partition projection type is not supported");
                                    case INJECTED -> true;
                                };
                            });
                    if (!areAllProjectionsInMatch) {
                        return Optional.of(getTableAsEntry(tableName)).stream();
                    }

                    return Optional.<Map.Entry<TableName, DiscoveredTable>>empty().stream();
                })
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private AbstractMap.SimpleEntry<TableName, DiscoveredTable> getTableAsEntry(TableName tableName)
    {
        return new AbstractMap.SimpleEntry<>(tableName, currentTables.get(tableName));
    }

    // even if table names are equal, we should consider validity, to be able to heal tables
    // which were errored previously, but now are fixed
    private Map<TableName, DiscoveredTable> difference(Map<TableName, DiscoveredTable> t1, Map<TableName, DiscoveredTable> t2)
    {
        Map<TableNameAndValidity, DiscoveredTable> t1IdentifierMap = t1.entrySet()
                .stream()
                .collect(toImmutableMap(e -> new TableNameAndValidity(e.getKey(), e.getValue().valid()), Entry::getValue));
        Map<TableNameAndValidity, DiscoveredTable> t2IdentifierMap = t2.entrySet()
                .stream()
                .collect(toImmutableMap(e -> new TableNameAndValidity(e.getKey(), e.getValue().valid()), Entry::getValue));

        Map<TableNameAndValidity, DiscoveredTable> temp = new HashMap<>(t1IdentifierMap);
        temp.keySet().removeAll(t2IdentifierMap.keySet());

        return temp.entrySet().stream().collect(toImmutableMap(e -> e.getKey().tableName(), Entry::getValue));
    }

    private Map<TableName, TableColumnChanges> buildColumnChanges(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .collect(toImmutableMap(identity(), tableName -> columnChangesForTable(tableName, discoveredTable -> discoveredTable.columns().columns())));
    }

    private Map<TableName, TableColumnChanges> buildPartitionColumnChangesForNonProjected(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .filter(tableName -> !currentTables.get(tableName).hasAnyProjectedPartition())
                .collect(toImmutableMap(identity(), tableName -> columnChangesForTable(tableName, discoveredTable -> discoveredTable.discoveredPartitions().columns())));
    }

    private TableColumnChanges columnChangesForTable(TableName tableName, Function<DiscoveredTable, List<Column>> accessor)
    {
        ImmutableList.Builder<Column> columnsToAdd = ImmutableList.builder();
        ImmutableSet.Builder<LowerCaseString> columnsToDrop = ImmutableSet.builder();
        ImmutableList.Builder<TableChanges.ColumnRename> columnRenames = ImmutableList.builder();
        List<Column> previousColumns = accessor.apply(previousTables.get(tableName));
        List<Column> currentColumns = accessor.apply(currentTables.get(tableName));
        for (int i = 0; i < Math.max(previousColumns.size(), currentColumns.size()); ++i) {
            Column previousColumn = (i < previousColumns.size()) ? previousColumns.get(i) : null;
            Column currentColumn = (i < currentColumns.size()) ? currentColumns.get(i) : null;

            if (previousColumn == null) {
                if (currentColumn != null) {
                    columnsToAdd.add(currentColumn);
                }
            }
            else if (currentColumn == null) {
                columnsToDrop.add(previousColumn.name());
            }
            else {
                boolean hasSameType = previousColumn.type().equals(currentColumn.type());
                boolean hasSameName = previousColumn.name().equals(currentColumn.name());
                if (!hasSameType) {
                    columnsToDrop.add(previousColumn.name());
                    columnsToAdd.add(currentColumn);
                }
                else if (!hasSameName) {
                    columnRenames.add(new TableChanges.ColumnRename(previousColumn.name(), currentColumn.name()));
                }
            }
        }
        return new TableColumnChanges(columnRenames.build(), columnsToAdd.build(), columnsToDrop.build());
    }

    private String lcase(String s)
    {
        return s.toLowerCase(Locale.getDefault());
    }

    private Set<TableName> filterConflicting(Collection<TableName> remainingTables)
    {
        return remainingTables.stream()
                .filter(this::hasNoConflicts)
                .collect(toImmutableSet());
    }

    private boolean hasNoConflicts(TableName tableName)
    {
        DiscoveredTable previousTable = previousTables.get(tableName);
        DiscoveredTable currentTable = currentTables.get(tableName);
        boolean hasNoConflicts = true;

        if (!previousTable.valid()) {
            errors.addTableError(currentTable.path(), "Previous table [%s] is invalid and will be ignored.", previousTable.tableName());
            hasNoConflicts = false;
        }
        if (!currentTable.valid()) {
            errors.addTableError(currentTable.path(), "Current table [%s] is invalid and will be ignored.", currentTable.tableName());
            hasNoConflicts = false;
        }
        if (!previousTable.format().equals(currentTable.format())) {
            errors.addTableError(currentTable.path(), "Format change in table [%s]. Previous: [%s] Current: [%s]. Table will be ignored.", tableName, previousTable.format(), currentTable.format());
            hasNoConflicts = false;
        }
        if (!previousTable.path().equals(currentTable.path())) {
            errors.addTableError(currentTable.path(), "Path change in table [%s]. Previous: [%s] Current: [%s]. Table will be ignored.", tableName, previousTable.path(), currentTable.path());
            hasNoConflicts = false;
        }

        return hasNoConflicts;
    }

    private record TableNameAndValidity(TableName tableName, boolean isValid)
    {
        public TableNameAndValidity
        {
            requireNonNull(tableName, "tableName is null");
        }
    }
}
