/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.SlashEndedPath;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static java.util.Objects.requireNonNull;

public record TableChanges(SlashEndedPath rootPath,
                           Collection<LowerCaseString> addedSchema,
                           Collection<DiscoveredTable> droppedTables,
                           List<DiscoveredTable> addedTables,
                           List<DiscoveredTable> tablesToRecreateForProjectionChanges,
                           Map<TableName, TableColumnChanges> columnChanges,
                           Map<TableName, TableColumnChanges> partitionColumnChanges,
                           Map<TableName, PartitionValueChanges> partitionValueChanges,
                           Map<TableName, BucketChanges> bucketChanges,
                           Map<String, List<String>> errorsPerPath)
{
    public TableChanges
    {
        requireNonNull(rootPath, "rootPath is null");
        addedSchema = ImmutableSet.copyOf(addedSchema);
        droppedTables = ImmutableSet.copyOf(droppedTables);
        addedTables = ImmutableList.copyOf(addedTables);
        tablesToRecreateForProjectionChanges = ImmutableList.copyOf(tablesToRecreateForProjectionChanges);
        columnChanges = ImmutableMap.copyOf(columnChanges);
        partitionColumnChanges = ImmutableMap.copyOf(partitionColumnChanges);
        partitionValueChanges = ImmutableMap.copyOf(partitionValueChanges);
        bucketChanges = ImmutableMap.copyOf(bucketChanges);
        errorsPerPath = ImmutableMap.copyOf(errorsPerPath);
    }

    public record TableName(Optional<LowerCaseString> schemaName, LowerCaseString tableName)
    {
        public TableName
        {
            requireNonNull(schemaName, "schemaName cannot be null");
            requireNonNull(tableName, "tableName cannot be null");
            checkArgument(!isNullOrEmpty(tableName.toString()), "tableName cannot be empty");
        }

        public String toStringQuoted(Optional<String> catalogNameMaybe, LowerCaseString defaultSchemaName)
        {
            return catalogNameMaybe.map(catalogName -> "\"%s\".\"%s\".\"%s\"".formatted(catalogName, schemaName.orElse(defaultSchemaName), tableName))
                    .orElseGet(() -> toStringQuoted(defaultSchemaName));
        }

        public String toString(Optional<String> catalogNameMaybe, LowerCaseString defaultSchemaName)
        {
            return catalogNameMaybe.map(catalogName -> "%s.%s.%s".formatted(catalogName, schemaName.orElse(defaultSchemaName), tableName))
                    .orElseGet(() -> toString(defaultSchemaName));
        }

        public String toString(LowerCaseString defaultSchemaName)
        {
            return "%s.%s".formatted(schemaName.orElse(defaultSchemaName), tableName);
        }

        public String toStringQuoted(LowerCaseString defaultSchemaName)
        {
            return "\"%s\".\"%s\"".formatted(schemaName.orElse(defaultSchemaName), tableName);
        }

        @Override
        public String toString()
        {
            return schemaName.map(__ -> toString(toLowerCase("dummy"))).orElse(tableName.toString());
        }
    }

    public record ColumnRename(LowerCaseString oldName, LowerCaseString newName)
    {
        public ColumnRename
        {
            requireNonNull(oldName, "oldName cannot be null");
            requireNonNull(newName, "newName cannot be null");
        }
    }

    public record TableColumnChanges(List<ColumnRename> columnRenames,
                                     List<Column> addedColumns, Collection<LowerCaseString> droppedColumns) {}

    public record BucketChanges(Collection<LowerCaseString> droppedBuckets, Collection<LowerCaseString> addedBuckets) {}

    public record PartitionValueChanges(List<Column> oldPartitionColumns,
                                        List<Column> newPartitionColumns,
                                        List<DiscoveredPartitionValues> droppedPartitionValues,
                                        List<DiscoveredPartitionValues> addedPartitionValues) {}
}
