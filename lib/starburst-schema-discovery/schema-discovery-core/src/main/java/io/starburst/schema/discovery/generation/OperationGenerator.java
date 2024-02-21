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

import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.TableChanges;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.Operation;
import io.starburst.schema.discovery.models.Operation.AddBucket;
import io.starburst.schema.discovery.models.Operation.AddColumn;
import io.starburst.schema.discovery.models.Operation.AddPartitionColumn;
import io.starburst.schema.discovery.models.Operation.AddPartitionValue;
import io.starburst.schema.discovery.models.Operation.Comment;
import io.starburst.schema.discovery.models.Operation.CreateSchema;
import io.starburst.schema.discovery.models.Operation.CreateTable;
import io.starburst.schema.discovery.models.Operation.DropBucket;
import io.starburst.schema.discovery.models.Operation.DropColumn;
import io.starburst.schema.discovery.models.Operation.DropPartitionColumn;
import io.starburst.schema.discovery.models.Operation.DropPartitionValue;
import io.starburst.schema.discovery.models.Operation.DropTable;
import io.starburst.schema.discovery.models.Operation.RegisterTable;
import io.starburst.schema.discovery.models.Operation.RenameColumn;
import io.starburst.schema.discovery.models.Operation.RenamePartitionColumn;
import io.starburst.schema.discovery.models.Operation.UnregisterTable;
import io.starburst.schema.discovery.models.SlashEndedPath;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.trino.filesystem.Location;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static java.util.Objects.requireNonNull;

public class OperationGenerator
{
    private final GenerateOptions options;
    private final Dialect dialect;

    public OperationGenerator(GenerateOptions options, Dialect dialect)
    {
        this.options = requireNonNull(options, "options is null");
        this.dialect = requireNonNull(dialect, "dialect is null");
    }

    public List<Operation> generateInitial(DiscoveredSchema schema)
    {
        ImmutableList.Builder<Operation> builder = ImmutableList.builder();
        addSchemaCreates(builder, schema.rootPath(), schema.tables());
        addTableCreates(builder, schema.tables());
        return builder.build();
    }

    public List<Operation> generateChanges(TableChanges tableChanges)
    {
        ImmutableList.Builder<Operation> builder = ImmutableList.builder();
        tableChanges.errorsPerPath().forEach((key, value) -> {
            builder.add(new Comment("Issues found when scanning path: [%s]".formatted(key)));
            value.forEach(error -> builder.add(new Comment(error)));
        });
        tableChanges.addedSchema().forEach(schemaName -> builder.add(new CreateSchema(mergeRootAndSchema(tableChanges.rootPath(), schemaName), schemaName)));
        addTableDrops(builder, tableChanges.droppedTables());
        addTableCreates(builder, tableChanges.addedTables());
        tableChanges.columnChanges().forEach((tableName, tableColumnChanges) -> {
            tableColumnChanges.droppedColumns().forEach(columnName -> builder.add(new DropColumn(tableName, columnName)));
            tableColumnChanges.addedColumns().forEach(column -> builder.add(new AddColumn(tableName, column)));
            tableColumnChanges.columnRenames().forEach(rename -> builder.add(new RenameColumn(tableName, rename.oldName(), rename.newName())));
        });
        if (options.includePartitions()) {
            tableChanges.tablesToRecreateForProjectionChanges().forEach(tableToRecreate -> {
                addTableDrops(builder, ImmutableList.of(tableToRecreate));
                addTableCreates(builder, ImmutableList.of(tableToRecreate));
            });

            tableChanges.bucketChanges().forEach((tableName, bucketChanges) -> {
                bucketChanges.droppedBuckets().forEach(bucketName -> builder.add(new DropBucket(tableName, bucketName)));
                bucketChanges.addedBuckets().forEach(bucketName -> builder.add(new AddBucket(tableName, bucketName)));
            });

            tableChanges.partitionColumnChanges().forEach((tableName, tableColumnChanges) -> {
                tableColumnChanges.droppedColumns().forEach(columnName -> builder.add(new DropPartitionColumn(tableName, columnName)));
                tableColumnChanges.addedColumns().forEach(column -> builder.add(new AddPartitionColumn(tableName, column)));
                tableColumnChanges.columnRenames().forEach(rename -> builder.add(new RenamePartitionColumn(tableName, rename.oldName(), rename.newName())));
            });

            tableChanges.partitionValueChanges().forEach((tableName, partitionChanges) -> {
                partitionChanges.droppedPartitionValues().forEach(partitionValues -> builder.add(new DropPartitionValue(tableName, partitionChanges.oldPartitionColumns(), partitionValues)));
                partitionChanges.addedPartitionValues().forEach(partitionValues -> builder.add(new AddPartitionValue(tableName, partitionChanges.newPartitionColumns(), partitionValues)));
            });
        }
        else if (!tableChanges.bucketChanges().isEmpty() || !tableChanges.partitionColumnChanges().isEmpty() || !tableChanges.partitionValueChanges().isEmpty()) {
            builder.add(new Comment("Partition/bucket changes found but partition creation disabled"));
        }

        if (tableChanges.droppedTables().isEmpty()
                && tableChanges.addedTables().isEmpty()
                && isEmpty(tableChanges.columnChanges(), TableChanges.TableColumnChanges::columnRenames, TableChanges.TableColumnChanges::addedColumns, TableChanges.TableColumnChanges::droppedColumns)
                && isEmpty(tableChanges.bucketChanges(), TableChanges.BucketChanges::addedBuckets, TableChanges.BucketChanges::droppedBuckets)
                && isEmpty(tableChanges.partitionColumnChanges(), TableChanges.TableColumnChanges::columnRenames, TableChanges.TableColumnChanges::addedColumns, TableChanges.TableColumnChanges::droppedColumns)
                && isEmpty(tableChanges.partitionValueChanges(), TableChanges.PartitionValueChanges::addedPartitionValues, TableChanges.PartitionValueChanges::droppedPartitionValues)) {
            builder.add(new Comment("No changes were found"));
        }
        return builder.build();
    }

    public List<String> generateSql(String defaultSchemaName, List<Operation> operations)
    {
        return operations.stream()
                .map(operation -> {
                    SimpleWriter writer = new SimpleWriter();
                    SqlGenerator sqlGenerator = new SqlGenerator(toLowerCase(defaultSchemaName), options, writer, dialect);
                    sqlGenerator.apply(operation);
                    return writer.toString();
                })
                .collect(toImmutableList());
    }

    @SafeVarargs
    private <T> boolean isEmpty(Map<?, T> map, Function<T, Collection<?>>... accessors)
    {
        requireNonNull(accessors, "accessors cannot be null");
        return map.values().stream()
                .allMatch(value -> Stream.of(accessors).allMatch(accessor -> accessor.apply(value).isEmpty()));
    }

    private void addSchemaCreates(ImmutableList.Builder<Operation> builder, SlashEndedPath rootPath, List<DiscoveredTable> tables)
    {
        tables.stream()
                .filter(DiscoveredTable::valid)
                .map(discoveredTable -> discoveredTable.tableName().schemaName()
                        .map(schemaName -> new CreateSchema(mergeRootAndSchema(rootPath, schemaName), schemaName))
                        .orElseGet(() -> new CreateSchema(rootPath, toLowerCase(options.defaultSchemaName()))))
                .distinct()
                .forEach(builder::add);
    }

    private SlashEndedPath mergeRootAndSchema(SlashEndedPath rootPath, LowerCaseString schemaName)
    {
        return ensureEndsWithSlash(Location.of(rootPath.toString()).appendPath(schemaName.string()).toString());
    }

    private void addTableCreates(ImmutableList.Builder<Operation> builder, List<DiscoveredTable> tablesToAdd)
    {
        tablesToAdd.forEach(table -> {
            if (table.valid()) {
                switch (table.format()) {
                    case ICEBERG, DELTA_LAKE -> builder.add(new RegisterTable(table.path(), table.tableName()));
                    default -> {
                        builder.add(new CreateTable(table));
                        if ((!table.discoveredPartitions().columns().isEmpty() || !table.buckets().isEmpty()) && !options.includePartitions()) {
                            builder.add(new Comment("Partition(s)/bucket(s) found but partition creation disabled"));
                        }
                    }
                }
            }
            else {
                builder.add(new Comment("Skipping invalid table: " + table.tableName()));
            }
        });
    }

    private static void addTableDrops(ImmutableList.Builder<Operation> builder, Collection<DiscoveredTable> tablesToDrop)
    {
        tablesToDrop.forEach(droppedTable -> {
            switch (droppedTable.format()) {
                case ICEBERG, DELTA_LAKE -> builder.add(new UnregisterTable(droppedTable.tableName()));
                case ERROR -> builder.add(new Comment("Table [%s] was previously invalid, nothing to drop.".formatted(droppedTable.tableName())));
                default -> builder.add(new DropTable(droppedTable.tableName()));
            }
        });
    }
}
