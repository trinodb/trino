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

import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.formats.csv.CsvFlags;
import io.starburst.schema.discovery.formats.csv.CsvOptions;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredPartitions.IntegerProjectionMinMaxRange;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.starburst.schema.discovery.models.Operation;
import io.starburst.schema.discovery.models.SlashEndedPath;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.models.TablePath;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.trino.plugin.hive.procedure.SyncPartitionMetadataProcedure;

import java.util.List;
import java.util.stream.Collectors;

import static io.starburst.schema.discovery.infer.InferPartitions.PARTITION_SEPARATOR;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("FormatStringAnnotation")
class SqlGenerator
        extends OperationsHandler<SqlGenerator>
{
    private final LowerCaseString schemaName;
    private final GenerateOptions options;
    private final SimpleWriter writer;
    private final Dialect dialect;

    SqlGenerator(LowerCaseString defaultSchemaName, GenerateOptions options, SimpleWriter writer, Dialect dialect)
    {
        this.schemaName = requireNonNull(defaultSchemaName, "defaultSchemaName is null");
        this.options = requireNonNull(options, "options is null");
        this.writer = requireNonNull(writer, "writer is null");
        this.dialect = requireNonNull(dialect, "dialect is null");
    }

    @Override
    protected void createSchema(Operation.CreateSchema createSchema)
    {
        if (options.catalogName().isPresent()) {
            writer.println("CREATE SCHEMA IF NOT EXISTS \"%s\".\"%s\" WITH (location = '%s');", options.catalogName().get(), createSchema.schemaName(), createSchema.rootUri());
        }
        else {
            writer.println("CREATE SCHEMA IF NOT EXISTS \"%s\" WITH (location = '%s');", createSchema.schemaName(), createSchema.rootUri());
        }
        writer.println();
    }

    @Override
    protected void generateComment(Operation.Comment comment)
    {
        writer.println("-- %s", comment.comment());
        writer.println();
    }

    @Override
    protected void dropTable(Operation.DropTable dropTable)
    {
        writer.println("DROP TABLE %s;", dropTable.tableName().toStringQuoted(options.catalogName(), schemaName));
        writer.println();
    }

    @Override
    protected void registerTable(Operation.RegisterTable registerTable)
    {
        String procedure = options.catalogName().map("CALL %s.system.register_table"::formatted).orElse("CALL system.register_table");
        writer.println("%s(schema_name => '%s', table_name => '%s', table_location => '%s');", procedure, registerTable.tableName().schemaName().orElse(schemaName), registerTable.tableName().tableName(), registerTable.path());
        writer.println();
    }

    @Override
    protected void unregisterTable(Operation.UnregisterTable unregisterTable)
    {
        String procedure = options.catalogName().map("CALL %s.system.unregister_table"::formatted).orElse("CALL system.unregister_table");
        writer.println("%s(schema_name => '%s', table_name => '%s');", procedure, unregisterTable.tableName().schemaName().orElse(schemaName), unregisterTable.tableName().tableName());
        writer.println();
    }

    @Override
    protected void createTable(Operation.CreateTable createTable)
    {
        DiscoveredTable table = createTable.table();
        String csvSerde = Mapping.tableFormat(table);
        boolean isCsvSerde = csvSerde.equals(TableFormat.CSV.name());
        boolean isPartitionProjection = table.hasAnyProjectedPartition();
        if (options.catalogName().isPresent()) {
            writer.println("CREATE TABLE \"%s\".\"%s\".\"%s\" (", options.catalogName().get(), table.tableName().schemaName().orElse(schemaName), table.tableName().tableName());
        }
        else {
            writer.println("USE \"%s\";", table.tableName().schemaName().orElse(schemaName));
            writer.println("CREATE TABLE \"%s\" (", table.tableName().tableName());
        }
        writer.indent();
        writer.startList();
        for (int i = 0; i < table.columns().columns().size(); ++i) {
            Column column = table.columns().columns().get(i);
            String columnType = isCsvSerde ? "varchar" : Mapping.columnType(column);
            writer.addToList("\"%s\" %s", column.name(), columnType);
        }
        DiscoveredPartitions tablePartitions = table.discoveredPartitions();
        for (int i = 0; i < tablePartitions.columns().size(); ++i) {
            Column column = tablePartitions.columns().get(i);
            writer.addToList("\"%s\" %s", column.name(), Mapping.columnType(column));

            if (isPartitionProjection) {
                addPartitionProjectionColumns(tablePartitions, column);
            }
        }
        writer.endList();
        writer.outdent();
        writer.println(")");
        writer.println("WITH (");
        writer.indent();
        writer.startList();
        if (dialect == Dialect.GALAXY) {
            writer.addToList("type = 'hive'");
        }
        writer.addToList("format = '%s'", csvSerde);
        if (table.format() == TableFormat.CSV) {
            addCsvRowFormats(table);
        }
        writer.addToList("external_location = '%s'", table.path());
        if (!tablePartitions.columns().isEmpty() && options.includePartitions()) {
            if (isPartitionProjection) {
                writer.addToList("partition_projection_enabled = true");
                addProjectionLocationTemplate(table, tablePartitions);
            }
            writer.addToList("partitioned_by = ARRAY[%s]", Mapping.commaList(tablePartitions.columns().stream(), c -> c.name().toString()));
        }
        if (!table.buckets().isEmpty() && options.includePartitions()) {
            writer.addToList("bucketed_by = ARRAY[%s]", Mapping.commaList(table.buckets().stream(), LowerCaseString::string));
            writer.addToList("bucket_count = %d", options.bucketQty());
        }
        writer.endList();
        writer.outdent();
        writer.println(");");

        if (!tablePartitions.columns().isEmpty() && options.includePartitions() && !isPartitionProjection) {
            writer.println();
            addPartitions(table);
        }
        writer.println();
    }

    private void addProjectionLocationTemplate(DiscoveredTable table, DiscoveredPartitions tablePartitions)
    {
        TablePath projectionTemplate = table.path();
        for (Column column : tablePartitions.columns()) {
            InferredPartitionProjection partitionProjection = tablePartitions.columnProjections().getOrDefault(column.name(), InferredPartitionProjection.NON_PROJECTED_PARTITION);
            String partitionPathPart = partitionProjection.isEqualSignSeparatedPartition() ?
                    column.name() + PARTITION_SEPARATOR + "${%s}".formatted(column.name()) :
                    "${%s}".formatted(column.name());
            projectionTemplate = SlashEndedPath.ensureEndsWithSlash(projectionTemplate.path() + partitionPathPart);
        }

        writer.addToList("partition_projection_location_template = '%s'", projectionTemplate);
    }

    private void addPartitionProjectionColumns(DiscoveredPartitions tablePartitions, Column column)
    {
        InferredPartitionProjection columnPartitionProjection = tablePartitions.columnProjections().getOrDefault(column.name(), InferredPartitionProjection.NON_PROJECTED_PARTITION);
        writer.println(" WITH (");
        writer.indent();
        writer.startList();
        writer.addToList("partition_projection_type = '%s'", columnPartitionProjection.projectionType());
        switch (columnPartitionProjection.projectionType()) {
            case INTEGER -> {
                IntegerProjectionMinMaxRange integerProjectionMinMaxRange = tablePartitions.computeIntegerProjectionRange(column.name());
                writer.addToList("partition_projection_range = ARRAY['%s', '%s']", integerProjectionMinMaxRange.min(), integerProjectionMinMaxRange.max());
            }
            case ENUM -> {
                String enumProjectionPossibleValues = tablePartitions.computeEnumProjectionPossibleValues(column.name());
                writer.addToList("partition_projection_values = ARRAY[%s]", enumProjectionPossibleValues);
            }
            // we aren't handling those
            case DATE, INJECTED -> {}
        }
        writer.outdent();
        writer.println();
        writer.print(")");
        writer.endList(false);
    }

    @Override
    protected void addColumn(Operation.AddColumn addColumn)
    {
        writer.println("ALTER TABLE %s", addColumn.tableName().toStringQuoted(options.catalogName(), schemaName));
        writer.indent();
        writer.println("ADD COLUMN \"%s\" %s;", addColumn.column().name(), Mapping.columnType(addColumn.column()));
        writer.outdent();
        writer.println();
    }

    @Override
    protected void addPartitionColumn(Operation.AddPartitionColumn addPartitionColumn)
    {
        writer.println("-- Partition column adds not supported. [%s.\"%s\"] will not be added.", addPartitionColumn.tableName().toStringQuoted(options.catalogName(), schemaName), addPartitionColumn.column().name());
        writer.println();
    }

    @Override
    protected void dropColumn(Operation.DropColumn dropColumn)
    {
        dropColumn(dropColumn.tableName(), dropColumn.columnName());
    }

    @Override
    protected void dropPartitionColumn(Operation.DropPartitionColumn dropPartitionColumn)
    {
        dropColumn(dropPartitionColumn.tableName(), dropPartitionColumn.columnName());
    }

    @Override
    protected void dropPartitionValue(Operation.DropPartitionValue dropPartitionValue)
    {
        writePartitionValues(dropPartitionValue.tableName(), false, dropPartitionValue.partitionColumns(), dropPartitionValue.partitionValues());
    }

    @Override
    protected void addPartitionValue(Operation.AddPartitionValue addPartitionValue)
    {
        writePartitionValues(addPartitionValue.tableName(), true, addPartitionValue.newPartitionColumns(), addPartitionValue.partitionValues());
    }

    @Override
    protected void renameColumn(Operation.RenameColumn renameColumn)
    {
        writer.println("ALTER TABLE %s", renameColumn.tableName().toStringQuoted(options.catalogName(), schemaName));
        writer.indent();
        writer.println("RENAME COLUMN \"%s\" TO \"%s\";", renameColumn.oldName(), renameColumn.newName());
        writer.outdent();
        writer.println();
    }

    @Override
    protected void renamePartitionColumn(Operation.RenamePartitionColumn renamePartitionColumn)
    {
        writer.println("-- Partition column renames not supported. [%s.\"%s\"] will not be renamed.", renamePartitionColumn.tableName().toStringQuoted(options.catalogName(), schemaName), renamePartitionColumn.oldName());
        writer.println();
    }

    @Override
    protected void dropBucket(Operation.DropBucket dropBucket)
    {
        writer.println("-- Bucket drops not supported. [%s.\"%s\"] will not be dropped.", dropBucket.tableName().toStringQuoted(options.catalogName(), schemaName), dropBucket.bucketName());
        writer.println();
    }

    @Override
    protected void addBucket(Operation.AddBucket addBucket)
    {
        writer.println("-- Bucket adds not supported. [%s.\"%s\"] will not be added.", addBucket.tableName().toStringQuoted(options.catalogName(), schemaName), addBucket.bucketName());
        writer.println();
    }

    private void dropColumn(TableName tableName, LowerCaseString columnName)
    {
        writer.println("ALTER TABLE %s", tableName.toStringQuoted(options.catalogName(), schemaName));
        writer.indent();
        writer.println("DROP COLUMN \"%s\";", columnName);
        writer.outdent();
        writer.println();
    }

    private void addCsvRowFormats(DiscoveredTable table)
    {
        CsvOptions options = new CsvOptions(new OptionsMap(table.options()));
        if (table.columns().flags().contains(CsvFlags.HAS_QUOTED_FIELDS)) {
            addToListSingleCharacter("csv_separator = ", options.delimiter().charAt(0));
            addToListSingleCharacter("csv_quote = ", options.quote());
            addToListSingleCharacter("csv_escape = ", options.escape());
        }
        else {
            addToListSingleCharacter("textfile_field_separator = ", options.delimiter().charAt(0));
            addToListSingleCharacter("textfile_field_separator_escape = ", options.escape());
        }
        if (options.firstLineIsHeaders()) {
            writer.addToList("skip_header_line_count = 1");
        }
    }

    private void addToListSingleCharacter(String prefix, char c)
    {
        if (Character.isISOControl(c)) {
            writer.addToList(prefix + "U&'\\%04x'", (int) c);
        }
        else {
            writer.addToList(prefix + "'%c'", c);
        }
    }

    private void addPartitions(DiscoveredTable table)
    {
        String procedure = options.catalogName().map("CALL %s.system.sync_partition_metadata"::formatted).orElse("CALL system.sync_partition_metadata");
        writer.println(
                "%s('%s', '%s', '%s');",
                procedure,
                table.tableName().schemaName().orElse(schemaName),
                table.tableName().tableName(),
                SyncPartitionMetadataProcedure.SyncMode.ADD);
    }

    private void writePartitionValues(TableName tableName, boolean register, List<Column> partitionColumns, DiscoveredPartitionValues partitionValues)
    {
        String partitionValuesList = partitionColumns.stream()
                .map(column -> Mapping.partitionValue(column, partitionValues.values().get(column.name())))
                .collect(Collectors.joining(", "));
        if (options.catalogName().isPresent()) {
            writer.println("CALL %s.system.%s(", options.catalogName().get(), register ? "register_partition" : "unregister_partition");
        }
        else {
            writer.println("CALL system.%s(", register ? "register_partition" : "unregister_partition");
        }
        writer.indent();
        writer.startList();
        writer.addToList("schema_name => '%s'", tableName.schemaName().orElse(schemaName));
        writer.addToList("table_name => '%s'", tableName.tableName());
        writer.addToList("partition_columns => ARRAY[%s]", Mapping.commaList(partitionColumns.stream(), c -> c.name().string()));
        writer.addToList("partition_values => ARRAY[%s]", partitionValuesList);
        if (register) {
            writer.addToList("location => '%s'", partitionValues.path());
        }
        writer.endList();
        writer.outdent();
        writer.println(");");
        writer.println();
    }
}
