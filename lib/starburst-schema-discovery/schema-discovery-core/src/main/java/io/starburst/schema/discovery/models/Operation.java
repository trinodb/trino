/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.internal.Column;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Operation.CreateSchema.class, name = "CreateSchema"),
        @JsonSubTypes.Type(value = Operation.Comment.class, name = "Comment"),
        @JsonSubTypes.Type(value = Operation.CreateTable.class, name = "CreateTable"),
        @JsonSubTypes.Type(value = Operation.DropTable.class, name = "DropTable"),
        @JsonSubTypes.Type(value = Operation.AddColumn.class, name = "AddColumn"),
        @JsonSubTypes.Type(value = Operation.AddPartitionColumn.class, name = "AddPartitionColumn"),
        @JsonSubTypes.Type(value = Operation.DropColumn.class, name = "DropColumn"),
        @JsonSubTypes.Type(value = Operation.DropPartitionColumn.class, name = "DropPartitionColumn"),
        @JsonSubTypes.Type(value = Operation.AddPartitionValue.class, name = "AddPartitionValue"),
        @JsonSubTypes.Type(value = Operation.DropPartitionValue.class, name = "DropPartitionValue"),
        @JsonSubTypes.Type(value = Operation.RenameColumn.class, name = "RenameColumn"),
        @JsonSubTypes.Type(value = Operation.RenamePartitionColumn.class, name = "RenamePartitionColumn"),
        @JsonSubTypes.Type(value = Operation.AddBucket.class, name = "AddBucket"),
        @JsonSubTypes.Type(value = Operation.DropBucket.class, name = "DropBucket"),
        @JsonSubTypes.Type(value = Operation.RegisterTable.class, name = "RegisterTable"),
        @JsonSubTypes.Type(value = Operation.UnregisterTable.class, name = "UnregisterTable"),
})
public sealed interface Operation
{
    sealed interface TableOperation
    {
        TableName tableName();
    }

    @JsonProperty("operationType")
    default String operationType()
    {
        return getClass().getSimpleName();
    }

    String operationSummary();

    record Comment(String comment)
            implements Operation
    {
        public Comment
        {
            requireNonNull(comment, "comment is null");
        }

        @Override
        public String operationSummary()
        {
            return comment;
        }
    }

    record CreateSchema(SlashEndedPath rootUri, LowerCaseString schemaName)
            implements Operation
    {
        public CreateSchema
        {
            requireNonNull(rootUri, "rootUri is null");
            requireNonNull(schemaName, "schemaName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Created schema: [%s], with location: [%s]".formatted(schemaName, rootUri);
        }
    }

    record CreateTable(DiscoveredTable table)
            implements Operation, TableOperation
    {
        public CreateTable
        {
            requireNonNull(table, "table is null");
        }

        @Override
        public TableName tableName()
        {
            return table.tableName();
        }

        @Override
        public String operationSummary()
        {
            return "Created table: [%s], with location: [%s]".formatted(table.tableName(), table.path());
        }
    }

    record RegisterTable(SlashEndedPath path, TableName tableName)
            implements Operation, TableOperation
    {
        public RegisterTable
        {
            requireNonNull(path, "path is null");
            requireNonNull(tableName, "tableName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Registered table: [%s], with location: [%s]".formatted(tableName, path);
        }
    }

    record UnregisterTable(TableName tableName)
            implements Operation, TableOperation
    {
        public UnregisterTable
        {
            requireNonNull(tableName, "tableName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Unregister table: [%s]".formatted(tableName);
        }
    }

    record DropTable(TableName tableName)
            implements Operation, TableOperation
    {
        public DropTable
        {
            requireNonNull(tableName, "tableName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Dropped table: [%s]".formatted(tableName);
        }
    }

    record AddColumn(TableName tableName, Column column)
            implements Operation, TableOperation
    {
        public AddColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(column, "column is null");
        }

        @Override
        public String operationSummary()
        {
            return "Added column: [%s], of type: [%s], to table: [%s]".formatted(column.name(), column.type().value(), tableName);
        }
    }

    record AddPartitionColumn(TableName tableName, Column column)
            implements Operation, TableOperation
    {
        public AddPartitionColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(column, "column is null");
        }

        @Override
        public String operationSummary()
        {
            return "Added partition column: [%s], of type: [%s], to table: [%s]".formatted(column.name(), column.type().value(), tableName);
        }
    }

    record DropColumn(TableName tableName, LowerCaseString columnName)
            implements Operation, TableOperation
    {
        public DropColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(columnName, "columnName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Dropped column: [%s], from table: [%s]".formatted(columnName, tableName);
        }
    }

    record DropPartitionColumn(TableName tableName, LowerCaseString columnName)
            implements Operation, TableOperation
    {
        public DropPartitionColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(columnName, "columnName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Dropped partition column: [%s], from table: [%s]".formatted(columnName, tableName);
        }
    }

    record AddPartitionValue(TableName tableName, List<Column> newPartitionColumns, DiscoveredPartitionValues partitionValues)
            implements Operation, TableOperation
    {
        public AddPartitionValue
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(newPartitionColumns, "newPartitionColumns is null");
            requireNonNull(partitionValues, "partitionValues is null");
        }

        @Override
        public String operationSummary()
        {
            return "Added [%s] partition values from path: [%s], to table: [%s]".formatted(partitionValues.values().size(), partitionValues.path(), tableName);
        }
    }

    record DropPartitionValue(TableName tableName, List<Column> partitionColumns, DiscoveredPartitionValues partitionValues)
            implements Operation, TableOperation
    {
        public DropPartitionValue
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(partitionColumns, "partitionColumns is null");
            requireNonNull(partitionValues, "partitionValues is null");
        }

        @Override
        public String operationSummary()
        {
            return "Dropped [%s] partition values from path: [%s], from table: [%s]".formatted(partitionValues.values().size(), partitionValues.path(), tableName);
        }
    }

    record RenameColumn(TableName tableName, LowerCaseString oldName, LowerCaseString newName)
            implements Operation, TableOperation
    {
        public RenameColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(oldName, "oldName is null");
            requireNonNull(newName, "newName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Renamed column: [%s] to: [%s], in table: [%s]".formatted(oldName, newName, tableName);
        }
    }

    record RenamePartitionColumn(TableName tableName, LowerCaseString oldName, LowerCaseString newName)
            implements Operation, TableOperation
    {
        public RenamePartitionColumn
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(oldName, "oldName is null");
            requireNonNull(newName, "newName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Nothing changed, partition column renames not supported";
        }
    }

    record AddBucket(TableName tableName, LowerCaseString bucketName)
            implements Operation, TableOperation
    {
        public AddBucket
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(bucketName, "bucketName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Nothing changed, bucket adds not supported";
        }
    }

    record DropBucket(TableName tableName, LowerCaseString bucketName)
            implements Operation, TableOperation
    {
        public DropBucket
        {
            requireNonNull(tableName, "tableName is null");
            requireNonNull(bucketName, "bucketName is null");
        }

        @Override
        public String operationSummary()
        {
            return "Nothing changed, bucket drops not supported";
        }
    }
}
