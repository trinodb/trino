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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.connector.CatalogName;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.NewTableLayout;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Table;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final WriterTarget target;
    private final Symbol rowCountSymbol;
    private final Symbol fragmentSymbol;
    private final List<Symbol> columns;
    private final List<String> columnNames;
    private final Set<Symbol> notNullColumnSymbols;
    private final Optional<PartitioningScheme> partitioningScheme;
    private final Optional<PartitioningScheme> preferredPartitioningScheme;
    private final Optional<StatisticAggregations> statisticsAggregation;
    private final Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor;
    private final List<Symbol> outputs;

    @JsonCreator
    public TableWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") WriterTarget target,
            @JsonProperty("rowCountSymbol") Symbol rowCountSymbol,
            @JsonProperty("fragmentSymbol") Symbol fragmentSymbol,
            @JsonProperty("columns") List<Symbol> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("notNullColumnSymbols") Set<Symbol> notNullColumnSymbols,
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme,
            @JsonProperty("preferredPartitioningScheme") Optional<PartitioningScheme> preferredPartitioningScheme,
            @JsonProperty("statisticsAggregation") Optional<StatisticAggregations> statisticsAggregation,
            @JsonProperty("statisticsAggregationDescriptor") Optional<StatisticAggregationsDescriptor<Symbol>> statisticsAggregationDescriptor)
    {
        super(id);

        requireNonNull(columns, "columns is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountSymbol = requireNonNull(rowCountSymbol, "rowCountSymbol is null");
        this.fragmentSymbol = requireNonNull(fragmentSymbol, "fragmentSymbol is null");
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.notNullColumnSymbols = ImmutableSet.copyOf(requireNonNull(notNullColumnSymbols, "notNullColumnSymbols is null"));
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
        this.preferredPartitioningScheme = requireNonNull(preferredPartitioningScheme, "preferredPartitioningScheme is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
        this.statisticsAggregationDescriptor = requireNonNull(statisticsAggregationDescriptor, "statisticsAggregationDescriptor is null");
        checkArgument(statisticsAggregation.isPresent() == statisticsAggregationDescriptor.isPresent(), "statisticsAggregation and statisticsAggregationDescriptor must be either present or absent");
        checkArgument(partitioningScheme.isEmpty() || preferredPartitioningScheme.isEmpty(), "Both partitioningScheme and preferredPartitioningScheme cannot be present");

        ImmutableList.Builder<Symbol> outputs = ImmutableList.<Symbol>builder()
                .add(rowCountSymbol)
                .add(fragmentSymbol);
        statisticsAggregation.ifPresent(aggregation -> {
            outputs.addAll(aggregation.getGroupingSymbols());
            outputs.addAll(aggregation.getAggregations().keySet());
        });
        this.outputs = outputs.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public WriterTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public Symbol getRowCountSymbol()
    {
        return rowCountSymbol;
    }

    @JsonProperty
    public Symbol getFragmentSymbol()
    {
        return fragmentSymbol;
    }

    @JsonProperty
    public List<Symbol> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public Set<Symbol> getNotNullColumnSymbols()
    {
        return notNullColumnSymbols;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPreferredPartitioningScheme()
    {
        return preferredPartitioningScheme;
    }

    @JsonProperty
    public Optional<StatisticAggregations> getStatisticsAggregation()
    {
        return statisticsAggregation;
    }

    @JsonProperty
    public Optional<StatisticAggregationsDescriptor<Symbol>> getStatisticsAggregationDescriptor()
    {
        return statisticsAggregationDescriptor;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TableWriterNode(getId(), Iterables.getOnlyElement(newChildren), target, rowCountSymbol, fragmentSymbol, columns, columnNames, notNullColumnSymbols, partitioningScheme, preferredPartitioningScheme, statisticsAggregation, statisticsAggregationDescriptor);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CreateTarget.class, name = "CreateTarget"),
            @JsonSubTypes.Type(value = InsertTarget.class, name = "InsertTarget"),
            @JsonSubTypes.Type(value = DeleteTarget.class, name = "DeleteTarget"),
            @JsonSubTypes.Type(value = UpdateTarget.class, name = "UpdateTarget"),
            @JsonSubTypes.Type(value = RefreshMaterializedViewTarget.class, name = "RefreshMaterializedViewTarget"),
            @JsonSubTypes.Type(value = TableExecuteTarget.class, name = "TableExecuteTarget"),
    })
    @SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
    public abstract static class WriterTarget
    {
        @Override
        public abstract String toString();

        public abstract CatalogName getCatalogName();
    }

    // only used during planning -- will not be serialized
    public static class CreateReference
            extends WriterTarget
    {
        private final CatalogName catalogName;
        private final ConnectorTableMetadata tableMetadata;
        private final Optional<NewTableLayout> layout;

        public CreateReference(CatalogName catalogName, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
            this.layout = requireNonNull(layout, "layout is null");
        }

        public ConnectorTableMetadata getTableMetadata()
        {
            return tableMetadata;
        }

        public Optional<NewTableLayout> getLayout()
        {
            return layout;
        }

        @Override
        public String toString()
        {
            return catalogName + "." + tableMetadata.getTable();
        }

        @Override
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class CreateTarget
            extends WriterTarget
    {
        private final OutputTableHandle handle;
        private final SchemaTableName schemaTableName;
        private final CatalogName catalogName;

        @JsonCreator
        public CreateTarget(
                @JsonProperty("handle") OutputTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public OutputTableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    // only used during planning -- will not be serialized
    public static class InsertReference
            extends WriterTarget
    {
        private final TableHandle handle;
        private final List<ColumnHandle> columns;
        private final CatalogName catalogName;

        public InsertReference(TableHandle handle, List<ColumnHandle> columns, CatalogName catalogName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class InsertTarget
            extends WriterTarget
    {
        private final InsertTableHandle handle;
        private final SchemaTableName schemaTableName;
        private final CatalogName catalogName;

        @JsonCreator
        public InsertTarget(
                @JsonProperty("handle") InsertTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public InsertTableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class RefreshMaterializedViewReference
            extends WriterTarget
    {
        private final Table table;
        private final TableHandle storageTableHandle;
        private final List<TableHandle> sourceTableHandles;
        private final CatalogName catalogName;

        public RefreshMaterializedViewReference(Table table, TableHandle storageTableHandle, List<TableHandle> sourceTableHandles, CatalogName catalogName)
        {
            this.table = requireNonNull(table, "table is null");
            this.storageTableHandle = requireNonNull(storageTableHandle, "storageTableHandle is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        public Table getTable()
        {
            return table;
        }

        public TableHandle getStorageTableHandle()
        {
            return storageTableHandle;
        }

        public List<TableHandle> getSourceTableHandles()
        {
            return sourceTableHandles;
        }

        @Override
        public String toString()
        {
            return table.toString();
        }

        @Override
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class RefreshMaterializedViewTarget
            extends WriterTarget
    {
        private final TableHandle tableHandle;
        private final InsertTableHandle insertHandle;
        private final SchemaTableName schemaTableName;
        private final List<TableHandle> sourceTableHandles;
        private final CatalogName catalogName;

        @JsonCreator
        public RefreshMaterializedViewTarget(
                @JsonProperty("tableHandle") TableHandle tableHandle,
                @JsonProperty("insertHandle") InsertTableHandle insertHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("sourceTableHandles") List<TableHandle> sourceTableHandles,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.insertHandle = requireNonNull(insertHandle, "insertHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public InsertTableHandle getInsertHandle()
        {
            return insertHandle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @JsonProperty
        public List<TableHandle> getSourceTableHandles()
        {
            return sourceTableHandles;
        }

        @Override
        public String toString()
        {
            return insertHandle.toString();
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class DeleteTarget
            extends WriterTarget
    {
        private final Optional<TableHandle> handle;
        private final SchemaTableName schemaTableName;
        private final CatalogName catalogName;

        @JsonCreator
        public DeleteTarget(
                @JsonProperty("handle") Optional<TableHandle> handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public Optional<TableHandle> getHandle()
        {
            return handle;
        }

        @JsonIgnore
        public TableHandle getHandleOrElseThrow()
        {
            return handle.orElseThrow(() -> new IllegalStateException("DeleteTarget does not contain handle"));
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.map(Object::toString).orElse("[]");
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class UpdateTarget
            extends WriterTarget
    {
        private final Optional<TableHandle> handle;
        private final SchemaTableName schemaTableName;
        private final List<String> updatedColumns;
        private final List<ColumnHandle> updatedColumnHandles;
        private final CatalogName catalogName;

        @JsonCreator
        public UpdateTarget(
                @JsonProperty("handle") Optional<TableHandle> handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("updatedColumns") List<String> updatedColumns,
                @JsonProperty("updatedColumnHandles") List<ColumnHandle> updatedColumnHandles,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            checkArgument(updatedColumns.size() == updatedColumnHandles.size(), "updatedColumns size %s must equal updatedColumnHandles size %s", updatedColumns.size(), updatedColumnHandles.size());
            this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
            this.updatedColumnHandles = requireNonNull(updatedColumnHandles, "updatedColumnHandles is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public Optional<TableHandle> getHandle()
        {
            return handle;
        }

        @JsonIgnore
        public TableHandle getHandleOrElseThrow()
        {
            return handle.orElseThrow(() -> new IllegalStateException("UpdateTarge does not contain handle"));
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @JsonProperty
        public List<String> getUpdatedColumns()
        {
            return updatedColumns;
        }

        @JsonProperty
        public List<ColumnHandle> getUpdatedColumnHandles()
        {
            return updatedColumnHandles;
        }

        @Override
        public String toString()
        {
            return handle.map(Object::toString).orElse("[]");
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }

    public static class TableExecuteTarget
            extends WriterTarget
    {
        private final TableExecuteHandle executeHandle;
        private final Optional<TableHandle> sourceHandle;
        private final SchemaTableName schemaTableName;
        private final CatalogName catalogName;

        @JsonCreator
        public TableExecuteTarget(
                @JsonProperty("executeHandle") TableExecuteHandle executeHandle,
                @JsonProperty("sourceHandle") Optional<TableHandle> sourceHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("catalogName") CatalogName catalogName)
        {
            this.executeHandle = requireNonNull(executeHandle, "handle is null");
            this.sourceHandle = requireNonNull(sourceHandle, "sourceHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
        }

        @JsonProperty
        public TableExecuteHandle getExecuteHandle()
        {
            return executeHandle;
        }

        @JsonProperty
        public Optional<TableHandle> getSourceHandle()
        {
            return sourceHandle;
        }

        public TableHandle getMandatorySourceHandle()
        {
            return sourceHandle.orElseThrow();
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return executeHandle.toString();
        }

        @Override
        @JsonProperty
        public CatalogName getCatalogName()
        {
            return catalogName;
        }
    }
}
