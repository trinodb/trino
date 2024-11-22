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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.errorprone.annotations.Immutable;
import io.trino.Session;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.spi.RefreshType;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

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
    private final Optional<PartitioningScheme> partitioningScheme;
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
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme,
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
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
        this.statisticsAggregationDescriptor = requireNonNull(statisticsAggregationDescriptor, "statisticsAggregationDescriptor is null");
        checkArgument(statisticsAggregation.isPresent() == statisticsAggregationDescriptor.isPresent(), "statisticsAggregation and statisticsAggregationDescriptor must be either present or absent");

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
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
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
        return new TableWriterNode(getId(), Iterables.getOnlyElement(newChildren), target, rowCountSymbol, fragmentSymbol, columns, columnNames, partitioningScheme, statisticsAggregation, statisticsAggregationDescriptor);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CreateTarget.class, name = "CreateTarget"),
            @JsonSubTypes.Type(value = InsertTarget.class, name = "InsertTarget"),
            @JsonSubTypes.Type(value = MergeTarget.class, name = "MergeTarget"),
            @JsonSubTypes.Type(value = RefreshMaterializedViewTarget.class, name = "RefreshMaterializedViewTarget"),
            @JsonSubTypes.Type(value = TableExecuteTarget.class, name = "TableExecuteTarget"),
    })

    @SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
    public abstract static class WriterTarget
    {
        @Override
        public abstract String toString();

        public abstract boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session);

        public abstract OptionalInt getMaxWriterTasks(Metadata metadata, Session session);

        public abstract WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session);
    }

    // only used during planning -- will not be serialized
    public static class CreateReference
            extends WriterTarget
    {
        private final String catalog;
        private final ConnectorTableMetadata tableMetadata;
        private final Optional<TableLayout> layout;
        private final boolean replace;

        public CreateReference(String catalog, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout, boolean replace)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
            this.layout = requireNonNull(layout, "layout is null");
            this.replace = replace;
        }

        public String getCatalog()
        {
            return catalog;
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return layout.map(tableLayout -> tableLayout.getLayout().supportsMultipleWritersPerPartition()).orElse(true);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return metadata.getMaxWriterTasks(session, catalog);
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            QualifiedObjectName tableName = new QualifiedObjectName(
                    catalog,
                    tableMetadata.getTableSchema().getTable().getSchemaName(),
                    tableMetadata.getTableSchema().getTable().getTableName());
            return metadata.getNewTableWriterScalingOptions(session, tableName, tableMetadata.getProperties());
        }

        public Optional<TableLayout> getLayout()
        {
            return layout;
        }

        public ConnectorTableMetadata getTableMetadata()
        {
            return tableMetadata;
        }

        public boolean isReplace()
        {
            return replace;
        }

        @Override
        public String toString()
        {
            return catalog + "." + tableMetadata.getTable();
        }
    }

    public static class CreateTarget
            extends WriterTarget
    {
        private final OutputTableHandle handle;
        private final SchemaTableName schemaTableName;
        private final boolean multipleWritersPerPartitionSupported;
        private final OptionalInt maxWriterTasks;
        private final WriterScalingOptions writerScalingOptions;
        private final boolean replace;

        @JsonCreator
        public CreateTarget(
                @JsonProperty("handle") OutputTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("multipleWritersPerPartitionSupported") boolean multipleWritersPerPartitionSupported,
                @JsonProperty("maxWriterTasks") OptionalInt maxWriterTasks,
                @JsonProperty("writerScalingOptions") WriterScalingOptions writerScalingOptions,
                @JsonProperty("replace") boolean replace)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.multipleWritersPerPartitionSupported = multipleWritersPerPartitionSupported;
            this.maxWriterTasks = requireNonNull(maxWriterTasks, "maxWriterTasks is null");
            this.writerScalingOptions = requireNonNull(writerScalingOptions, "writerScalingOptions is null");
            this.replace = replace;
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

        @JsonProperty
        public boolean isMultipleWritersPerPartitionSupported()
        {
            return multipleWritersPerPartitionSupported;
        }

        @JsonProperty
        public WriterScalingOptions getWriterScalingOptions()
        {
            return writerScalingOptions;
        }

        @JsonProperty
        public boolean isReplace()
        {
            return replace;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return multipleWritersPerPartitionSupported;
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return maxWriterTasks;
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return writerScalingOptions;
        }
    }

    // only used during planning -- will not be serialized
    public static class InsertReference
            extends WriterTarget
    {
        private final TableHandle handle;
        private final List<ColumnHandle> columns;

        public InsertReference(TableHandle handle, List<ColumnHandle> columns)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
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
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return metadata.getInsertLayout(session, handle)
                    .map(layout -> layout.getLayout().supportsMultipleWritersPerPartition())
                    .orElse(true);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return metadata.getMaxWriterTasks(session, handle.catalogHandle().getCatalogName().toString());
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return metadata.getInsertWriterScalingOptions(session, handle);
        }
    }

    public static class InsertTarget
            extends WriterTarget
    {
        private final InsertTableHandle handle;
        private final SchemaTableName schemaTableName;
        private final boolean multipleWritersPerPartitionSupported;
        private final OptionalInt maxWriterTasks;
        private final WriterScalingOptions writerScalingOptions;
        private final List<TableHandle> sourceTableHandles;

        @JsonCreator
        public InsertTarget(
                @JsonProperty("handle") InsertTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("multipleWritersPerPartitionSupported") boolean multipleWritersPerPartitionSupported,
                @JsonProperty("maxWriterTasks") OptionalInt maxWriterTasks,
                @JsonProperty("writerScalingOptions") WriterScalingOptions writerScalingOptions,
                @JsonProperty("sourceTableHandles") List<TableHandle> sourceTableHandles)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.multipleWritersPerPartitionSupported = multipleWritersPerPartitionSupported;
            this.maxWriterTasks = requireNonNull(maxWriterTasks, "maxWriterTasks is null");
            this.writerScalingOptions = requireNonNull(writerScalingOptions, "writerScalingOptions is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
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

        @JsonProperty
        public boolean isMultipleWritersPerPartitionSupported()
        {
            return multipleWritersPerPartitionSupported;
        }

        @JsonProperty
        public WriterScalingOptions getWriterScalingOptions()
        {
            return writerScalingOptions;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return multipleWritersPerPartitionSupported;
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return maxWriterTasks;
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return writerScalingOptions;
        }

        @JsonProperty
        public List<TableHandle> getSourceTableHandles()
        {
            return sourceTableHandles;
        }
    }

    public static class RefreshMaterializedViewReference
            extends WriterTarget
    {
        private final String table;
        private final TableHandle storageTableHandle;
        private final List<TableHandle> sourceTableHandles;
        private final List<String> sourceTableFunctions;
        private final RefreshType refreshType;

        public RefreshMaterializedViewReference(
                String table,
                TableHandle storageTableHandle,
                List<TableHandle> sourceTableHandles,
                List<String> sourceTableFunctions,
                RefreshType refreshType)
        {
            this.table = requireNonNull(table, "table is null");
            this.storageTableHandle = requireNonNull(storageTableHandle, "storageTableHandle is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
            this.sourceTableFunctions = ImmutableList.copyOf(sourceTableFunctions);
            this.refreshType = requireNonNull(refreshType, "refreshType is null");
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
            return table;
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return metadata.getInsertLayout(session, storageTableHandle)
                    .map(layout -> layout.getLayout().supportsMultipleWritersPerPartition())
                    .orElse(true);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return metadata.getMaxWriterTasks(session, storageTableHandle.catalogHandle().getCatalogName().toString());
        }

        public List<String> getSourceTableFunctions()
        {
            return sourceTableFunctions;
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return metadata.getInsertWriterScalingOptions(session, storageTableHandle);
        }

        public RefreshType getRefreshType()
        {
            return refreshType;
        }

        public RefreshMaterializedViewReference withRefreshType(RefreshType refreshType)
        {
            return new RefreshMaterializedViewReference(table, storageTableHandle, sourceTableHandles, sourceTableFunctions, refreshType);
        }
    }

    public static class RefreshMaterializedViewTarget
            extends WriterTarget
    {
        private final TableHandle tableHandle;
        private final InsertTableHandle insertHandle;
        private final SchemaTableName schemaTableName;
        private final List<TableHandle> sourceTableHandles;
        private final List<String> sourceTableFunctions;
        private final WriterScalingOptions writerScalingOptions;

        @JsonCreator
        public RefreshMaterializedViewTarget(
                @JsonProperty("tableHandle") TableHandle tableHandle,
                @JsonProperty("insertHandle") InsertTableHandle insertHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("sourceTableHandles") List<TableHandle> sourceTableHandles,
                @JsonProperty("sourceTableFunctions") List<String> sourceTableFunctions,
                @JsonProperty("writerScalingOptions") WriterScalingOptions writerScalingOptions)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.insertHandle = requireNonNull(insertHandle, "insertHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
            this.sourceTableFunctions = ImmutableList.copyOf(sourceTableFunctions);
            this.writerScalingOptions = requireNonNull(writerScalingOptions, "writerScalingOptions is null");
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

        @JsonProperty
        public List<String> getSourceTableFunctions()
        {
            return sourceTableFunctions;
        }

        @JsonProperty
        public WriterScalingOptions getWriterScalingOptions()
        {
            return writerScalingOptions;
        }

        @Override
        public String toString()
        {
            return insertHandle.toString();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return metadata.getInsertLayout(session, tableHandle)
                    .map(layout -> layout.getLayout().supportsMultipleWritersPerPartition())
                    .orElse(true);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return metadata.getMaxWriterTasks(session, tableHandle.catalogHandle().getCatalogName().toString());
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return writerScalingOptions;
        }
    }

    public static class TableExecuteTarget
            extends WriterTarget
    {
        private final TableExecuteHandle executeHandle;
        private final Optional<TableHandle> sourceHandle;
        private final SchemaTableName schemaTableName;
        private final WriterScalingOptions writerScalingOptions;

        @JsonCreator
        public TableExecuteTarget(
                @JsonProperty("executeHandle") TableExecuteHandle executeHandle,
                @JsonProperty("sourceHandle") Optional<TableHandle> sourceHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("writerScalingOptions") WriterScalingOptions writerScalingOptions)
        {
            this.executeHandle = requireNonNull(executeHandle, "handle is null");
            this.sourceHandle = requireNonNull(sourceHandle, "sourceHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.writerScalingOptions = requireNonNull(writerScalingOptions, "writerScalingOptions is null");
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

        @JsonProperty
        public WriterScalingOptions getWriterScalingOptions()
        {
            return writerScalingOptions;
        }

        @Override
        public String toString()
        {
            return executeHandle.toString();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return metadata.getLayoutForTableExecute(session, executeHandle)
                    .map(layout -> layout.getLayout().supportsMultipleWritersPerPartition())
                    .orElse(true);
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return metadata.getMaxWriterTasks(session, executeHandle.catalogHandle().getCatalogName().toString());
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return writerScalingOptions;
        }
    }

    public static class MergeTarget
            extends WriterTarget
    {
        private final TableHandle handle;
        private final Optional<MergeHandle> mergeHandle;
        private final SchemaTableName schemaTableName;
        private final MergeParadigmAndTypes mergeParadigmAndTypes;
        private final List<TableHandle> sourceTableHandles;
        private final Multimap<Integer, ColumnHandle> updateCaseColumnHandles;

        @JsonCreator
        public MergeTarget(
                @JsonProperty("handle") TableHandle handle,
                @JsonProperty("mergeHandle") Optional<MergeHandle> mergeHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("mergeParadigmAndTypes") MergeParadigmAndTypes mergeParadigmAndTypes,
                @JsonProperty("sourceTableHandles") List<TableHandle> sourceTableHandles,
                @JsonProperty("updateCaseColumnHandles") Multimap<Integer, ColumnHandle> updateCaseColumnHandles)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.mergeHandle = requireNonNull(mergeHandle, "mergeHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.mergeParadigmAndTypes = requireNonNull(mergeParadigmAndTypes, "mergeElements is null");
            this.sourceTableHandles = ImmutableList.copyOf(requireNonNull(sourceTableHandles, "sourceTableHandles is null"));
            this.updateCaseColumnHandles = requireNonNull(updateCaseColumnHandles, "updateCaseColumnHandles is null");
        }

        @JsonProperty
        public TableHandle getHandle()
        {
            return handle;
        }

        @JsonProperty
        public Optional<MergeHandle> getMergeHandle()
        {
            return mergeHandle;
        }

        @JsonProperty
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @JsonProperty
        public MergeParadigmAndTypes getMergeParadigmAndTypes()
        {
            return mergeParadigmAndTypes;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            return false;
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            return OptionalInt.empty();
        }

        @Override
        public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
        {
            return WriterScalingOptions.DISABLED;
        }

        @JsonProperty
        public List<TableHandle> getSourceTableHandles()
        {
            return sourceTableHandles;
        }

        @JsonProperty
        public Multimap<Integer, ColumnHandle> getUpdateCaseColumnHandles()
        {
            return updateCaseColumnHandles;
        }
    }

    public static class MergeParadigmAndTypes
    {
        private final Optional<RowChangeParadigm> paradigm;
        private final List<Type> columnTypes;
        private final List<String> columnNames;
        private final Type rowIdType;

        @JsonCreator
        public MergeParadigmAndTypes(
                @JsonProperty("paradigm") Optional<RowChangeParadigm> paradigm,
                @JsonProperty("columnTypes") List<Type> columnTypes,
                @JsonProperty("columnNames") List<String> columnNames,
                @JsonProperty("rowIdType") Type rowIdType)
        {
            this.paradigm = requireNonNull(paradigm, "paradigm is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
            this.columnNames = requireNonNull(columnNames, "columnNames is null");
            this.rowIdType = requireNonNull(rowIdType, "rowIdType is null");
        }

        @JsonProperty
        public Optional<RowChangeParadigm> getParadigm()
        {
            return paradigm;
        }

        @JsonProperty
        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        @JsonProperty
        public List<String> getColumnNames()
        {
            return columnNames;
        }

        @JsonProperty
        public Type getRowIdType()
        {
            return rowIdType;
        }
    }
}
