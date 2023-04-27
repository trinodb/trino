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
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableExecuteHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableLayout;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

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

        public abstract boolean supportsReportingWrittenBytes(Metadata metadata, Session session);

        public abstract boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session);

        public abstract OptionalInt getMaxWriterTasks(Metadata metadata, Session session);
    }

    // only used during planning -- will not be serialized
    public static class CreateReference
            extends WriterTarget
    {
        private final String catalog;
        private final ConnectorTableMetadata tableMetadata;
        private final Optional<TableLayout> layout;

        public CreateReference(String catalog, ConnectorTableMetadata tableMetadata, Optional<TableLayout> layout)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
            this.layout = requireNonNull(layout, "layout is null");
        }

        public String getCatalog()
        {
            return catalog;
        }

        @Override
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            QualifiedObjectName fullTableName = new QualifiedObjectName(
                    catalog,
                    tableMetadata.getTableSchema().getTable().getSchemaName(),
                    tableMetadata.getTableSchema().getTable().getTableName());
            return metadata.supportsReportingWrittenBytes(session, fullTableName, tableMetadata.getProperties());
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

        public Optional<TableLayout> getLayout()
        {
            return layout;
        }

        public ConnectorTableMetadata getTableMetadata()
        {
            return tableMetadata;
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
        private final boolean reportingWrittenBytesSupported;
        private final boolean multipleWritersPerPartitionSupported;
        private final OptionalInt maxWriterTasks;

        @JsonCreator
        public CreateTarget(
                @JsonProperty("handle") OutputTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("reportingWrittenBytesSupported") boolean reportingWrittenBytesSupported,
                @JsonProperty("multipleWritersPerPartitionSupported") boolean multipleWritersPerPartitionSupported,
                @JsonProperty("maxWriterTasks") OptionalInt maxWriterTasks)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.reportingWrittenBytesSupported = reportingWrittenBytesSupported;
            this.multipleWritersPerPartitionSupported = multipleWritersPerPartitionSupported;
            this.maxWriterTasks = requireNonNull(maxWriterTasks, "maxWriterTasks is null");
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
        public boolean getReportingWrittenBytesSupported()
        {
            return reportingWrittenBytesSupported;
        }

        @JsonProperty
        public boolean isMultipleWritersPerPartitionSupported()
        {
            return multipleWritersPerPartitionSupported;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return reportingWrittenBytesSupported;
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return metadata.supportsReportingWrittenBytes(session, handle);
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
            return metadata.getMaxWriterTasks(session, handle.getCatalogHandle().getCatalogName());
        }
    }

    public static class InsertTarget
            extends WriterTarget
    {
        private final InsertTableHandle handle;
        private final SchemaTableName schemaTableName;
        private final boolean reportingWrittenBytesSupported;
        private final boolean multipleWritersPerPartitionSupported;
        private final OptionalInt maxWriterTasks;

        @JsonCreator
        public InsertTarget(
                @JsonProperty("handle") InsertTableHandle handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("reportingWrittenBytesSupported") boolean reportingWrittenBytesSupported,
                @JsonProperty("multipleWritersPerPartitionSupported") boolean multipleWritersPerPartitionSupported,
                @JsonProperty("maxWriterTasks") OptionalInt maxWriterTasks)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.reportingWrittenBytesSupported = reportingWrittenBytesSupported;
            this.multipleWritersPerPartitionSupported = multipleWritersPerPartitionSupported;
            this.maxWriterTasks = requireNonNull(maxWriterTasks, "maxWriterTasks is null");
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
        public boolean getReportingWrittenBytesSupported()
        {
            return reportingWrittenBytesSupported;
        }

        @JsonProperty
        public boolean isMultipleWritersPerPartitionSupported()
        {
            return multipleWritersPerPartitionSupported;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }

        @Override
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return reportingWrittenBytesSupported;
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
    }

    public static class RefreshMaterializedViewReference
            extends WriterTarget
    {
        private final String table;
        private final TableHandle storageTableHandle;
        private final List<TableHandle> sourceTableHandles;

        public RefreshMaterializedViewReference(String table, TableHandle storageTableHandle, List<TableHandle> sourceTableHandles)
        {
            this.table = requireNonNull(table, "table is null");
            this.storageTableHandle = requireNonNull(storageTableHandle, "storageTableHandle is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return metadata.supportsReportingWrittenBytes(session, storageTableHandle);
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
            return metadata.getMaxWriterTasks(session, storageTableHandle.getCatalogHandle().getCatalogName());
        }
    }

    public static class RefreshMaterializedViewTarget
            extends WriterTarget
    {
        private final TableHandle tableHandle;
        private final InsertTableHandle insertHandle;
        private final SchemaTableName schemaTableName;
        private final List<TableHandle> sourceTableHandles;

        @JsonCreator
        public RefreshMaterializedViewTarget(
                @JsonProperty("tableHandle") TableHandle tableHandle,
                @JsonProperty("insertHandle") InsertTableHandle insertHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("sourceTableHandles") List<TableHandle> sourceTableHandles)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.insertHandle = requireNonNull(insertHandle, "insertHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.sourceTableHandles = ImmutableList.copyOf(sourceTableHandles);
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return metadata.supportsReportingWrittenBytes(session, tableHandle);
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
            return metadata.getMaxWriterTasks(session, tableHandle.getCatalogHandle().getCatalogName());
        }
    }

    public static class DeleteTarget
            extends WriterTarget
    {
        private final Optional<TableHandle> handle;
        private final SchemaTableName schemaTableName;

        @JsonCreator
        public DeleteTarget(
                @JsonProperty("handle") Optional<TableHandle> handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class UpdateTarget
            extends WriterTarget
    {
        private final Optional<TableHandle> handle;
        private final SchemaTableName schemaTableName;
        private final List<String> updatedColumns;
        private final List<ColumnHandle> updatedColumnHandles;

        @JsonCreator
        public UpdateTarget(
                @JsonProperty("handle") Optional<TableHandle> handle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("updatedColumns") List<String> updatedColumns,
                @JsonProperty("updatedColumnHandles") List<ColumnHandle> updatedColumnHandles)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            checkArgument(updatedColumns.size() == updatedColumnHandles.size(), "updatedColumns size %s must equal updatedColumnHandles size %s", updatedColumns.size(), updatedColumnHandles.size());
            this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
            this.updatedColumnHandles = requireNonNull(updatedColumnHandles, "updatedColumnHandles is null");
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TableExecuteTarget
            extends WriterTarget
    {
        private final TableExecuteHandle executeHandle;
        private final Optional<TableHandle> sourceHandle;
        private final SchemaTableName schemaTableName;
        private final boolean reportingWrittenBytesSupported;

        @JsonCreator
        public TableExecuteTarget(
                @JsonProperty("executeHandle") TableExecuteHandle executeHandle,
                @JsonProperty("sourceHandle") Optional<TableHandle> sourceHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("reportingWrittenBytesSupported") boolean reportingWrittenBytesSupported)
        {
            this.executeHandle = requireNonNull(executeHandle, "handle is null");
            this.sourceHandle = requireNonNull(sourceHandle, "sourceHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.reportingWrittenBytesSupported = reportingWrittenBytesSupported;
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
        public boolean isReportingWrittenBytesSupported()
        {
            return reportingWrittenBytesSupported;
        }

        @Override
        public String toString()
        {
            return executeHandle.toString();
        }

        @Override
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return sourceHandle.map(tableHandle -> metadata.supportsReportingWrittenBytes(session, tableHandle)).orElse(reportingWrittenBytesSupported);
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
            return metadata.getMaxWriterTasks(session, executeHandle.getCatalogHandle().getCatalogName());
        }
    }

    public static class MergeTarget
            extends WriterTarget
    {
        private final TableHandle handle;
        private final Optional<MergeHandle> mergeHandle;
        private final SchemaTableName schemaTableName;
        private final MergeParadigmAndTypes mergeParadigmAndTypes;

        @JsonCreator
        public MergeTarget(
                @JsonProperty("handle") TableHandle handle,
                @JsonProperty("mergeHandle") Optional<MergeHandle> mergeHandle,
                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                @JsonProperty("mergeParadigmAndTypes") MergeParadigmAndTypes mergeParadigmAndTypes)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.mergeHandle = requireNonNull(mergeHandle, "mergeHandle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.mergeParadigmAndTypes = requireNonNull(mergeParadigmAndTypes, "mergeElements is null");
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
        public boolean supportsReportingWrittenBytes(Metadata metadata, Session session)
        {
            return false;
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
