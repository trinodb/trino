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
package io.trino.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.ptf.ConnectorTableFunctionHandle;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;
import javax.inject.Provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class ExceptColumn
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "runtime";
    public static final String NAME = "except_column";

    private final MetadataManager metadataManager;

    @Inject
    public ExceptColumn(MetadataManager metadataManager)
    {
        this.metadataManager = requireNonNull(metadataManager, "metadataManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ExceptColumnFunction(metadataManager);
    }

    public static class ExceptColumnFunction
            extends AbstractConnectorTableFunction
    {
        private final Metadata metadata;

        public ExceptColumnFunction(Metadata metadata)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(ScalarArgumentSpecification.builder()
                                    .name("INPUT")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("EXCEPT_COLUMN")
                                    .type(new ArrayType(VARCHAR))
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession connectorSession, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
        {
            ScalarArgument input = (ScalarArgument) arguments.get("INPUT");
            String inputTableName = ((Slice) input.getValue()).toStringUtf8();
            QualifiedObjectName tableName;
            try {
                tableName = QualifiedObjectName.valueOf(inputTableName);
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(GENERIC_USER_ERROR, "INPUT argument must be fully qualified: " + inputTableName);
            }

            ScalarArgument exclude = (ScalarArgument) arguments.get("EXCEPT_COLUMN");
            Block excludeBlock = (Block) exclude.getValue();
            if (excludeBlock.getPositionCount() <= 0) {
                throw new TrinoException(GENERIC_USER_ERROR, "EXCEPT_COLUMN argument should have at least one element");
            }
            ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builderWithExpectedSize(excludeBlock.getPositionCount());
            for (int position = 0; position < excludeBlock.getPositionCount(); position++) {
                columnNamesBuilder.add(excludeBlock.getSlice(position, 0, excludeBlock.getSliceLength(position)).toStringUtf8());
            }

            List<String> columnNames = columnNamesBuilder.build();

            Session session = ((FullConnectorSession) connectorSession).getSession();

            TableHandle tableHandle = metadata.getTableHandle(session, tableName)
                    .orElseThrow(() -> new TableNotFoundException(tableName.asSchemaTableName()));
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);

            List<ColumnMetadata> columnMetadata = tableMetadata.getColumns();
            ImmutableList.Builder<String> allColumnNamesBuilder = ImmutableList.builderWithExpectedSize(columnMetadata.size());
            ImmutableList.Builder<String> hiddenColumnNamesBuilder = ImmutableList.builder();
            for (ColumnMetadata column : columnMetadata) {
                allColumnNamesBuilder.add(column.getName());
                if (column.isHidden()) {
                    hiddenColumnNamesBuilder.add(column.getName());
                }
            }
            List<String> allColumnNames = allColumnNamesBuilder.build();
            List<String> hiddenColumnNames = hiddenColumnNamesBuilder.build();

            verifyColumnNames(allColumnNames, columnNames);
            List<Descriptor.Field> descriptorFields = columnMetadata.stream()
                    .filter(column -> !columnNames.contains(column.getName()) && !column.isHidden())
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toImmutableList());

            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            verifyColumnNames(columnHandles.keySet(), columnNames);
            Map<String, ColumnHandle> assignments = columnHandles.entrySet().stream()
                    .filter(entry -> allColumnNames.contains(entry.getKey()) && !columnNames.contains(entry.getKey()) && !hiddenColumnNames.contains(entry.getKey()))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            checkState(assignments.size() == descriptorFields.size(), "assignments and descriptorFields sizes don't match");
            if (assignments.isEmpty()) {
                throw new TrinoException(GENERIC_USER_ERROR, "Output column is empty");
            }

            TableHandle returnTableHandle = metadata.applyProjection(session, tableHandle, ImmutableList.of(), assignments)
                    .map(ProjectionApplicationResult::getHandle)
                    .orElse(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(new Descriptor(descriptorFields))
                    .handle(new ExceptColumnHandle(
                            createRootCatalogHandle(tableName.getCatalogName(), new CatalogHandle.CatalogVersion("system")),
                            returnTableHandle.getConnectorHandle(),
                            assignments.values().stream().collect(toImmutableList()),
                            returnTableHandle.getTransaction()))
                    .build();
        }
    }

    private static void verifyColumnNames(Collection<String> columnNames, List<String> exceptColumnNames)
    {
        for (String exceptColumnName : exceptColumnNames) {
            if (!exceptColumnName.equals(exceptColumnName.toLowerCase(ENGLISH))) {
                throw new TrinoException(GENERIC_USER_ERROR, "Column name must be lowercase: " + exceptColumnName);
            }

            if (!columnNames.contains(exceptColumnName)) {
                throw new TrinoException(COLUMN_NOT_FOUND, "Column does not exist: " + exceptColumnName);
            }
        }
    }

    public static class ExceptColumnHandle
            implements ConnectorTableFunctionHandle
    {
        private final CatalogHandle catalogHandle;
        private final ConnectorTableHandle tableHandle;
        private final List<ColumnHandle> columnHandles;
        private final ConnectorTransactionHandle transactionHandle;

        @JsonCreator
        public ExceptColumnHandle(
                @JsonProperty("catalogHandle") CatalogHandle catalogHandle,
                @JsonProperty("tableHandle") ConnectorTableHandle tableHandle,
                @JsonProperty("columnHandles") List<ColumnHandle> columnHandles,
                @JsonProperty("transaction") ConnectorTransactionHandle transactionHandle)
        {
            this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
            this.transactionHandle = requireNonNull(transactionHandle, "transactionHandle is null");
        }

        @JsonProperty
        public CatalogHandle getCatalogHandle()
        {
            return catalogHandle;
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }

        @JsonProperty
        public List<ColumnHandle> getColumnHandles()
        {
            return columnHandles;
        }

        @JsonProperty
        public ConnectorTransactionHandle getTransactionHandle()
        {
            return transactionHandle;
        }
    }
}
