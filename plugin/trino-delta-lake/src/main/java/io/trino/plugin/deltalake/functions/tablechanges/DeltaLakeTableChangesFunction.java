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
package io.trino.plugin.deltalake.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.ptf.AbstractConnectorTableFunction;
import io.trino.spi.ptf.Argument;
import io.trino.spi.ptf.Descriptor;
import io.trino.spi.ptf.ScalarArgument;
import io.trino.spi.ptf.ScalarArgumentSpecification;
import io.trino.spi.ptf.TableFunctionAnalysis;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.toColumnHandle;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.ptf.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DeltaLakeTableChangesFunction
        extends AbstractConnectorTableFunction
{
    public static final String TABLE_SCHEMA_NAME_ARGUMENT = "TABLE_SCHEMA_NAME";
    public static final String TABLE_NAME_ARGUMENT = "TABLE_NAME";
    public static final String START_VERSION_ARGUMENT = "START_VERSION";
    public static final String END_VERSION_ARGUMENT = "END_VERSION";
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "table_changes";
    public static final SchemaFunctionName TABLE_CHANGES_NAME = new SchemaFunctionName(SCHEMA_NAME, NAME);
    public static final String CHANGE_TYPE_COLUMN_NAME = "_change_type";
    public static final String COMMIT_VERSION_COLUMN_NAME = "_commit_version";
    public static final String COMMIT_TIMESTAMP_COLUMN_NAME = "_commit_timestamp";

    private final TypeManager typeManager;
    private final DeltaLakeMetadataFactory deltaLakeMetadataFactory;

    public DeltaLakeTableChangesFunction(TypeManager typeManager, DeltaLakeMetadataFactory deltaLakeMetadataFactory)
    {
        super(
                SCHEMA_NAME,
                NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(TABLE_SCHEMA_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_ARGUMENT).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(START_VERSION_ARGUMENT).type(BIGINT).defaultValue(null).build(),
                        ScalarArgumentSpecification.builder().name(END_VERSION_ARGUMENT).type(BIGINT).defaultValue(Long.MAX_VALUE).build()),
                GENERIC_TABLE);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.deltaLakeMetadataFactory = requireNonNull(deltaLakeMetadataFactory, "deltaLakeMetadataFactory is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, CatalogHandle catalogHandle, ConnectorTransactionHandle transaction, Map<String, Argument> arguments)
    {
        ScalarArgument schemaNameArgument = (ScalarArgument) arguments.get(TABLE_SCHEMA_NAME_ARGUMENT);
        requireNonNull(schemaNameArgument.getValue(), "schemaName value for function table_changes() is null");
        String schemaName = ((Slice) schemaNameArgument.getValue()).toStringUtf8();

        ScalarArgument tableNameArgument = (ScalarArgument) arguments.get(TABLE_NAME_ARGUMENT);
        requireNonNull(tableNameArgument.getValue(), "tableName value for function table_changes() is null");
        String tableName = ((Slice) tableNameArgument.getValue()).toStringUtf8();

        ScalarArgument startVersionArgument = (ScalarArgument) arguments.get(START_VERSION_ARGUMENT);
        Long startVersion = (Long) startVersionArgument.getValue();
        if (startVersion == null) {
            startVersion = 0L;
        }
        else {
            startVersion++; // to ensure that the startVersion is exclusive
        }
        checkArgument(startVersion >= 0, "startVersion value for function table_changes() must not be negative");

        ScalarArgument endVersionArgument = (ScalarArgument) arguments.get(END_VERSION_ARGUMENT);
        Long endVersion = (Long) endVersionArgument.getValue();
        requireNonNull(endVersion, "endVersion value for function table_changes() is null");
        if (endVersion < 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid value of endVersion " + endVersion + ", endVersion cannot be negative");
        }
        if (endVersion < startVersion) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                    format("Invalid arguments endVersion: %s, startVersion: %s. startVersion can't be greater then endVersion",
                            endVersion,
                            startVersion));
        }

        MetadataEntry metadata = getTableMetadata(session, schemaName, tableName);

        ImmutableList.Builder<Descriptor.Field> outputFields = ImmutableList.builder();
        List<DeltaLakeColumnMetadata> deltaLakeColumnMetadata = extractSchema(metadata, typeManager);
        deltaLakeColumnMetadata.stream()
                .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                .forEach(outputFields::add);
        outputFields.add(new Descriptor.Field(CHANGE_TYPE_COLUMN_NAME, Optional.of(VARCHAR)));
        outputFields.add(new Descriptor.Field(COMMIT_VERSION_COLUMN_NAME, Optional.of(INTEGER)));
        outputFields.add(new Descriptor.Field(COMMIT_TIMESTAMP_COLUMN_NAME, Optional.of(TIMESTAMP_TZ_MILLIS)));
        List<Descriptor.Field> requiredColumns = outputFields.build();

        List<DeltaLakeColumnHandle> columns = deltaLakeColumnMetadata.stream()
                .map(column -> toColumnHandle(
                        column.getColumnMetadata(),
                        column.getFieldId(),
                        column.getPhysicalName(),
                        column.getPhysicalColumnType(),
                        metadata.getCanonicalPartitionColumns()))
                .collect(toImmutableList());

        return TableFunctionAnalysis.builder()
                .handle(new TableChangesFunctionTableHandle(catalogHandle, schemaName, tableName, startVersion, endVersion, columns))
                .returnedType(new Descriptor(requiredColumns))
                .build();
    }

    private MetadataEntry getTableMetadata(ConnectorSession session, String schemaName, String tableName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        DeltaLakeTableHandle tableHandle = deltaLakeMetadataFactory.create(session.getIdentity())
                .getTableHandle(session, schemaTableName);
        requireNonNull(tableHandle, "tableHandle is null");
        MetadataEntry metadataEntry = tableHandle.getMetadataEntry();
        if (metadataEntry == null) {
            throw new TrinoException(NOT_FOUND, "metadata for " + schemaTableName + " is not available");
        }
        return metadataEntry;
    }
}
