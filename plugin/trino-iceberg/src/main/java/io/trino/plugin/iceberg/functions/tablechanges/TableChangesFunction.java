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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergUtil;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.util.SnapshotUtil;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_ORDINAL_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_ORDINAL_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TIMESTAMP_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TIMESTAMP_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TYPE_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TYPE_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_VERSION_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_VERSION_NAME;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final Logger log = Logger.get(TableChangesFunction.class);

    private static final String FUNCTION_NAME = "table_changes";
    @Deprecated
    private static final String SCHEMA_VAR_NAME = "SCHEMA";
    private static final String SCHEMA_NAME_VAR_NAME = "SCHEMA_NAME";
    @Deprecated
    private static final String TABLE_VAR_NAME = "TABLE";
    private static final String TABLE_NAME_VAR_NAME = "TABLE_NAME";
    private static final String START_SNAPSHOT_VAR_NAME = "START_SNAPSHOT_ID";
    private static final String END_SNAPSHOT_VAR_NAME = "END_SNAPSHOT_ID";

    private final TrinoCatalogFactory trinoCatalogFactory;
    private final TypeManager typeManager;

    @Inject
    public TableChangesFunction(TrinoCatalogFactory trinoCatalogFactory, TypeManager typeManager)
    {
        super(
                "system",
                FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_VAR_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(TABLE_VAR_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(START_SNAPSHOT_VAR_NAME)
                                .type(BIGINT)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(END_SNAPSHOT_VAR_NAME)
                                .type(BIGINT)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(TABLE_NAME_VAR_NAME)
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                GENERIC_TABLE);

        this.trinoCatalogFactory = requireNonNull(trinoCatalogFactory, "trinoCatalogFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl)
    {
        String schema = getSchemaName(arguments);
        String table = getTableName(arguments);

        long startSnapshotId = (long) checkNonNull(((ScalarArgument) arguments.get(START_SNAPSHOT_VAR_NAME)).getValue());
        long endSnapshotId = (long) checkNonNull(((ScalarArgument) arguments.get(END_SNAPSHOT_VAR_NAME)).getValue());

        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        Table icebergTable = trinoCatalogFactory.create(session.getIdentity())
                .loadTable(session, schemaTableName);

        checkSnapshotExists(icebergTable, startSnapshotId);
        checkSnapshotExists(icebergTable, endSnapshotId);
        if (!SnapshotUtil.isParentAncestorOf(icebergTable, endSnapshotId, startSnapshotId)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Starting snapshot (exclusive) %s is not a parent ancestor of end snapshot %s".formatted(startSnapshotId, endSnapshotId));
        }

        ImmutableList.Builder<Descriptor.Field> columns = ImmutableList.builder();
        Schema tableSchema = icebergTable.schemas().get(icebergTable.snapshot(endSnapshotId).schemaId());
        tableSchema.columns().stream()
                .map(column -> new Descriptor.Field(column.name(), Optional.of(toTrinoType(column.type(), typeManager))))
                .forEach(columns::add);

        columns.add(new Descriptor.Field(DATA_CHANGE_TYPE_NAME, Optional.of(VARCHAR)));
        columns.add(new Descriptor.Field(DATA_CHANGE_VERSION_NAME, Optional.of(BIGINT)));
        columns.add(new Descriptor.Field(DATA_CHANGE_TIMESTAMP_NAME, Optional.of(TIMESTAMP_TZ_MILLIS)));
        columns.add(new Descriptor.Field(DATA_CHANGE_ORDINAL_NAME, Optional.of(INTEGER)));

        ImmutableList.Builder<IcebergColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        IcebergUtil.getColumns(tableSchema, typeManager).forEach(columnHandlesBuilder::add);
        columnHandlesBuilder.add(new IcebergColumnHandle(
                new ColumnIdentity(DATA_CHANGE_TYPE_ID, DATA_CHANGE_TYPE_NAME, PRIMITIVE, ImmutableList.of()),
                VARCHAR,
                ImmutableList.of(),
                VARCHAR,
                false,
                Optional.empty()));
        columnHandlesBuilder.add(new IcebergColumnHandle(
                new ColumnIdentity(DATA_CHANGE_VERSION_ID, DATA_CHANGE_VERSION_NAME, PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                false,
                Optional.empty()));
        columnHandlesBuilder.add(new IcebergColumnHandle(
                new ColumnIdentity(DATA_CHANGE_TIMESTAMP_ID, DATA_CHANGE_TIMESTAMP_NAME, PRIMITIVE, ImmutableList.of()),
                TIMESTAMP_TZ_MILLIS,
                ImmutableList.of(),
                TIMESTAMP_TZ_MILLIS,
                false,
                Optional.empty()));
        columnHandlesBuilder.add(new IcebergColumnHandle(
                new ColumnIdentity(DATA_CHANGE_ORDINAL_ID, DATA_CHANGE_ORDINAL_NAME, PRIMITIVE, ImmutableList.of()),
                INTEGER,
                ImmutableList.of(),
                INTEGER,
                false,
                Optional.empty()));
        List<IcebergColumnHandle> columnHandles = columnHandlesBuilder.build();

        accessControl.checkCanSelectFromColumns(null, schemaTableName, columnHandles.stream()
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableSet()));

        return TableFunctionAnalysis.builder()
                .returnedType(new Descriptor(columns.build()))
                .handle(new TableChangesFunctionHandle(
                        schemaTableName,
                        SchemaParser.toJson(tableSchema),
                        columnHandles,
                        Optional.ofNullable(icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING)),
                        startSnapshotId,
                        endSnapshotId))
                .build();
    }

    private static String getSchemaName(Map<String, Argument> arguments)
    {
        if (argumentExists(arguments, SCHEMA_VAR_NAME) && argumentExists(arguments, SCHEMA_NAME_VAR_NAME)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot use both " + SCHEMA_VAR_NAME + " and " + SCHEMA_NAME_VAR_NAME + " arguments");
        }
        if (argumentExists(arguments, SCHEMA_VAR_NAME)) {
            log.warn("%s argument is deprecated. Use %s instead.", SCHEMA_VAR_NAME, SCHEMA_NAME_VAR_NAME);
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(SCHEMA_VAR_NAME)).getValue())).toStringUtf8();
        }
        if (argumentExists(arguments, SCHEMA_NAME_VAR_NAME)) {
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(SCHEMA_NAME_VAR_NAME)).getValue())).toStringUtf8();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, SCHEMA_NAME_VAR_NAME + " argument not found");
    }

    private static String getTableName(Map<String, Argument> arguments)
    {
        if (argumentExists(arguments, TABLE_VAR_NAME) && argumentExists(arguments, TABLE_NAME_VAR_NAME)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot use both " + TABLE_VAR_NAME + " and " + TABLE_NAME_VAR_NAME + " arguments");
        }
        if (argumentExists(arguments, TABLE_VAR_NAME)) {
            log.warn("%s argument is deprecated. Use %s instead.", TABLE_VAR_NAME, TABLE_NAME_VAR_NAME);
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(TABLE_VAR_NAME)).getValue())).toStringUtf8();
        }
        if (argumentExists(arguments, TABLE_NAME_VAR_NAME)) {
            return ((Slice) checkNonNull(((ScalarArgument) arguments.get(TABLE_NAME_VAR_NAME)).getValue())).toStringUtf8();
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, TABLE_NAME_VAR_NAME + " argument not found");
    }

    private static boolean argumentExists(Map<String, Argument> arguments, String key)
    {
        Argument argument = arguments.get(key);
        if (argument instanceof ScalarArgument scalarArgument) {
            return !scalarArgument.getNullableValue().isNull();
        }
        throw new IllegalArgumentException("Unsupported argument type: " + argument);
    }

    private static Object checkNonNull(Object argumentValue)
    {
        if (argumentValue == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " arguments may not be null");
        }
        return argumentValue;
    }

    private static void checkSnapshotExists(Table icebergTable, long snapshotId)
    {
        if (icebergTable.snapshot(snapshotId) == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Snapshot not found in Iceberg table history: " + snapshotId);
        }
    }
}
