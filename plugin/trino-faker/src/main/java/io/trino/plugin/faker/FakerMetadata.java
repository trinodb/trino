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

package io.trino.plugin.faker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_COLUMN_REFERENCE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FakerMetadata
        implements ConnectorMetadata
{
    public static final String SCHEMA_NAME = "default";
    public static final String ROW_ID_COLUMN_NAME = "$row_id";

    @GuardedBy("this")
    private final List<SchemaInfo> schemas = new ArrayList<>();
    private final double nullProbability;
    private final long defaultLimit;

    @GuardedBy("this")
    private final Map<SchemaTableName, TableInfo> tables = new HashMap<>();

    private final FakerFunctionProvider functionsProvider;

    @Inject
    public FakerMetadata(FakerConfig config, FakerFunctionProvider functionProvider)
    {
        this.schemas.add(new SchemaInfo(SCHEMA_NAME, Map.of()));
        this.nullProbability = config.getNullProbability();
        this.defaultLimit = config.getDefaultLimit();
        this.functionsProvider = requireNonNull(functionProvider, "functionProvider is null");
    }

    @Override
    public synchronized List<String> listSchemaNames(ConnectorSession connectorSession)
    {
        return schemas.stream()
                .map(SchemaInfo::name)
                .collect(toImmutableList());
    }

    @Override
    public synchronized void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        if (schemas.stream().anyMatch(schema -> schema.name().equals(schemaName))) {
            throw new TrinoException(SCHEMA_ALREADY_EXISTS, format("Schema '%s' already exists", schemaName));
        }
        schemas.add(new SchemaInfo(schemaName, properties));
    }

    @Override
    public synchronized void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        verify(schemas.remove(getSchema(schemaName)));
    }

    private synchronized SchemaInfo getSchema(String name)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(schemaInfo -> schemaInfo.name().equals(name))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", name));
        }
        return schema.get();
    }

    @Override
    public synchronized ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        TableInfo tableInfo = tables.get(tableName);
        if (tableInfo == null) {
            return null;
        }
        long schemaLimit = (long) schema.properties().getOrDefault(SchemaInfo.DEFAULT_LIMIT_PROPERTY, defaultLimit);
        long tableLimit = (long) tables.get(tableName).properties().getOrDefault(TableInfo.DEFAULT_LIMIT_PROPERTY, schemaLimit);
        return new FakerTableHandle(tableName, TupleDomain.all(), tableLimit);
    }

    @Override
    public synchronized ConnectorTableMetadata getTableMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        SchemaTableName name = tableHandle.schemaTableName();
        TableInfo tableInfo = tables.get(tableHandle.schemaTableName());
        return new ConnectorTableMetadata(
                name,
                tableInfo.columns().stream().map(ColumnInfo::metadata).collect(toImmutableList()),
                tableInfo.properties(),
                tableInfo.comment());
    }

    @Override
    public synchronized List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.keySet().stream()
                .filter(table -> schemaName.map(table.getSchemaName()::contentEquals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public synchronized Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        return tables.get(tableHandle.schemaTableName())
                .columns().stream()
                .collect(toImmutableMap(ColumnInfo::name, ColumnInfo::handle));
    }

    @Override
    public synchronized ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession,
            ConnectorTableHandle connectorTableHandle,
            ColumnHandle columnHandle)
    {
        FakerTableHandle tableHandle = (FakerTableHandle) connectorTableHandle;
        return tables.get(tableHandle.schemaTableName())
                .column(columnHandle)
                .metadata();
    }

    @Override
    public synchronized Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return tables.entrySet().stream()
                .filter(entry -> prefix.matches(entry.getKey()))
                .map(entry -> TableColumnsMetadata.forTable(
                        entry.getKey(),
                        entry.getValue().columns().stream().map(ColumnInfo::metadata).collect(toImmutableList())))
                .iterator();
    }

    @Override
    public synchronized void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        tables.remove(handle.schemaTableName());
    }

    @Override
    public synchronized void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        checkSchemaExists(newTableName.getSchemaName());
        checkTableNotExists(newTableName);

        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName oldTableName = handle.schemaTableName();

        TableInfo oldInfo = tables.get(oldTableName);
        tables.remove(oldTableName);
        tables.put(newTableName, oldInfo);
    }

    @Override
    public synchronized void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = tables.get(tableName);
        ImmutableMap.Builder updatedProperties = ImmutableMap.<String, Object>builder().putAll(oldInfo.properties());
        if (properties.containsKey(TableInfo.NULL_PROBABILITY_PROPERTY)) {
            double nullProbability = (double) properties.get(TableInfo.NULL_PROBABILITY_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The null_probability property cannot be empty"));
            updatedProperties.put(TableInfo.NULL_PROBABILITY_PROPERTY, nullProbability);
        }
        if (properties.containsKey(TableInfo.DEFAULT_LIMIT_PROPERTY)) {
            long defaultLimit = (long) properties.get(TableInfo.DEFAULT_LIMIT_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The default_limit property cannot be empty"));
            updatedProperties.put(TableInfo.DEFAULT_LIMIT_PROPERTY, defaultLimit);
        }
        tables.put(tableName, oldInfo.withProperties(updatedProperties.buildOrThrow()));
    }

    @Override
    public synchronized void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = requireNonNull(tables.get(tableName), "tableInfo is null");
        tables.put(tableName, oldInfo.withComment(comment));
    }

    @Override
    public synchronized void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        FakerTableHandle handle = (FakerTableHandle) tableHandle;
        SchemaTableName tableName = handle.schemaTableName();

        TableInfo oldInfo = tables.get(tableName);
        List<ColumnInfo> columns = oldInfo.columns().stream()
                .map(columnInfo -> {
                    if (columnInfo.handle().equals(column)) {
                        if (ROW_ID_COLUMN_NAME.equals(columnInfo.handle().name())) {
                            throw new TrinoException(INVALID_COLUMN_REFERENCE, "Cannot set comment for %s column".formatted(ROW_ID_COLUMN_NAME));
                        }
                        return columnInfo.withComment(comment);
                    }
                    return columnInfo;
                })
                .collect(toImmutableList());
        tables.put(tableName, oldInfo.withColumns(columns));
    }

    @Override
    public synchronized void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        if (saveMode == SaveMode.REPLACE) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        ConnectorOutputTableHandle outputTableHandle = beginCreateTable(session, tableMetadata, Optional.empty(), NO_RETRIES, false);
        finishCreateTable(session, outputTableHandle, ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public synchronized FakerOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        if (replace) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support replacing tables");
        }
        SchemaTableName tableName = tableMetadata.getTable();
        SchemaInfo schema = getSchema(tableName.getSchemaName());
        checkTableNotExists(tableMetadata.getTable());

        double schemaNullProbability = (double) schema.properties().getOrDefault(SchemaInfo.NULL_PROBABILITY_PROPERTY, nullProbability);
        double tableNullProbability = (double) tableMetadata.getProperties().getOrDefault(TableInfo.NULL_PROBABILITY_PROPERTY, schemaNullProbability);

        ImmutableList.Builder<ColumnInfo> columns = ImmutableList.builder();
        int columnId = 0;
        for (; columnId < tableMetadata.getColumns().size(); columnId++) {
            ColumnMetadata column = tableMetadata.getColumns().get(columnId);
            double nullProbability = 0;
            if (column.isNullable()) {
                nullProbability = (double) column.getProperties().getOrDefault(ColumnInfo.NULL_PROBABILITY_PROPERTY, tableNullProbability);
            }
            String generator = (String) column.getProperties().get(ColumnInfo.GENERATOR_PROPERTY);
            if (generator != null && !isCharacterColumn(column)) {
                throw new TrinoException(INVALID_COLUMN_PROPERTY, "The `generator` property can only be set for CHAR, VARCHAR or VARBINARY columns");
            }
            columns.add(new ColumnInfo(
                    new FakerColumnHandle(
                            columnId,
                            column.getName(),
                            column.getType(),
                            nullProbability,
                            generator),
                    column));
        }

        columns.add(new ColumnInfo(
                new FakerColumnHandle(
                        columnId,
                        ROW_ID_COLUMN_NAME,
                        BigintType.BIGINT,
                        0,
                        ""),
                ColumnMetadata.builder()
                        .setName(ROW_ID_COLUMN_NAME)
                        .setType(BigintType.BIGINT)
                        .setHidden(true)
                        .setNullable(false)
                        .build()));

        tables.put(tableName, new TableInfo(
                columns.build(),
                tableMetadata.getProperties(),
                tableMetadata.getComment()));

        return new FakerOutputTableHandle(tableName);
    }

    private boolean isCharacterColumn(ColumnMetadata column)
    {
        return column.getType() instanceof CharType || column.getType() instanceof VarcharType || column.getType() instanceof VarbinaryType;
    }

    private synchronized void checkSchemaExists(String schemaName)
    {
        if (schemas.stream().noneMatch(schema -> schema.name().equals(schemaName))) {
            throw new SchemaNotFoundException(schemaName);
        }
    }

    private synchronized void checkTableNotExists(SchemaTableName tableName)
    {
        if (tables.containsKey(tableName)) {
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table '%s' already exists", tableName));
        }
    }

    @Override
    public synchronized Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        FakerOutputTableHandle fakerOutputHandle = (FakerOutputTableHandle) tableHandle;

        SchemaTableName tableName = fakerOutputHandle.schemaTableName();

        TableInfo info = tables.get(tableName);
        requireNonNull(info, "info is null");

        // TODO ensure fragments is empty?

        tables.put(tableName, new TableInfo(info.columns(), info.properties(), info.comment()));
        return Optional.empty();
    }

    @Override
    public synchronized Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        Optional<SchemaInfo> schema = schemas.stream()
                .filter(s -> s.name().equals(schemaName))
                .findAny();
        if (schema.isEmpty()) {
            throw new TrinoException(SCHEMA_NOT_FOUND, format("Schema '%s' does not exist", schemaName));
        }

        return schema.get().properties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle table,
            long limit)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;
        if (fakerTable.limit() == limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(
                fakerTable.withLimit(limit),
                false,
                true));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint constraint)
    {
        FakerTableHandle fakerTable = (FakerTableHandle) table;

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) {
            return Optional.empty();
        }
        // the only reason not to use isNone is so the linter doesn't complain about not checking an Optional
        if (summary.getDomains().isEmpty()) {
            throw new IllegalArgumentException("summary cannot be none");
        }

        TupleDomain<ColumnHandle> currentConstraint = fakerTable.constraint();
        if (currentConstraint.getDomains().isEmpty()) {
            throw new IllegalArgumentException("currentConstraint is none but should be all!");
        }

        // push down everything, unsupported constraints will throw an exception during data generation
        boolean anyUpdated = false;
        for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().get().entrySet()) {
            FakerColumnHandle column = (FakerColumnHandle) entry.getKey();
            Domain domain = entry.getValue();

            if (currentConstraint.getDomains().get().containsKey(column)) {
                Domain currentDomain = currentConstraint.getDomains().get().get(column);
                // it is important to avoid processing same constraint multiple times
                // so that planner doesn't get stuck in a loop
                if (currentDomain.equals(domain)) {
                    continue;
                }
                // TODO write test cases for this, it doesn't seem to work with IS NULL
                currentDomain.union(domain);
            }
            else {
                Map<ColumnHandle, Domain> domains = new HashMap<>(currentConstraint.getDomains().get());
                domains.put(column, domain);
                currentConstraint = TupleDomain.withColumnDomains(domains);
            }
            anyUpdated = true;
        }
        if (!anyUpdated) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                fakerTable.withConstraint(currentConstraint),
                TupleDomain.all(),
                constraint.getExpression(),
                true));
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return functionsProvider.functionsMetadata();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        return functionsProvider.functionsMetadata().stream()
                .filter(function -> function.getCanonicalName().equals(name.getFunctionName()))
                .collect(toImmutableList());
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return functionsProvider.functionsMetadata().stream()
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown function " + functionId));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }
}
