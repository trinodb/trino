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
package io.trino.plugin.paimon.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.paimon.PaimonColumnHandle;
import io.trino.plugin.paimon.PaimonMetadata;
import io.trino.plugin.paimon.PaimonMetadataFactory;
import io.trino.plugin.paimon.PaimonTableHandle;
import io.trino.plugin.paimon.PaimonTableOptionUtils;
import io.trino.plugin.paimon.PaimonTableSupport;
import io.trino.plugin.paimon.PaimonTypeUtils;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
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
import io.trino.spi.function.table.TableArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.predicate.TupleDomain;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.base.util.Functions.checkFunctionArgument;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableSet;

public class TableChangesFunction
        extends AbstractConnectorTableFunction
{
    private static final String FUNCTION_NAME = "table_changes";
    private static final String SCHEMA_NAME_VAR_NAME = "SCHEMA_NAME";
    private static final String TABLE_NAME_VAR_NAME = "TABLE_NAME";
    private static final String INCREMENTAL_BETWEEN_SCAN_MODE = PaimonTableOptionUtils
            .convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN_TIMESTAMP = PaimonTableOptionUtils
            .convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN = PaimonTableOptionUtils
            .convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT = PaimonTableOptionUtils
            .convertOptionKey(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key()).toUpperCase(ENGLISH);
    private static final String INCREMENTAL_TO_AUTO_TAG = PaimonTableOptionUtils
            .convertOptionKey(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key()).toUpperCase(ENGLISH);
    private final PaimonMetadata trinoMetadata;

    @Inject
    public TableChangesFunction(PaimonMetadataFactory trinoMetadataFactory)
    {
        super("system",
                FUNCTION_NAME,
                ImmutableList.of(
                        ScalarArgumentSpecification.builder().name(SCHEMA_NAME_VAR_NAME).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(TABLE_NAME_VAR_NAME).type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(INCREMENTAL_BETWEEN_SCAN_MODE)
                                .defaultValue(
                                        Slices.utf8Slice(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.defaultValue().toString()))
                                .type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(INCREMENTAL_BETWEEN).defaultValue(null)
                                .type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(INCREMENTAL_BETWEEN_TIMESTAMP).defaultValue(null)
                                .type(VARCHAR).build(),
                        ScalarArgumentSpecification.builder().name(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT)
                                .defaultValue(false)
                                .type(BOOLEAN).build(),
                        ScalarArgumentSpecification.builder().name(INCREMENTAL_TO_AUTO_TAG)
                                .defaultValue(null)
                                .type(VARCHAR).build()),
                GENERIC_TABLE);
        this.trinoMetadata = requireNonNull(trinoMetadataFactory, "trinoMetadataFactory is null").create();
    }

    private static String getSchemaName(Map<String, Argument> arguments)
    {
        return getRequiredNonBlankVarcharArgument(arguments, SCHEMA_NAME_VAR_NAME);
    }

    private static String getTableName(Map<String, Argument> arguments)
    {
        return getRequiredNonBlankVarcharArgument(arguments, TABLE_NAME_VAR_NAME);
    }

    private static String getRequiredNonBlankVarcharArgument(Map<String, Argument> arguments, String key)
    {
        String value = getRequiredVarcharArgument(arguments, key).toStringUtf8().strip();
        checkFunctionArgument(!value.isBlank(), "%s argument %s may not be blank", FUNCTION_NAME, key);
        return value;
    }

    private static Slice getRequiredVarcharArgument(Map<String, Argument> arguments, String key)
    {
        Object value = getScalarArgument(arguments, key).getValue();
        if (value == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " argument " + key + " may not be null");
        }
        return checkVarcharArgumentValue(value, key);
    }

    private static Optional<Slice> getOptionalVarcharArgument(Map<String, Argument> arguments, String key)
    {
        Object value = getScalarArgument(arguments, key).getValue();
        if (value == null) {
            return Optional.empty();
        }
        return Optional.of(checkVarcharArgumentValue(value, key));
    }

    private static boolean getRequiredBooleanArgument(Map<String, Argument> arguments, String key)
    {
        Object value = getScalarArgument(arguments, key).getValue();
        if (value == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, FUNCTION_NAME + " argument " + key + " may not be null");
        }
        return checkBooleanArgumentValue(value, key);
    }

    private static ScalarArgument getScalarArgument(Map<String, Argument> arguments, String key)
    {
        Argument argument = requireNonNull(arguments, "arguments is null").get(key);
        if (argument == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, key + " argument not found");
        }
        if (argument instanceof ScalarArgument scalarArgument) {
            return scalarArgument;
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported argument type for " + key + ": " + argumentTypeName(argument));
    }

    private static String argumentTypeName(Argument argument)
    {
        if (argument instanceof TableArgument) {
            return "table";
        }
        return argument.getClass().getName();
    }

    private static Slice checkVarcharArgumentValue(Object argumentValue, String key)
    {
        if (argumentValue instanceof Slice slice) {
            return slice;
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported argument value for " + key + ": " + argumentValue.getClass().getName());
    }

    private static boolean checkBooleanArgumentValue(Object argumentValue, String key)
    {
        if (argumentValue instanceof Boolean bool) {
            return bool;
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported argument value for " + key + ": " + argumentValue.getClass().getName());
    }

    private static String normalizeIncrementalWindow(String argumentName, Slice value)
    {
        String[] parts = value.toStringUtf8().split(",", -1);
        if (parts.length != 2) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    argumentName + " must be two non-empty values separated by a comma");
        }
        String start = parts[0].strip();
        String end = parts[1].strip();
        if (start.isBlank() || end.isBlank()) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    argumentName + " must be two non-empty values separated by a comma");
        }
        return start + "," + end;
    }

    private static String normalizeNonBlankValue(String argumentName, Slice value)
    {
        String normalizedValue = value.toStringUtf8().strip();
        if (normalizedValue.isBlank()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, argumentName + " may not be blank");
        }
        return normalizedValue;
    }

    private static void validateIncrementalBetweenScanMode(String value)
    {
        try {
            Options.fromMap(Map.of(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), value))
                    .get(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Invalid " + INCREMENTAL_BETWEEN_SCAN_MODE + ": " + value,
                    e);
        }
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        requireNonNull(session, "session is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(accessControl, "accessControl is null");
        String schema = getSchemaName(arguments);
        String table = getTableName(arguments);

        Optional<Slice> incrementalBetweenValue = getOptionalVarcharArgument(arguments, INCREMENTAL_BETWEEN);
        Optional<Slice> incrementalBetweenTimestamp = getOptionalVarcharArgument(arguments, INCREMENTAL_BETWEEN_TIMESTAMP);
        boolean incrementalBetweenTagToSnapshot = getRequiredBooleanArgument(arguments, INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT);
        Optional<Slice> incrementalToAutoTag = getOptionalVarcharArgument(arguments, INCREMENTAL_TO_AUTO_TAG);
        int incrementalModeCount = (incrementalBetweenValue.isPresent() ? 1 : 0)
                + (incrementalBetweenTimestamp.isPresent() ? 1 : 0)
                + (incrementalToAutoTag.isPresent() ? 1 : 0);
        if (incrementalModeCount == 0) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "One of " + INCREMENTAL_BETWEEN + ", " + INCREMENTAL_BETWEEN_TIMESTAMP + " or " + INCREMENTAL_TO_AUTO_TAG + " must be provided");
        }
        if (incrementalModeCount > 1) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Only one of " + INCREMENTAL_BETWEEN + ", " + INCREMENTAL_BETWEEN_TIMESTAMP + " or " + INCREMENTAL_TO_AUTO_TAG + " may be provided");
        }
        if (incrementalBetweenTagToSnapshot && incrementalBetweenValue.isEmpty()) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT + " requires " + INCREMENTAL_BETWEEN);
        }
        Optional<String> normalizedIncrementalBetweenValue = incrementalBetweenValue
                .map(value -> normalizeIncrementalWindow(INCREMENTAL_BETWEEN, value));
        Optional<String> normalizedIncrementalBetweenTimestamp = incrementalBetweenTimestamp
                .map(value -> normalizeIncrementalWindow(INCREMENTAL_BETWEEN_TIMESTAMP, value));
        Optional<String> normalizedIncrementalToAutoTag = incrementalToAutoTag
                .map(value -> normalizeNonBlankValue(INCREMENTAL_TO_AUTO_TAG, value));

        Optional<String> incrementalBetweenScanMode = Optional.empty();
        if (incrementalBetweenValue.isPresent() || incrementalBetweenTimestamp.isPresent()) {
            String scanMode = getRequiredNonBlankVarcharArgument(arguments, INCREMENTAL_BETWEEN_SCAN_MODE);
            validateIncrementalBetweenScanMode(scanMode);
            incrementalBetweenScanMode = Optional.of(scanMode);
        }

        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        try {
            PaimonCatalog catalog = trinoMetadata.catalog();
            Catalog sessionCatalog = catalog.forSession(session);
            Table paimonTable = PaimonTableSupport.requireFileStoreTable(
                            sessionCatalog.getTable(Identifier.create(schema, table)),
                            "system.table_changes")
                    .copyWithLatestSchema();
            Map<String, String> options = new HashMap<>();
            if (normalizedIncrementalBetweenValue.isPresent()) {
                options.put(CoreOptions.INCREMENTAL_BETWEEN.key(), normalizedIncrementalBetweenValue.orElseThrow());
            }
            if (normalizedIncrementalBetweenTimestamp.isPresent()) {
                options.put(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP.key(),
                        normalizedIncrementalBetweenTimestamp.orElseThrow());
            }
            incrementalBetweenScanMode.ifPresent(scanMode ->
                    options.put(CoreOptions.INCREMENTAL_BETWEEN_SCAN_MODE.key(), scanMode));
            if (incrementalBetweenTagToSnapshot) {
                options.put(CoreOptions.INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT.key(), "true");
            }
            if (normalizedIncrementalToAutoTag.isPresent()) {
                options.put(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), normalizedIncrementalToAutoTag.orElseThrow());
            }

            ImmutableList.Builder<Descriptor.Field> columns = ImmutableList.builder();
            List<PaimonColumnHandle> projectedColumns = new ArrayList<>();
            paimonTable.rowType().getFields().stream().forEach(column -> {
                columns.add(
                        new Descriptor.Field(column.name(), Optional.of(PaimonTypeUtils.fromPaimonType(
                                column.type(),
                                trinoMetadata.typeManager()))));
                projectedColumns.add(PaimonColumnHandle.of(column.name(), column.type(), trinoMetadata.typeManager()));
            });
            accessControl.checkCanSelectFromColumns(null, schemaTableName, projectedColumns.stream()
                    .map(PaimonColumnHandle::getColumnName)
                    .collect(toUnmodifiableSet()));
            return TableFunctionAnalysis.builder().returnedType(new Descriptor(columns.build()))
                    .handle(new PaimonTableHandle(
                            schema,
                            table,
                            options,
                            TupleDomain.all(),
                            Optional.of(projectedColumns),
                            Optional.empty(),
                            OptionalLong.empty()))
                    .build();
        }
        catch (Catalog.TableNotExistException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Table not found: " + schemaTableName);
        }
        catch (Exception e) {
            throw PaimonMetadata.paimonMetadataException(
                    "Failed to analyze Paimon table_changes for " + schemaTableName, e);
        }
    }
}
