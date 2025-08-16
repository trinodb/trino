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
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Output;
import io.trino.sql.analyzer.OutputColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.TableElement;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.UNSUPPORTED_TABLE_TYPE;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeDefaultColumnValue;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.tree.LikeClause.PropertiesOption.EXCLUDING;
import static io.trino.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class CreateTableTask
        implements DataDefinitionTask<CreateTable>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;

    @Inject
    public CreateTableTask(
            PlannerContext plannerContext,
            AccessControl accessControl,
            ColumnPropertyManager columnPropertyManager,
            TablePropertyManager tablePropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "CREATE TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateTable statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        return internalExecute(statement, stateMachine.getSession(), parameters, output -> stateMachine.setOutput(Optional.of(output)), warningCollector);
    }

    @VisibleForTesting
    ListenableFuture<Void> internalExecute(CreateTable statement, Session session, List<Expression> parameters, Consumer<Output> outputConsumer, WarningCollector warningCollector)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");

        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle;
        try {
            tableHandle = plannerContext.getMetadata().getTableHandle(session, tableName);
        }
        catch (TrinoException e) {
            if (e.getErrorCode().equals(UNSUPPORTED_TABLE_TYPE.toErrorCode())) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' of unsupported type already exists", tableName);
            }
            throw e;
        }
        if (tableHandle.isPresent() && statement.getSaveMode() != REPLACE) {
            if (statement.getSaveMode() == FAIL) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
            }
            return immediateVoidFuture();
        }

        String catalogName = tableName.catalogName();
        CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, catalogName);

        Map<String, Object> properties = tablePropertyManager.getProperties(
                catalogName,
                catalogHandle,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                parameterLookup,
                true);

        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<>();
        Map<String, Object> inheritedProperties = ImmutableMap.of();
        boolean includingProperties = false;
        boolean supportsDefaultColumnValue = plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(DEFAULT_COLUMN_VALUE);
        for (TableElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition column) {
                if (column.getName().getParts().size() != 1) {
                    throw semanticException(NOT_SUPPORTED, statement, "Column name '%s' must not be qualified", column.getName());
                }
                Identifier name = getOnlyElement(column.getName().getOriginalParts());
                Type type;
                try {
                    type = plannerContext.getTypeManager().getType(toTypeSignature(column.getType()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", column.getType(), name);
                }
                if (type.equals(UNKNOWN)) {
                    throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", column.getType(), name);
                }
                if (columns.containsKey(name.getValue().toLowerCase(ENGLISH))) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, column, "Column name '%s' specified more than once", name);
                }
                if (column.getDefaultValue().isPresent() && !supportsDefaultColumnValue) {
                    throw semanticException(NOT_SUPPORTED, column, "Catalog '%s' does not support default value for column name '%s'", catalogName, name);
                }
                if (!column.isNullable() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                    throw semanticException(NOT_SUPPORTED, column, "Catalog '%s' does not support non-null column for column name '%s'", catalogName, name);
                }
                Map<String, Object> columnProperties = columnPropertyManager.getProperties(
                        catalogName,
                        catalogHandle,
                        column.getProperties(),
                        session,
                        plannerContext,
                        accessControl,
                        parameterLookup,
                        true);

                Type supportedType = getSupportedType(session, catalogHandle, properties, type);
                column.getDefaultValue().ifPresent(value -> analyzeDefaultColumnValue(session, plannerContext, accessControl, parameterLookup, warningCollector, supportedType, value));
                columns.put(name.getValue().toLowerCase(ENGLISH), ColumnMetadata.builder()
                        .setName(name.getValue().toLowerCase(ENGLISH))
                        .setType(supportedType)
                        .setDefaultValue(column.getDefaultValue().map(Expression::toString))
                        .setNullable(column.isNullable())
                        .setComment(column.getComment())
                        .setProperties(columnProperties)
                        .build());
            }
            else if (element instanceof LikeClause likeClause) {
                QualifiedObjectName originalLikeTableName = createQualifiedObjectName(session, statement, likeClause.getTableName());
                if (plannerContext.getMetadata().getCatalogHandle(session, originalLikeTableName.catalogName()).isEmpty()) {
                    throw semanticException(CATALOG_NOT_FOUND, statement, "LIKE table catalog '%s' not found", originalLikeTableName.catalogName());
                }

                RedirectionAwareTableHandle redirection = plannerContext.getMetadata().getRedirectionAwareTableHandle(session, originalLikeTableName);
                TableHandle likeTable = redirection.tableHandle()
                        .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "LIKE table '%s' does not exist", originalLikeTableName));

                LikeClause.PropertiesOption propertiesOption = likeClause.getPropertiesOption().orElse(EXCLUDING);
                QualifiedObjectName likeTableName = redirection.redirectedTableName().orElse(originalLikeTableName);
                if (propertiesOption == INCLUDING && !catalogName.equals(likeTableName.catalogName())) {
                    if (!originalLikeTableName.equals(likeTableName)) {
                        throw semanticException(
                                NOT_SUPPORTED,
                                statement,
                                "CREATE TABLE LIKE table INCLUDING PROPERTIES across catalogs is not supported. LIKE table '%s' redirected to '%s'.",
                                originalLikeTableName,
                                likeTableName);
                    }
                    throw semanticException(
                            NOT_SUPPORTED,
                            statement,
                            "CREATE TABLE LIKE table INCLUDING PROPERTIES across catalogs is not supported");
                }

                TableMetadata likeTableMetadata = plannerContext.getMetadata().getTableMetadata(session, likeTable);

                if (propertiesOption == INCLUDING) {
                    if (includingProperties) {
                        throw semanticException(NOT_SUPPORTED, statement, "Only one LIKE clause can specify INCLUDING PROPERTIES");
                    }
                    includingProperties = true;
                    inheritedProperties = likeTableMetadata.metadata().getProperties();
                }

                try {
                    accessControl.checkCanSelectFromColumns(
                            session.toSecurityContext(),
                            likeTableName,
                            likeTableMetadata.columns().stream()
                                    .map(ColumnMetadata::getName)
                                    .collect(toImmutableSet()));
                }
                catch (AccessDeniedException e) {
                    throw new AccessDeniedException("Cannot reference columns of table " + likeTableName, e);
                }
                if (propertiesOption == INCLUDING) {
                    try {
                        accessControl.checkCanShowCreateTable(session.toSecurityContext(), likeTableName);
                    }
                    catch (AccessDeniedException e) {
                        throw new AccessDeniedException("Cannot reference properties of table " + likeTableName, e);
                    }
                }

                likeTableMetadata.columns().stream()
                        .filter(column -> !column.isHidden())
                        .forEach(column -> {
                            if (columns.containsKey(column.getName().toLowerCase(Locale.ENGLISH))) {
                                throw semanticException(DUPLICATE_COLUMN_NAME, element, "Column name '%s' specified more than once", column.getName());
                            }
                            columns.put(
                                    column.getName().toLowerCase(Locale.ENGLISH),
                                    ColumnMetadata.builderFrom(column)
                                            .setType(getSupportedType(session, catalogHandle, properties, column.getType()))
                                            .build());
                        });
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }

        Set<String> specifiedPropertyKeys = statement.getProperties().stream()
                // property names are case-insensitive and normalized to lower case
                .map(property -> property.getName().getValue().toLowerCase(ENGLISH))
                .collect(toImmutableSet());
        Map<String, Object> explicitlySetProperties = properties.keySet().stream()
                .peek(key -> verify(key.equals(key.toLowerCase(ENGLISH)), "Property name '%s' not in lower-case", key))
                .filter(specifiedPropertyKeys::contains)
                .collect(toImmutableMap(Function.identity(), properties::get));
        accessControl.checkCanCreateTable(session.toSecurityContext(), tableName, explicitlySetProperties);

        Map<String, Object> finalProperties = combineProperties(specifiedPropertyKeys, properties, inheritedProperties);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.copyOf(columns.values()), finalProperties, statement.getComment());
        try {
            plannerContext.getMetadata().createTable(session, catalogName, tableMetadata, toConnectorSaveMode(statement.getSaveMode()));
        }
        catch (TrinoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || statement.getSaveMode() == FAIL) {
                throw e;
            }
        }
        outputConsumer.accept(new Output(
                catalogName,
                catalogHandle.getVersion(),
                tableName.schemaName(),
                tableName.objectName(),
                Optional.of(tableMetadata.getColumns().stream()
                        .map(column -> new OutputColumn(new Column(column.getName(), column.getType().toString()), ImmutableSet.of()))
                        .collect(toImmutableList()))));
        return immediateVoidFuture();
    }

    private Type getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
    {
        return plannerContext.getMetadata()
                .getSupportedType(session, catalogHandle, tableProperties, type)
                .orElse(type);
    }

    private static Map<String, Object> combineProperties(Set<String> specifiedPropertyKeys, Map<String, Object> defaultProperties, Map<String, Object> inheritedProperties)
    {
        Map<String, Object> finalProperties = new HashMap<>(inheritedProperties);
        for (Map.Entry<String, Object> entry : defaultProperties.entrySet()) {
            if (specifiedPropertyKeys.contains(entry.getKey()) || !finalProperties.containsKey(entry.getKey())) {
                finalProperties.put(entry.getKey(), entry.getValue());
            }
        }
        return finalProperties;
    }

    private static SaveMode toConnectorSaveMode(io.trino.sql.tree.SaveMode saveMode)
    {
        return switch (saveMode) {
            case FAIL -> SaveMode.FAIL;
            case IGNORE -> SaveMode.IGNORE;
            case REPLACE -> SaveMode.REPLACE;
        };
    }
}
