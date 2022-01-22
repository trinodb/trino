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
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Output;
import io.trino.sql.analyzer.OutputColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.TableElement;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
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
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.sql.tree.LikeClause.PropertiesOption.EXCLUDING;
import static io.trino.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CreateTableTask
        implements DataDefinitionTask<CreateTable>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final ColumnPropertyManager columnPropertyManager;
    private final TablePropertyManager tablePropertyManager;
    private final boolean disableSetPropertiesSecurityCheckForCreateDdl;

    @Inject
    public CreateTableTask(
            PlannerContext plannerContext,
            AccessControl accessControl,
            ColumnPropertyManager columnPropertyManager,
            TablePropertyManager tablePropertyManager,
            FeaturesConfig featuresConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.disableSetPropertiesSecurityCheckForCreateDdl = featuresConfig.isDisableSetPropertiesSecurityCheckForCreateDdl();
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
        return internalExecute(statement, stateMachine.getSession(), parameters, output -> stateMachine.setOutput(Optional.of(output)));
    }

    @VisibleForTesting
    ListenableFuture<Void> internalExecute(CreateTable statement, Session session, List<Expression> parameters, Consumer<Output> outputConsumer)
    {
        checkArgument(!statement.getElements().isEmpty(), "no columns for table");

        Map<NodeRef<Parameter>, Expression> parameterLookup = parameterExtractor(statement, parameters);
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = plannerContext.getMetadata().getTableHandle(session, tableName);
        if (tableHandle.isPresent()) {
            if (!statement.isNotExists()) {
                throw semanticException(TABLE_ALREADY_EXISTS, statement, "Table '%s' already exists", tableName);
            }
            return immediateVoidFuture();
        }

        CatalogName catalogName = getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, tableName.getCatalogName());

        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<>();
        Map<String, Object> inheritedProperties = ImmutableMap.of();
        boolean includingProperties = false;
        for (TableElement element : statement.getElements()) {
            if (element instanceof ColumnDefinition) {
                ColumnDefinition column = (ColumnDefinition) element;
                String name = column.getName().getValue().toLowerCase(Locale.ENGLISH);
                Type type;
                try {
                    type = plannerContext.getTypeManager().getType(toTypeSignature(column.getType()));
                }
                catch (TypeNotFoundException e) {
                    throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (type.equals(UNKNOWN)) {
                    throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", column.getType(), column.getName());
                }
                if (columns.containsKey(name)) {
                    throw semanticException(DUPLICATE_COLUMN_NAME, column, "Column name '%s' specified more than once", column.getName());
                }
                if (!column.isNullable() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogName).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                    throw semanticException(NOT_SUPPORTED, column, "Catalog '%s' does not support non-null column for column name '%s'", catalogName.getCatalogName(), column.getName());
                }
                Map<String, Object> columnProperties = columnPropertyManager.getProperties(
                        catalogName,
                        column.getProperties(),
                        session,
                        plannerContext,
                        accessControl,
                        parameterLookup,
                        true);

                columns.put(name, ColumnMetadata.builder()
                        .setName(name)
                        .setType(type)
                        .setNullable(column.isNullable())
                        .setComment(column.getComment())
                        .setProperties(columnProperties)
                        .build());
            }
            else if (element instanceof LikeClause) {
                LikeClause likeClause = (LikeClause) element;
                QualifiedObjectName originalLikeTableName = createQualifiedObjectName(session, statement, likeClause.getTableName());
                if (plannerContext.getMetadata().getCatalogHandle(session, originalLikeTableName.getCatalogName()).isEmpty()) {
                    throw semanticException(CATALOG_NOT_FOUND, statement, "LIKE table catalog '%s' does not exist", originalLikeTableName.getCatalogName());
                }

                RedirectionAwareTableHandle redirection = plannerContext.getMetadata().getRedirectionAwareTableHandle(session, originalLikeTableName);
                TableHandle likeTable = redirection.getTableHandle()
                        .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "LIKE table '%s' does not exist", originalLikeTableName));

                QualifiedObjectName likeTableName = redirection.getRedirectedTableName().orElse(originalLikeTableName);
                if (!tableName.getCatalogName().equals(likeTableName.getCatalogName())) {
                    String message = "CREATE TABLE LIKE across catalogs is not supported";
                    if (!originalLikeTableName.equals(likeTableName)) {
                        message += format(". LIKE table '%s' redirected to '%s'.", originalLikeTableName, likeTableName);
                    }
                    throw semanticException(NOT_SUPPORTED, statement, message);
                }

                TableMetadata likeTableMetadata = plannerContext.getMetadata().getTableMetadata(session, likeTable);

                Optional<LikeClause.PropertiesOption> propertiesOption = likeClause.getPropertiesOption();
                if (propertiesOption.isPresent() && propertiesOption.get() == LikeClause.PropertiesOption.INCLUDING) {
                    if (includingProperties) {
                        throw semanticException(NOT_SUPPORTED, statement, "Only one LIKE clause can specify INCLUDING PROPERTIES");
                    }
                    includingProperties = true;
                    inheritedProperties = likeTableMetadata.getMetadata().getProperties();
                }

                try {
                    accessControl.checkCanSelectFromColumns(
                            session.toSecurityContext(),
                            likeTableName,
                            likeTableMetadata.getColumns().stream()
                                    .map(ColumnMetadata::getName)
                                    .collect(toImmutableSet()));
                }
                catch (AccessDeniedException e) {
                    throw new AccessDeniedException("Cannot reference columns of table " + likeTableName);
                }
                if (propertiesOption.orElse(EXCLUDING) == INCLUDING) {
                    try {
                        accessControl.checkCanShowCreateTable(session.toSecurityContext(), likeTableName);
                    }
                    catch (AccessDeniedException e) {
                        throw new AccessDeniedException("Cannot reference properties of table " + likeTableName);
                    }
                }

                likeTableMetadata.getColumns().stream()
                        .filter(column -> !column.isHidden())
                        .forEach(column -> {
                            if (columns.containsKey(column.getName().toLowerCase(Locale.ENGLISH))) {
                                throw semanticException(DUPLICATE_COLUMN_NAME, element, "Column name '%s' specified more than once", column.getName());
                            }
                            columns.put(column.getName().toLowerCase(Locale.ENGLISH), column);
                        });
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid TableElement: " + element.getClass().getName());
            }
        }
        Map<String, Object> properties = tablePropertyManager.getProperties(
                catalogName,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                parameterLookup,
                true);

        if (!disableSetPropertiesSecurityCheckForCreateDdl) {
            accessControl.checkCanCreateTable(session.toSecurityContext(), tableName, properties);
        }
        else {
            accessControl.checkCanCreateTable(session.toSecurityContext(), tableName);
        }

        Set<String> specifiedPropertyKeys = statement.getProperties().stream()
                .map(property -> property.getName().getValue())
                .collect(toImmutableSet());
        Map<String, Object> finalProperties = combineProperties(specifiedPropertyKeys, properties, inheritedProperties);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(tableName.asSchemaTableName(), ImmutableList.copyOf(columns.values()), finalProperties, statement.getComment());
        try {
            plannerContext.getMetadata().createTable(session, tableName.getCatalogName(), tableMetadata, statement.isNotExists());
        }
        catch (TrinoException e) {
            // connectors are not required to handle the ignoreExisting flag
            if (!e.getErrorCode().equals(ALREADY_EXISTS.toErrorCode()) || !statement.isNotExists()) {
                throw e;
            }
        }
        outputConsumer.accept(new Output(
                tableName.getCatalogName(),
                tableName.getSchemaName(),
                tableName.getObjectName(),
                Optional.of(tableMetadata.getColumns().stream()
                        .map(column -> new OutputColumn(new Column(column.getName(), column.getType().toString()), ImmutableSet.of()))
                        .collect(toImmutableList()))));
        return immediateVoidFuture();
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
}
