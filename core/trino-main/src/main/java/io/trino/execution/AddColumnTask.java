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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableMetadata;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.ColumnPosition;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeDefaultColumnValue;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class AddColumnTask
        implements DataDefinitionTask<AddColumn>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final ColumnPropertyManager columnPropertyManager;

    @Inject
    public AddColumnTask(PlannerContext plannerContext, AccessControl accessControl, ColumnPropertyManager columnPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.columnPropertyManager = requireNonNull(columnPropertyManager, "columnPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public ListenableFuture<Void> execute(
            AddColumn statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getName());
        RedirectionAwareTableHandle redirectionAwareTableHandle = plannerContext.getMetadata().getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        CatalogHandle catalogHandle = tableHandle.catalogHandle();

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.redirectedTableName().orElse(originalTableName);

        TableMetadata tableMetadata = plannerContext.getMetadata().getTableMetadata(session, tableHandle);
        Map<String, ColumnMetadata> columns = tableMetadata.columns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, identity()));

        ColumnDefinition element = statement.getColumn();
        Identifier columnName = element.getName().getOriginalParts().get(0);
        ColumnPosition position = statement.getPosition().orElse(new ColumnPosition.Last());
        Type type;
        try {
            type = plannerContext.getTypeManager().getType(toTypeSignature(element.getType()));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", element.getType(), columnName);
        }

        if (element.getName().getParts().size() == 1) {
            accessControl.checkCanAddColumns(session.toSecurityContext(), qualifiedTableName);

            if (type.equals(UNKNOWN)) {
                throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", element.getType(), columnName);
            }
            if (columns.containsKey(columnName.getValue().toLowerCase(ENGLISH))) {
                if (!statement.isColumnNotExists()) {
                    throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", columnName);
                }
                return immediateVoidFuture();
            }
            if (element.getDefaultValue().isPresent() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(DEFAULT_COLUMN_VALUE)) {
                throw semanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support default value for column name '%s'", catalogHandle, columnName);
            }
            if (!element.isNullable() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                throw semanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", catalogHandle, columnName);
            }
            if (position instanceof ColumnPosition.After after && !columns.containsKey(after.column().getValue().toLowerCase(ENGLISH))) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not", after.column().getValue());
            }

            Map<String, Object> columnProperties = columnPropertyManager.getProperties(
                    catalogHandle.getCatalogName().toString(),
                    catalogHandle,
                    element.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    parameterLookup,
                    true);

            Type supportedType = getSupportedType(session, catalogHandle, tableMetadata.metadata().getProperties(), type);
            element.getDefaultValue().ifPresent(value -> analyzeDefaultColumnValue(session, plannerContext, accessControl, parameterLookup, warningCollector, supportedType, value));
            ColumnMetadata column = ColumnMetadata.builder()
                    .setName(columnName.getValue())
                    .setType(supportedType)
                    .setDefaultValue(element.getDefaultValue().map(Expression::toString))
                    .setNullable(element.isNullable())
                    .setComment(element.getComment())
                    .setProperties(columnProperties)
                    .build();

            plannerContext.getMetadata().addColumn(
                    session,
                    tableHandle,
                    qualifiedTableName.asCatalogSchemaTableName(),
                    column,
                    toConnectorColumnPosition(position));
        }
        else {
            accessControl.checkCanAlterColumn(session.toSecurityContext(), qualifiedTableName);

            if (!columns.containsKey(columnName.getValue().toLowerCase(ENGLISH))) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", columnName);
            }
            if (!(position instanceof ColumnPosition.Last)) {
                // TODO https://github.com/trinodb/trino/issues/24513 Support FIRST and AFTER options
                throw semanticException(NOT_SUPPORTED, statement, "Specifying column position is not supported for nested columns");
            }

            List<String> parentPath = statement.getColumn().getName().getOriginalParts().subList(0, statement.getColumn().getName().getOriginalParts().size() - 1).stream()
                    .map(identifier -> identifier.getValue().toLowerCase(ENGLISH))
                    .collect(toImmutableList());
            List<String> fieldPath = statement.getColumn().getName().getOriginalParts().subList(1, statement.getColumn().getName().getOriginalParts().size()).stream()
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            ColumnMetadata columnMetadata = columns.get(columnName.getValue().toLowerCase(ENGLISH));
            Type currentType = columnMetadata.getType();
            for (int i = 0; i < fieldPath.size() - 1; i++) {
                String fieldName = fieldPath.get(i);
                List<RowType.Field> candidates = getCandidates(currentType, fieldName);

                if (candidates.isEmpty()) {
                    throw semanticException(COLUMN_NOT_FOUND, statement, "Field '%s' does not exist within %s", fieldName, currentType);
                }
                if (candidates.size() > 1) {
                    throw semanticException(AMBIGUOUS_NAME, statement, "Field path %s within %s is ambiguous", fieldPath, columnMetadata.getType());
                }
                currentType = getOnlyElement(candidates).getType();
            }
            if (!(currentType instanceof RowType)) {
                // check if field path denotes a record after unwrapping possible containers
                throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + currentType);
            }

            String fieldName = getLast(statement.getColumn().getName().getParts());
            List<RowType.Field> candidates = getCandidates(currentType, fieldName);

            if (!candidates.isEmpty()) {
                if (statement.isColumnNotExists()) {
                    return immediateVoidFuture();
                }
                throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Field '%s' already exists", fieldName);
            }
            plannerContext.getMetadata().addField(
                    session,
                    tableHandle,
                    parentPath,
                    fieldName,
                    getSupportedType(session, catalogHandle, tableMetadata.metadata().getProperties(), type),
                    statement.isColumnNotExists());
        }

        return immediateVoidFuture();
    }

    private static List<RowType.Field> getCandidates(Type type, String fieldName)
    {
        if (type instanceof ArrayType arrayType) {
            if (!fieldName.equals("element")) {
                throw new TrinoException(NOT_SUPPORTED, "ARRAY type should be denoted by 'element' in the path; found '%s'".formatted(fieldName));
            }
            // return nameless Field to denote unwrapping of container
            return ImmutableList.of(RowType.field(arrayType.getElementType()));
        }
        if (!(type instanceof RowType rowType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
        List<RowType.Field> candidates = rowType.getFields().stream()
                // case-insensitive match
                .filter(rowField -> rowField.getName().isPresent() && rowField.getName().get().equalsIgnoreCase(fieldName))
                .collect(toImmutableList());

        return candidates;
    }

    private Type getSupportedType(Session session, CatalogHandle catalogHandle, Map<String, Object> tableProperties, Type type)
    {
        return plannerContext.getMetadata()
                .getSupportedType(session, catalogHandle, tableProperties, type)
                .orElse(type);
    }

    private static io.trino.spi.connector.ColumnPosition toConnectorColumnPosition(ColumnPosition columnPosition)
    {
        return switch (columnPosition) {
            case ColumnPosition.First _ -> new io.trino.spi.connector.ColumnPosition.First();
            case ColumnPosition.After after -> new io.trino.spi.connector.ColumnPosition.After(after.column().getValue().toLowerCase(ENGLISH));
            case ColumnPosition.Last _ -> new io.trino.spi.connector.ColumnPosition.Last();
        };
    }
}
