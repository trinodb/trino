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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.ColumnPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

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
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getName());
        RedirectionAwareTableHandle redirectionAwareTableHandle = plannerContext.getMetadata().getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.redirectedTableName().orElse(originalTableName);

        Map<String, ColumnHandle> columnHandles = plannerContext.getMetadata().getColumnHandles(session, tableHandle);

        ColumnDefinition element = statement.getColumn();
        Identifier columnName = element.getName().getOriginalParts().get(0);
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
            if (columnHandles.containsKey(columnName.getValue().toLowerCase(ENGLISH))) {
                if (!statement.isColumnNotExists()) {
                    throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", columnName);
                }
                return immediateVoidFuture();
            }
            if (!element.isNullable() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
                throw semanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", catalogHandle, columnName);
            }

            Map<String, Object> columnProperties = columnPropertyManager.getProperties(
                    catalogHandle.getCatalogName(),
                    catalogHandle,
                    element.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    bindParameters(statement, parameters),
                    true);

            ColumnMetadata column = ColumnMetadata.builder()
                    .setName(columnName.getValue())
                    .setType(type)
                    .setNullable(element.isNullable())
                    .setComment(element.getComment())
                    .setProperties(columnProperties)
                    .build();

            plannerContext.getMetadata().addColumn(session, tableHandle, qualifiedTableName.asCatalogSchemaTableName(), column);
        }
        else {
            accessControl.checkCanAlterColumn(session.toSecurityContext(), qualifiedTableName);

            if (!columnHandles.containsKey(columnName.getValue().toLowerCase(ENGLISH))) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", columnName);
            }

            List<String> parentPath = statement.getColumn().getName().getOriginalParts().subList(0, statement.getColumn().getName().getOriginalParts().size() - 1).stream()
                    .map(identifier -> identifier.getValue().toLowerCase(ENGLISH))
                    .collect(toImmutableList());
            List<String> fieldPath = statement.getColumn().getName().getOriginalParts().subList(1, statement.getColumn().getName().getOriginalParts().size()).stream()
                    .map(Identifier::getValue)
                    .collect(toImmutableList());

            ColumnMetadata columnMetadata = plannerContext.getMetadata().getColumnMetadata(session, tableHandle, columnHandles.get(columnName.getValue().toLowerCase(ENGLISH)));
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

            String fieldName = getLast(statement.getColumn().getName().getParts());
            List<RowType.Field> candidates = getCandidates(currentType, fieldName);

            if (!candidates.isEmpty()) {
                if (statement.isColumnNotExists()) {
                    return immediateVoidFuture();
                }
                throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Field '%s' already exists", fieldName);
            }
            plannerContext.getMetadata().addField(session, tableHandle, parentPath, fieldName, type, statement.isColumnNotExists());
        }

        return immediateVoidFuture();
    }

    private static List<RowType.Field> getCandidates(Type type, String fieldName)
    {
        if (!(type instanceof RowType rowType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
        List<RowType.Field> candidates = rowType.getFields().stream()
                // case-insensitive match
                .filter(rowField -> rowField.getName().isPresent() && rowField.getName().get().equalsIgnoreCase(fieldName))
                .collect(toImmutableList());

        return candidates;
    }
}
