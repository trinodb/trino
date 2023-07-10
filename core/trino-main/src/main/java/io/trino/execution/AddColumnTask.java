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
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
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
        if (redirectionAwareTableHandle.getTableHandle().isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = redirectionAwareTableHandle.getTableHandle().get();
        CatalogHandle catalogHandle = tableHandle.getCatalogHandle();

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.getRedirectedTableName().orElse(originalTableName);
        accessControl.checkCanAddColumns(session.toSecurityContext(), qualifiedTableName);

        Map<String, ColumnHandle> columnHandles = plannerContext.getMetadata().getColumnHandles(session, tableHandle);

        ColumnDefinition element = statement.getColumn();
        Type type;
        try {
            type = plannerContext.getTypeManager().getType(toTypeSignature(element.getType()));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (type.equals(UNKNOWN)) {
            throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (columnHandles.containsKey(element.getName().getValue().toLowerCase(ENGLISH))) {
            if (!statement.isColumnNotExists()) {
                throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", element.getName());
            }
            return immediateVoidFuture();
        }
        if (!element.isNullable() && !plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
            throw semanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", catalogHandle, element.getName());
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
                .setName(element.getName().getValue())
                .setType(type)
                .setNullable(element.isNullable())
                .setComment(element.getComment())
                .setProperties(columnProperties)
                .build();

        plannerContext.getMetadata().addColumn(session, tableHandle, qualifiedTableName.asCatalogSchemaTableName(), column);

        return immediateVoidFuture();
    }
}
