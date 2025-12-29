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
import io.trino.connector.CatalogHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetNotNullConstraint;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SetNotNullConstraintTask
        implements DataDefinitionTask<SetNotNullConstraint>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;

    @Inject
    public SetNotNullConstraintTask(PlannerContext plannerContext, AccessControl accessControl)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET NOT NULL";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetNotNullConstraint statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Metadata metadata = plannerContext.getMetadata();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, tableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            String exceptionMessage = "Table '%s' does not exist".formatted(tableName);
            if (metadata.getMaterializedView(session, tableName).isPresent()) {
                exceptionMessage += ", but a materialized view with that name exists.";
            }
            else if (metadata.isView(session, tableName)) {
                exceptionMessage += ", but a view with that name exists.";
            }
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "%s", exceptionMessage);
            }
            return immediateVoidFuture();
        }
        accessControl.checkCanAlterColumn(session.toSecurityContext(), tableName);

        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        String column = statement.getColumn().toString();
        ColumnHandle columnHandle = metadata.getColumnHandles(session, tableHandle).get(column);

        if (columnHandle == null) {
            throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", column);
        }

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
        if (columnMetadata.isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot modify hidden column");
        }
        if (!columnMetadata.isNullable()) {
            throw semanticException(NOT_SUPPORTED, statement, "Column is already not nullable");
        }
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        if (!plannerContext.getMetadata().getConnectorCapabilities(session, catalogHandle).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
            throw semanticException(NOT_SUPPORTED, statement, "Catalog '%s' does not support NOT NULL for column '%s'", catalogHandle, column);
        }
        metadata.setNotNullConstraint(session, tableHandle, columnHandle);
        return immediateVoidFuture();
    }
}
