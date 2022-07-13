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
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.MISSING_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CommentTask
        implements DataDefinitionTask<Comment>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public CommentTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "COMMENT";
    }

    @Override
    public ListenableFuture<Void> execute(
            Comment statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        if (statement.getType() == Comment.Type.TABLE) {
            commentOnTable(statement, session);
        }
        else if (statement.getType() == Comment.Type.VIEW) {
            commentOnView(statement, session);
        }
        else if (statement.getType() == Comment.Type.COLUMN) {
            commentOnColumn(statement, session);
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported comment type: %s", statement.getType());
        }

        return immediateVoidFuture();
    }

    private void commentOnTable(Comment statement, Session session)
    {
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getName());
        if (metadata.isMaterializedView(session, originalTableName)) {
            throw semanticException(
                    TABLE_NOT_FOUND,
                    statement,
                    "Table '%s' does not exist, but a materialized view with that name exists. Setting comments on materialized views is unsupported.", originalTableName);
        }

        if (metadata.isView(session, originalTableName)) {
            throw semanticException(
                    TABLE_NOT_FOUND,
                    statement,
                    "Table '%1$s' does not exist, but a view with that name exists. Did you mean COMMENT ON VIEW %1$s IS ...?", originalTableName);
        }

        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.getTableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", originalTableName);
        }

        accessControl.checkCanSetTableComment(session.toSecurityContext(), redirectionAwareTableHandle.getRedirectedTableName().orElse(originalTableName));
        TableHandle tableHandle = redirectionAwareTableHandle.getTableHandle().get();
        metadata.setTableComment(session, tableHandle, statement.getComment());
    }

    private void commentOnView(Comment statement, Session session)
    {
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getName());
        if (metadata.getView(session, viewName).isEmpty()) {
            String exceptionMessage = format("View '%s' does not exist", viewName);
            if (metadata.getMaterializedView(session, viewName).isPresent()) {
                exceptionMessage += ", but a materialized view with that name exists. Setting comments on materialized views is unsupported.";
            }
            else if (metadata.getTableHandle(session, viewName).isPresent()) {
                exceptionMessage += ", but a table with that name exists. Did you mean COMMENT ON TABLE " + viewName + " IS ...?";
            }
            throw semanticException(TABLE_NOT_FOUND, statement, exceptionMessage);
        }

        accessControl.checkCanSetViewComment(session.toSecurityContext(), viewName);
        metadata.setViewComment(session, viewName, statement.getComment());
    }

    private void commentOnColumn(Comment statement, Session session)
    {
        Optional<QualifiedName> prefix = statement.getName().getPrefix();
        if (prefix.isEmpty()) {
            throw semanticException(MISSING_TABLE, statement, "Table must be specified");
        }

        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, prefix.get());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.getTableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: " + originalTableName);
        }
        TableHandle tableHandle = redirectionAwareTableHandle.getTableHandle().get();

        String columnName = statement.getName().getSuffix();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        if (!columnHandles.containsKey(columnName)) {
            throw semanticException(COLUMN_NOT_FOUND, statement, "Column does not exist: " + columnName);
        }

        accessControl.checkCanSetColumnComment(session.toSecurityContext(), redirectionAwareTableHandle.getRedirectedTableName().orElse(originalTableName));

        metadata.setColumnComment(session, tableHandle, columnHandles.get(columnName), statement.getComment());
    }
}
