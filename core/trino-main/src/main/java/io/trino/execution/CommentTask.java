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
import io.trino.metadata.MaterializedViewDefinition;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.metadata.ViewColumn;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.MISSING_TABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
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
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", originalTableName);
        }

        accessControl.checkCanSetTableComment(session.toSecurityContext(), redirectionAwareTableHandle.redirectedTableName().orElse(originalTableName));
        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        metadata.setTableComment(session, tableHandle, statement.getComment());
    }

    private void commentOnView(Comment statement, Session session)
    {
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getName());
        if (metadata.getView(session, viewName).isEmpty()) {
            String additionalInformation;
            if (metadata.getMaterializedView(session, viewName).isPresent()) {
                additionalInformation = ", but a materialized view with that name exists. Setting comments on materialized views is unsupported.";
            }
            else if (metadata.getTableHandle(session, viewName).isPresent()) {
                additionalInformation = ", but a table with that name exists. Did you mean COMMENT ON TABLE " + viewName + " IS ...?";
            }
            else {
                additionalInformation = "";
            }
            throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist%s", viewName, additionalInformation);
        }

        accessControl.checkCanSetViewComment(session.toSecurityContext(), viewName);
        metadata.setViewComment(session, viewName, statement.getComment());
    }

    private void commentOnColumn(Comment statement, Session session)
    {
        QualifiedName prefix = statement.getName().getPrefix()
                .orElseThrow(() -> semanticException(MISSING_TABLE, statement, "Table must be specified"));

        QualifiedObjectName originalObjectName = createQualifiedObjectName(session, statement, prefix);
        if (metadata.isView(session, originalObjectName)) {
            ViewDefinition viewDefinition = metadata.getView(session, originalObjectName).get();
            ViewColumn viewColumn = findAndCheckViewColumn(statement, session, viewDefinition, originalObjectName);
            metadata.setViewColumnComment(session, originalObjectName, viewColumn.getName(), statement.getComment());
        }
        else if (metadata.isMaterializedView(session, originalObjectName)) {
            MaterializedViewDefinition materializedViewDefinition = metadata.getMaterializedView(session, originalObjectName).get();
            ViewColumn viewColumn = findAndCheckViewColumn(statement, session, materializedViewDefinition, originalObjectName);
            metadata.setMaterializedViewColumnComment(session, originalObjectName, viewColumn.getName(), statement.getComment());
        }
        else {
            RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, originalObjectName);
            if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", originalObjectName);
            }
            TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();

            String columnName = statement.getName().getSuffix();
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            if (!columnHandles.containsKey(columnName)) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column does not exist: %s", columnName);
            }

            accessControl.checkCanSetColumnComment(session.toSecurityContext(), redirectionAwareTableHandle.redirectedTableName().orElse(originalObjectName));

            metadata.setColumnComment(session, tableHandle, columnHandles.get(columnName), statement.getComment());
        }
    }

    private ViewColumn findAndCheckViewColumn(Comment statement, Session session, ViewDefinition viewDefinition, QualifiedObjectName originalObjectName)
    {
        String columnName = statement.getName().getSuffix();
        ViewColumn viewColumn = viewDefinition.getColumns().stream()
                .filter(column -> column.getName().equals(columnName))
                .findAny()
                .orElseThrow(() -> semanticException(COLUMN_NOT_FOUND, statement, "Column does not exist: %s", columnName));
        accessControl.checkCanSetColumnComment(session.toSecurityContext(), originalObjectName);
        return viewColumn;
    }
}
