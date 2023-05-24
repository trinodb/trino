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
import io.trino.security.AccessControl;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropTableTask
        implements DataDefinitionTask<DropTable>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropTableTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP TABLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropTable statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getTableName());
        if (metadata.isMaterializedView(session, originalTableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a materialized view with that name exists. Did you mean DROP MATERIALIZED VIEW %s?", originalTableName, originalTableName);
        }

        if (metadata.isView(session, originalTableName)) {
            throw semanticException(
                    GENERIC_USER_ERROR,
                    statement,
                    "Table '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", originalTableName, originalTableName);
        }

        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.getTableHandle().isEmpty()) {
            if (!statement.isExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
            }
            return immediateVoidFuture();
        }
        QualifiedObjectName tableName = redirectionAwareTableHandle.getRedirectedTableName().orElse(originalTableName);
        accessControl.checkCanDropTable(session.toSecurityContext(), tableName);

        metadata.dropTable(session, redirectionAwareTableHandle.getTableHandle().get(), tableName.asCatalogSchemaTableName());

        return immediateVoidFuture();
    }
}
