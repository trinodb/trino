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
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RenameView;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class RenameViewTask
        implements DataDefinitionTask<RenameView>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public RenameViewTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "RENAME VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameView statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource());
        if (metadata.isMaterializedView(session, viewName)) {
            throw semanticException(
                    TABLE_NOT_FOUND,
                    statement,
                    "View '%s' does not exist, but a materialized view with that name exists. Did you mean ALTER MATERIALIZED VIEW %s RENAME TO ...?", viewName, viewName);
        }

        if (!metadata.isView(session, viewName)) {
            if (metadata.getTableHandle(session, viewName).isPresent()) {
                throw semanticException(
                        TABLE_NOT_FOUND,
                        statement,
                        "View '%s' does not exist, but a table with that name exists. Did you mean ALTER TABLE %s RENAME TO ...?", viewName, viewName);
            }

            throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        if (metadata.getCatalogHandle(session, target.getCatalogName()).isEmpty()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.isMaterializedView(session, target)) {
            throw semanticException(GENERIC_USER_ERROR, statement, "Target view '%s' does not exist, but a materialized view with that name exists.", target);
        }
        if (metadata.isView(session, target)) {
            throw semanticException(GENERIC_USER_ERROR, statement, "Target view '%s' already exists", target);
        }
        if (metadata.getTableHandle(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target view '%s' does not exist, but a table with that name exists.", target);
        }
        if (!viewName.getCatalogName().equals(target.getCatalogName())) {
            throw semanticException(NOT_SUPPORTED, statement, "View rename across catalogs is not supported");
        }

        accessControl.checkCanRenameView(session.toSecurityContext(), viewName, target);

        metadata.renameView(session, viewName, target);

        return immediateVoidFuture();
    }
}
