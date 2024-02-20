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
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetTableAuthorization;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SetTableAuthorizationTask
        implements DataDefinitionTask<SetTableAuthorization>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SetTableAuthorizationTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET TABLE AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetTableAuthorization statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getSource());

        getRequiredCatalogHandle(metadata, session, statement, tableName.getCatalogName());
        RedirectionAwareTableHandle redirection = metadata.getRedirectionAwareTableHandle(session, tableName);
        if (redirection.tableHandle().isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
        }
        if (redirection.redirectedTableName().isPresent()) {
            throw semanticException(NOT_SUPPORTED, statement, "Table %s is redirected to %s and SET TABLE AUTHORIZATION is not supported with table redirections", tableName, redirection.redirectedTableName().get());
        }

        TrinoPrincipal principal = createPrincipal(statement.getPrincipal());
        checkRoleExists(session, statement, metadata, principal, Optional.of(tableName.getCatalogName()).filter(catalog -> metadata.isCatalogManagedSecurity(session, catalog)));

        accessControl.checkCanSetTableAuthorization(session.toSecurityContext(), tableName, principal);

        metadata.setTableAuthorization(session, tableName.asCatalogSchemaTableName(), principal);

        return immediateVoidFuture();
    }
}
