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
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class SetViewAuthorizationTask
        implements DataDefinitionTask<SetViewAuthorization>
{
    @Override
    public String getName()
    {
        return "SET VIEW AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetViewAuthorization statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource());
        CatalogName catalogName = metadata.getCatalogHandle(session, viewName.getCatalogName())
                .orElseThrow(() -> new TrinoException(NOT_FOUND, "Catalog does not exist: " + viewName.getCatalogName()));
        if (metadata.getView(session, viewName).isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName);
        }

        TrinoPrincipal principal = createPrincipal(statement.getPrincipal());
        if (principal.getType() == PrincipalType.ROLE
                && !metadata.listRoles(session, catalogName.getCatalogName()).contains(principal.getName())) {
            throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", principal.getName());
        }

        accessControl.checkCanSetViewAuthorization(session.toSecurityContext(), viewName, principal);

        metadata.setViewAuthorization(session, viewName.asCatalogSchemaTableName(), principal);

        return immediateVoidFuture();
    }
}
