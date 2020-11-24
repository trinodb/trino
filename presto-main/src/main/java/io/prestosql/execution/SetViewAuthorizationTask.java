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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SetViewAuthorization;
import io.prestosql.transaction.TransactionManager;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createPrincipal;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class SetViewAuthorizationTask
        implements DataDefinitionTask<SetViewAuthorization>
{
    @Override
    public String getName()
    {
        return "SET VIEW AUTHORIZATION";
    }

    @Override
    public ListenableFuture<?> execute(SetViewAuthorization statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource());
        CatalogName catalogName = metadata.getCatalogHandle(session, viewName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + viewName.getCatalogName()));
        metadata.getView(session, viewName)
                .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName));

        PrestoPrincipal principal = createPrincipal(statement.getPrincipal());
        if (principal.getType() == PrincipalType.ROLE
                && !metadata.listRoles(session, catalogName.getCatalogName()).contains(principal.getName())) {
            throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", principal.getName());
        }

        accessControl.checkCanSetViewAuthorization(session.toSecurityContext(), viewName, principal);

        metadata.setViewAuthorization(session, viewName.asCatalogSchemaTableName(), principal);

        return immediateFuture(null);
    }
}
