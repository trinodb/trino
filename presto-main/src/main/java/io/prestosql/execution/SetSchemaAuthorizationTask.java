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
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SetSchemaAuthorization;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogSchemaName;
import static io.prestosql.metadata.MetadataUtil.createPrincipal;
import static io.prestosql.metadata.MetadataUtil.getSessionCatalog;
import static io.prestosql.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class SetSchemaAuthorizationTask
        implements DataDefinitionTask<SetSchemaAuthorization>
{
    @Override
    public String getName()
    {
        return "SET SCHEMA AUTHORIZATION";
    }

    @Override
    public ListenableFuture<?> execute(SetSchemaAuthorization statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        String catalog = getSessionCatalog(metadata, session, statement);

        CatalogSchemaName source = createCatalogSchemaName(session, statement, Optional.of(statement.getSource()));

        if (!metadata.schemaExists(session, source)) {
            throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", source);
        }
        PrestoPrincipal principal = createPrincipal(statement.getPrincipal());
        if (principal.getType() == PrincipalType.ROLE
                && !metadata.listRoles(session, catalog).contains(principal.getName())) {
            throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", principal.getName());
        }

        accessControl.checkCanSetSchemaAuthorization(session.toSecurityContext(), source, principal);

        metadata.setSchemaAuthorization(session, source, principal);

        return immediateFuture(null);
    }
}
