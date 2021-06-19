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
import io.trino.security.AccessControl;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.Expression;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.getSessionCatalog;
import static io.trino.spi.StandardErrorCode.ROLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;

public class CreateRoleTask
        implements DataDefinitionTask<CreateRole>
{
    @Override
    public String getName()
    {
        return "CREATE ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateRole statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        String catalog = getSessionCatalog(metadata, session, statement);
        String role = statement.getName().getValue().toLowerCase(ENGLISH);
        Optional<TrinoPrincipal> grantor = statement.getGrantor().map(specification -> createPrincipal(session, specification));
        accessControl.checkCanCreateRole(session.toSecurityContext(), role, grantor, catalog);
        Set<String> existingRoles = metadata.listRoles(session, catalog);
        if (existingRoles.contains(role)) {
            throw semanticException(ROLE_ALREADY_EXISTS, statement, "Role '%s' already exists", role);
        }
        if (grantor.isPresent() && grantor.get().getType() == ROLE && !existingRoles.contains(grantor.get().getName())) {
            throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", grantor.get().getName());
        }
        metadata.createRole(session, role, grantor, catalog);
        return immediateVoidFuture();
    }
}
