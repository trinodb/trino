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
import io.trino.metadata.MetadataUtil;
import io.trino.security.AccessControl;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GrantRoles;
import io.trino.transaction.TransactionManager;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.getSessionCatalog;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

public class GrantRolesTask
        implements DataDefinitionTask<GrantRoles>
{
    @Override
    public String getName()
    {
        return "GRANT ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            GrantRoles statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        Set<String> roles = statement.getRoles().stream().map(role -> role.getValue().toLowerCase(Locale.ENGLISH)).collect(toImmutableSet());
        Set<TrinoPrincipal> grantees = statement.getGrantees().stream()
                .map(MetadataUtil::createPrincipal)
                .collect(toImmutableSet());
        boolean adminOption = statement.isAdminOption();
        Optional<TrinoPrincipal> grantor = statement.getGrantor().map(specification -> createPrincipal(session, specification));
        String catalog = getSessionCatalog(metadata, session, statement);

        Set<String> availableRoles = metadata.listRoles(session, catalog);
        Set<String> specifiedRoles = new LinkedHashSet<>();
        specifiedRoles.addAll(roles);
        grantees.stream()
                .filter(principal -> principal.getType() == ROLE)
                .map(TrinoPrincipal::getName)
                .forEach(specifiedRoles::add);
        if (grantor.isPresent() && grantor.get().getType() == ROLE) {
            specifiedRoles.add(grantor.get().getName());
        }

        for (String role : specifiedRoles) {
            if (!availableRoles.contains(role)) {
                throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", role);
            }
        }

        accessControl.checkCanGrantRoles(session.toSecurityContext(), roles, grantees, adminOption, grantor, catalog);
        metadata.grantRoles(session, roles, grantees, adminOption, grantor, catalog);

        return immediateVoidFuture();
    }
}
