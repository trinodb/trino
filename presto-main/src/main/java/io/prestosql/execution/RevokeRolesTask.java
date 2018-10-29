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
import io.prestosql.metadata.MetadataUtil;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RevokeRoles;
import io.prestosql.transaction.TransactionManager;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createCatalogName;
import static io.prestosql.metadata.MetadataUtil.createPrincipal;
import static io.prestosql.spi.security.PrincipalType.ROLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_ROLE;

public class RevokeRolesTask
        implements DataDefinitionTask<RevokeRoles>
{
    @Override
    public String getName()
    {
        return "GRANT ROLE";
    }

    @Override
    public ListenableFuture<?> execute(RevokeRoles statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();

        Set<String> roles = statement.getRoles().stream().map(role -> role.getValue().toLowerCase(Locale.ENGLISH)).collect(toImmutableSet());
        Set<PrestoPrincipal> grantees = statement.getGrantees().stream()
                .map(MetadataUtil::createPrincipal)
                .collect(toImmutableSet());
        boolean adminOptionFor = statement.isAdminOptionFor();
        Optional<PrestoPrincipal> grantor = statement.getGrantor().map(specification -> createPrincipal(session, specification));
        String catalog = createCatalogName(session, statement);

        Set<String> availableRoles = metadata.listRoles(session, catalog);
        Set<String> specifiedRoles = new LinkedHashSet<>();
        specifiedRoles.addAll(roles);
        grantees.stream()
                .filter(principal -> principal.getType() == ROLE)
                .map(PrestoPrincipal::getName)
                .forEach(specifiedRoles::add);
        if (grantor.isPresent() && grantor.get().getType() == ROLE) {
            specifiedRoles.add(grantor.get().getName());
        }

        for (String role : specifiedRoles) {
            if (!availableRoles.contains(role)) {
                throw new SemanticException(MISSING_ROLE, statement, "Role '%s' does not exist", role);
            }
        }

        accessControl.checkCanRevokeRoles(session.getRequiredTransactionId(), session.getIdentity(), roles, grantees, adminOptionFor, grantor, catalog);
        metadata.revokeRoles(session, roles, grantees, adminOptionFor, grantor, catalog);

        return immediateFuture(null);
    }
}
