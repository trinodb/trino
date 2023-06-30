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
import io.trino.metadata.MetadataUtil;
import io.trino.security.AccessControl;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.Identifier;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.processRoleCommandCatalog;
import static io.trino.spi.security.PrincipalType.ROLE;
import static java.util.Objects.requireNonNull;

public class GrantRolesTask
        implements DataDefinitionTask<GrantRoles>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public GrantRolesTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "GRANT ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            GrantRoles statement,
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
        Optional<String> catalog = processRoleCommandCatalog(metadata, session, statement, statement.getCatalog().map(Identifier::getValue));

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
            checkRoleExists(session, statement, metadata, role, catalog);
        }

        accessControl.checkCanGrantRoles(session.toSecurityContext(), roles, grantees, adminOption, grantor, catalog);
        metadata.grantRoles(session, roles, grantees, adminOption, grantor, catalog);

        return immediateVoidFuture();
    }
}
