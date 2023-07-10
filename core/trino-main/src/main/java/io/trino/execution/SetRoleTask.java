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
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SetRole;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.processRoleCommandCatalog;
import static io.trino.spi.StandardErrorCode.ROLE_NOT_FOUND;
import static io.trino.spi.security.AccessDeniedException.denySetRole;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SetRoleTask
        implements DataDefinitionTask<SetRole>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SetRoleTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET ROLE";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetRole statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Optional<String> catalog = processRoleCommandCatalog(metadata, session, statement, statement.getCatalog().map(Identifier::getValue));
        if (statement.getType() == SetRole.Type.ROLE) {
            String role = statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH)).orElseThrow();
            if (!metadata.roleExists(session, role, catalog)) {
                throw semanticException(ROLE_NOT_FOUND, statement, "Role '%s' does not exist", role);
            }
            if (catalog.isPresent()) {
                accessControl.checkCanSetCatalogRole(SecurityContext.of(session), role, catalog.get());
            }
            else {
                Set<RoleGrant> roleGrants = metadata.listApplicableRoles(session, new TrinoPrincipal(USER, session.getUser()), Optional.empty());
                if (roleGrants.stream().map(RoleGrant::getRoleName).noneMatch(role::equals)) {
                    denySetRole(role);
                }
            }
        }
        SelectedRole.Type type = toSelectedRoleType(statement.getType());
        stateMachine.addSetRole(catalog.orElse("system"), new SelectedRole(type, statement.getRole().map(c -> c.getValue().toLowerCase(ENGLISH))));
        return immediateVoidFuture();
    }

    private static SelectedRole.Type toSelectedRoleType(SetRole.Type statementRoleType)
    {
        switch (statementRoleType) {
            case ROLE:
                return SelectedRole.Type.ROLE;
            case ALL:
                return SelectedRole.Type.ALL;
            case NONE:
                return SelectedRole.Type.NONE;
        }
        throw new IllegalArgumentException("Unsupported type: " + statementRoleType);
    }
}
