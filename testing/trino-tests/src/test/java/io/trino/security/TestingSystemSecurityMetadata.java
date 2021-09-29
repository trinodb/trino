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
package io.trino.security;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Collections.synchronizedSet;

class TestingSystemSecurityMetadata
        implements SystemSecurityMetadata
{
    private final Set<String> roles = synchronizedSet(new HashSet<>());
    private final Set<RoleGrant> roleGrants = synchronizedSet(new HashSet<>());

    public void reset()
    {
        roles.clear();
        roleGrants.clear();
    }

    @Override
    public boolean roleExists(Session session, String role)
    {
        return roles.contains(role);
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        checkArgument(grantor.isEmpty(), "Grantor is not yet supported");
        roles.add(role);
    }

    @Override
    public void dropRole(Session session, String role)
    {
        roles.remove(role);
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        return ImmutableSet.copyOf(roles);
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(Session session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        return ImmutableSet.copyOf(roleGrants);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        return getRoleGrants(principal);
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        roleGrants.addAll(createRoleGrants(roles, grantees, adminOption, grantor));
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        roleGrants.removeAll(createRoleGrants(roles, grantees, adminOption, grantor));
    }

    private static Set<RoleGrant> createRoleGrants(Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        checkArgument(grantor.isEmpty(), "Grantor is not yet supported");
        Set<RoleGrant> roleGrantToAdd = new HashSet<>();
        for (String role : roles) {
            for (TrinoPrincipal grantee : grantees) {
                roleGrantToAdd.add(new RoleGrant(grantee, role, adminOption));
            }
        }
        return roleGrantToAdd;
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        return getRoleGrantsRecursively(principal);
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        Set<String> allUserRoles = getRoleGrantsRecursively(new TrinoPrincipal(USER, identity.getUser())).stream()
                .map(RoleGrant::getRoleName)
                .collect(toImmutableSet());

        if (identity.getEnabledRoles().isEmpty()) {
            return allUserRoles;
        }

        Set<String> enabledRoles = identity.getEnabledRoles().stream()
                .filter(allUserRoles::contains)
                .collect(toImmutableSet());

        Set<String> transitiveRoles = enabledRoles.stream()
                .flatMap(role -> getRoleGrantsRecursively(new TrinoPrincipal(ROLE, role)).stream())
                .map(RoleGrant::getRoleName)
                .collect(toImmutableSet());

        return ImmutableSet.<String>builder()
                .addAll(enabledRoles)
                .addAll(transitiveRoles)
                .build();
    }

    private Set<RoleGrant> getRoleGrantsRecursively(TrinoPrincipal principal)
    {
        Queue<RoleGrant> pending = new ArrayDeque<>(getRoleGrants(principal));
        Set<RoleGrant> seen = new HashSet<>();
        while (!pending.isEmpty()) {
            RoleGrant current = pending.remove();
            if (!seen.add(current)) {
                continue;
            }
            pending.addAll(getRoleGrants(new TrinoPrincipal(ROLE, current.getRoleName())));
        }
        return ImmutableSet.copyOf(seen);
    }

    private Set<RoleGrant> getRoleGrants(TrinoPrincipal principal)
    {
        return roleGrants.stream()
                .filter(roleGrant -> roleGrant.getGrantee().equals(principal))
                .collect(toImmutableSet());
    }

    @Override
    public void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        throw new UnsupportedOperationException();
    }
}
