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
package io.trino.metadata;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;

public class DisabledSystemSecurityMetadata
        implements SystemSecurityMetadata
{
    @Override
    public boolean roleExists(Session session, String role)
    {
        return false;
    }

    @Override
    public void createRole(Session session, String role, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "System roles are not enabled");
    }

    @Override
    public void dropRole(Session session, String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "System roles are not enabled");
    }

    @Override
    public Set<String> listRoles(Session session)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(Session session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "System roles are not enabled");
    }

    @Override
    public void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "System roles are not enabled");
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<String> listEnabledRoles(Identity identity)
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantSchemaPrivileges(
            Session session,
            CatalogSchemaName schemaName,
            Set<Privilege> privileges,
            TrinoPrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, format("Catalog '%s' does not support permission management", schemaName.getCatalogName()));
    }

    @Override
    public void revokeSchemaPrivileges(
            Session session,
            CatalogSchemaName schemaName,
            Set<Privilege> privileges,
            TrinoPrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, format("Catalog '%s' does not support permission management", schemaName.getCatalogName()));
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, format("Catalog '%s' does not support permission management", tableName.getCatalogName()));
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, format("Catalog '%s' does not support permission management", tableName.getCatalogName()));
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return ImmutableSet.of();
    }
}
