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
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

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
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void revokeSchemaPrivileges(
            Session session,
            CatalogSchemaName schemaName,
            Set<Privilege> privileges,
            TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(schemaName.getCatalogName());
    }

    @Override
    public void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw notSupportedException(tableName.getCatalogName());
    }

    @Override
    public Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix)
    {
        return ImmutableSet.of();
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        return Optional.empty();
    }

    @Override
    public void setSchemaOwner(Session session, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        throw notSupportedException(schema.getCatalogName());
    }

    @Override
    public void setTableOwner(Session session, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        throw notSupportedException(table.getCatalogName());
    }

    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName view)
    {
        return Optional.empty();
    }

    @Override
    public void setViewOwner(Session session, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        throw notSupportedException(view.getCatalogName());
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema) {}

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema) {}

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema) {}

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table) {}

    @Override
    public void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable) {}

    @Override
    public void tableDropped(Session session, CatalogSchemaTableName table) {}

    @Override
    public void columnCreated(Session session, CatalogSchemaTableName table, String column) {}

    @Override
    public void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName) {}

    @Override
    public void columnDropped(Session session, CatalogSchemaTableName table, String column) {}

    private static TrinoException notSupportedException(String catalogName)
    {
        return new TrinoException(NOT_SUPPORTED, "Catalog does not support permission management: " + catalogName);
    }
}
