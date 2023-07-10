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
package io.trino.plugin.hive.security;

import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public interface AccessControlMetadata
{
    default boolean isUsingSystemSecurity()
    {
        return false;
    }

    /**
     * Does the specified role exist.
     */
    default boolean roleExists(ConnectorSession session, String role)
    {
        return listRoles(session).contains(role);
    }

    /**
     * Creates the specified role.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    default void createRole(ConnectorSession session, String role, Optional<HivePrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support create role");
    }

    /**
     * Drops the specified role.
     */
    default void dropRole(ConnectorSession session, String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support drop role");
    }

    /**
     * List available roles.
     */
    default Set<String> listRoles(ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List role grants for a given principal, not recursively.
     */
    default Set<RoleGrant> listRoleGrants(ConnectorSession session, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified roles to the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void grantRoles(ConnectorSession connectorSession, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, Optional<HivePrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Revokes the specified roles from the specified grantees
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    default void revokeRoles(ConnectorSession connectorSession, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, Optional<HivePrincipal> grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    default Set<RoleGrant> listApplicableRoles(ConnectorSession session, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * List applicable roles, including the transitive grants, in given session
     */
    default Set<String> listEnabledRoles(ConnectorSession session)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support roles");
    }

    /**
     * Grants the specified privilege to the specified user on the specified schema
     */
    default void grantSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support grants on schemas");
    }

    /**
     * Get the owner on the specified schema
     */
    default Optional<HivePrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return Optional.empty();
    }

    /**
     * Revokes the specified privilege on the specified schema from the specified user
     */
    default void revokeSchemaPrivileges(ConnectorSession session, String schemaName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support revokes on schemas");
    }

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    default void grantTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support grants on tables");
    }

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    default void revokeTablePrivileges(ConnectorSession session, SchemaTableName tableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support revokes on tables");
    }

    /**
     * List the table privileges granted to the specified grantee for the tables that have the specified prefix considering the selected session role
     */
    default List<GrantInfo> listTablePrivileges(ConnectorSession session, List<SchemaTableName> tableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support table privileges");
    }
}
