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

import io.trino.Session;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.connector.EntityPrivilege;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.security.FunctionAuthorization;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SchemaAuthorization;
import io.trino.spi.security.TableAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Optional;
import java.util.Set;

public interface SystemSecurityMetadata
{
    /**
     * Does the specified role exist.
     */
    boolean roleExists(Session session, String role);

    /**
     * Creates the specified role.
     *
     * @param grantor represents the principal specified by WITH ADMIN statement
     */
    void createRole(Session session, String role, Optional<TrinoPrincipal> grantor);

    /**
     * Drops the specified role.
     */
    void dropRole(Session session, String role);

    /**
     * List available roles.
     */
    Set<String> listRoles(Session session);

    /**
     * List roles grants for a given principal, not recursively.
     */
    Set<RoleGrant> listRoleGrants(Session session, TrinoPrincipal principal);

    /**
     * Grants the specified roles to the specified grantees.
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void grantRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor);

    /**
     * Revokes the specified roles from the specified grantees.
     *
     * @param grantor represents the principal specified by GRANTED BY statement
     */
    void revokeRoles(Session session, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor);

    /**
     * List applicable roles, including the transitive grants, for the specified principal
     */
    Set<RoleGrant> listApplicableRoles(Session session, TrinoPrincipal principal);

    /**
     * List applicable roles, including the transitive grants, in given identity
     */
    Set<String> listEnabledRoles(Identity identity);

    /**
     * Grants the specified privilege to the specified user on the specified schema.
     */
    void grantSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Denys the specified privilege to the specified user on the specified schema.
     */
    void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee);

    /**
     * Revokes the specified privilege on the specified schema from the specified user.
     */
    void revokeSchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Grants the specified privilege to the specified user on the specified table
     */
    void grantTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Denys the specified privilege to the specified user on the specified table
     */
    void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee);

    /**
     * Revokes the specified privilege on the specified table from the specified user
     */
    void revokeTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Gets the privileges for the specified table available to the given grantee considering the selected session role
     */
    Set<GrantInfo> listTablePrivileges(Session session, QualifiedTablePrefix prefix);

    /**
     * Grants the specified privilege to the specified user on the specified branch
     */
    void grantTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    /**
     * Denies the specified privilege to the specified user on the specified branch
     */
    void denyTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee);

    /**
     * Revokes the specified privilege on the specified branch from the specified user
     */
    void revokeTableBranchPrivileges(Session session, QualifiedObjectName tableName, String branchName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption);

    default Set<EntityPrivilege> getAllEntityKindPrivileges(String entityKind)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Grants the specified privilege to the specified user on the specified entity
     */
    default void grantEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Deny the specified privilege to the specified principal on the specified entity
     */
    default void denyEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Revokes the specified privilege on the specified entity from the specified user
     */
    default void revokeEntityPrivileges(Session session, EntityKindAndName entity, Set<EntityPrivilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Throws an exception if the entityKind is not supported, or if the privileges
     * are not supported for the entityKind
     */
    default void validateEntityKindAndPrivileges(Session session, String entityKind, Set<String> privileges)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Set the owner of the specified schema
     */
    Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema);

    /**
     * Get the identity to run the view as
     */
    Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName);

    /**
     * Get the identity to run the function as
     */
    Optional<Identity> getFunctionRunAsIdentity(Session session, CatalogSchemaFunctionName functionName);

    /**
     * A function is created
     */
    void functionCreated(Session session, CatalogSchemaFunctionName function);

    /**
     * A function is dropped
     */
    void functionDropped(Session session, CatalogSchemaFunctionName function);

    /**
     * A schema was created
     */
    void schemaCreated(Session session, CatalogSchemaName schema);

    /**
     * A schema was renamed
     */
    void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema);

    /**
     * A schema was dropped
     */
    void schemaDropped(Session session, CatalogSchemaName schema);

    /**
     * A table or view was created
     */
    void tableCreated(Session session, CatalogSchemaTableName table);

    /**
     * A table or view was renamed
     */
    void tableRenamed(Session session, CatalogSchemaTableName sourceTable, CatalogSchemaTableName targetTable);

    /**
     * A table or view was dropped
     */
    void tableDropped(Session session, CatalogSchemaTableName table);

    /**
     * A column was created
     */
    void columnCreated(Session session, CatalogSchemaTableName table, String column);

    /**
     * A column was renamed
     */
    void columnRenamed(Session session, CatalogSchemaTableName table, String oldName, String newName);

    /**
     * A column was dropped
     */
    void columnDropped(Session session, CatalogSchemaTableName table, String column);

    /**
     * Column type was changed
     */
    void columnTypeChanged(Session session, CatalogSchemaTableName table, String column, String oldType, String newType);

    /**
     * Column's NOT NULL constraint was dropped
     */
    void columnNotNullConstraintDropped(Session session, CatalogSchemaTableName table, String column);

    /**
     * Set the owner of the entity of entity kind ownedKind, with dotted name from the components of the name list,
     * to the supplied principal.  The ownedKind string is guaranteed to be uppercase, and the name is guaranteed
     * to be fully qualified, i.e., if the entity is a table, the name is of size three.
     */
    void setEntityOwner(Session session, EntityKindAndName entityKindAndName, TrinoPrincipal principal);

    Set<SchemaAuthorization> getSchemasAuthorizationInfo(Session session, QualifiedSchemaPrefix prefix);

    Set<TableAuthorization> getTablesAuthorizationInfo(Session session, QualifiedTablePrefix prefix);

    Set<FunctionAuthorization> getFunctionsAuthorizationInfo(Session session, QualifiedObjectPrefix prefix);
}
