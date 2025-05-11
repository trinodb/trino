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
import io.trino.metadata.QualifiedObjectPrefix;
import io.trino.metadata.QualifiedSchemaPrefix;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.EntityKindAndName;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.security.FunctionAuthorization;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SchemaAuthorization;
import io.trino.spi.security.TableAuthorization;
import io.trino.spi.security.TrinoPrincipal;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Collections.synchronizedMap;
import static java.util.Collections.synchronizedSet;

class TestingSystemSecurityMetadata
        implements SystemSecurityMetadata
{
    private final Set<String> roles = synchronizedSet(new HashSet<>());
    private final Set<RoleGrant> roleGrants = synchronizedSet(new HashSet<>());
    private final Map<CatalogSchemaTableName, Identity> viewOwners = synchronizedMap(new HashMap<>());
    private final Map<CatalogSchemaTableName, Identity> tableOwners = synchronizedMap(new HashMap<>());
    private final Map<CatalogSchemaFunctionName, Identity> functionOwners = synchronizedMap(new HashMap<>());
    private final Map<CatalogSchemaName, Identity> schemaOwners = synchronizedMap(new HashMap<>());

    public void reset()
    {
        roles.clear();
        roleGrants.clear();
        viewOwners.clear();
        tableOwners.clear();
        functionOwners.clear();
        schemaOwners.clear();
    }

    public String getFunctionOwner(CatalogSchemaFunctionName functionName)
    {
        return functionOwners.get(functionName).getUser();
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
    public void denySchemaPrivileges(Session session, CatalogSchemaName schemaName, Set<Privilege> privileges, TrinoPrincipal grantee)
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
    public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
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

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(Session session, CatalogSchemaName schema)
    {
        return Optional.empty();
    }

    @Override
    public Optional<Identity> getViewRunAsIdentity(Session session, CatalogSchemaTableName viewName)
    {
        return Optional.ofNullable(viewOwners.get(viewName))
                .map(identity -> Identity.from(identity)
                        .withEnabledRoles(getRoleGrantsRecursively(new TrinoPrincipal(USER, identity.getUser()))
                                .stream()
                                .map(RoleGrant::getRoleName)
                                .collect(toImmutableSet()))
                        .build());
    }

    @Override
    public Optional<Identity> getFunctionRunAsIdentity(Session session, CatalogSchemaFunctionName functionName)
    {
        return Optional.ofNullable(functionOwners.get(functionName))
                .map(identity -> Identity.from(identity)
                        .withEnabledRoles(getRoleGrantsRecursively(new TrinoPrincipal(USER, identity.getUser()))
                                .stream()
                                .map(RoleGrant::getRoleName)
                                .collect(toImmutableSet()))
                        .build());
    }

    @Override
    public void functionCreated(Session session, CatalogSchemaFunctionName function)
    {
        functionOwners.put(function, session.getIdentity());
    }

    @Override
    public void functionDropped(Session session, CatalogSchemaFunctionName function)
    {
        functionOwners.remove(function);
    }

    @Override
    public void schemaCreated(Session session, CatalogSchemaName schema)
    {
        schemaOwners.put(schema, session.getIdentity());
    }

    @Override
    public void schemaRenamed(Session session, CatalogSchemaName sourceSchema, CatalogSchemaName targetSchema) {}

    @Override
    public void schemaDropped(Session session, CatalogSchemaName schema) {}

    @Override
    public void tableCreated(Session session, CatalogSchemaTableName table)
    {
        tableOwners.put(table, session.getIdentity());
    }

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

    @Override
    public void columnTypeChanged(Session session, CatalogSchemaTableName table, String column, String oldType, String newType) {}

    @Override
    public void columnNotNullConstraintDropped(Session session, CatalogSchemaTableName table, String column) {}

    @Override
    public void setEntityOwner(Session session, EntityKindAndName entityKindAndName, TrinoPrincipal principal)
    {
        List<String> name = entityKindAndName.name();
        if (entityKindAndName.entityKind().contains("VIEW")) {
            checkArgument(principal.getType() == USER, "Only a user can be a view owner");
            viewOwners.put(new CatalogSchemaTableName(name.get(0), name.get(1), name.get(2)), Identity.ofUser(principal.getName()));
        }
        else if (entityKindAndName.entityKind().startsWith("TABLE")) {
            checkArgument(principal.getType() == USER, "Only a user can be a table owner");
            tableOwners.put(new CatalogSchemaTableName(name.get(0), name.get(1), name.get(2)), Identity.ofUser(principal.getName()));
        }
        else if (entityKindAndName.entityKind().startsWith("FUNCTION")) {
            checkArgument(principal.getType() == USER, "Only a user can be a function owner");
            functionOwners.put(new CatalogSchemaFunctionName(name.get(0), name.get(1), name.get(2)), Identity.ofUser(principal.getName()));
        }
        else if (entityKindAndName.entityKind().startsWith("SCHEMA")) {
            checkArgument(principal.getType() == USER, "Only a user can be a schema owner");
            schemaOwners.put(new CatalogSchemaName(name.get(0), name.get(1)), Identity.ofUser(principal.getName()));
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Set<SchemaAuthorization> getSchemasAuthorizationInfo(Session session, QualifiedSchemaPrefix prefix)
    {
        return schemaOwners.keySet().stream()
                .filter(catalogSchemaName -> prefix.matches(
                        new CatalogSchemaName(
                                catalogSchemaName.getCatalogName(),
                                catalogSchemaName.getSchemaName())))
                .map(schemaName -> {
                    Identity owner = schemaOwners.get(schemaName);
                    return new SchemaAuthorization(schemaName.getSchemaName(), new TrinoPrincipal(USER, owner.getUser()));
                })
                .collect(toImmutableSet());
    }

    @Override
    public Set<TableAuthorization> getTablesAuthorizationInfo(Session session, QualifiedTablePrefix prefix)
    {
        Map<CatalogSchemaTableName, Identity> combined = new HashMap<>();
        combined.putAll(tableOwners);
        combined.putAll(viewOwners);
        return combined.keySet().stream()
                .filter(catalogSchemaTableName -> prefix.matches(
                        new QualifiedObjectName(
                                catalogSchemaTableName.getCatalogName(),
                                catalogSchemaTableName.getSchemaTableName().getSchemaName(),
                                catalogSchemaTableName.getSchemaTableName().getTableName())))
                .map(viewName -> {
                    Identity owner = combined.get(viewName);
                    return new TableAuthorization(viewName.getSchemaTableName(), new TrinoPrincipal(USER, owner.getUser()));
                })
                .collect(toImmutableSet());
    }

    @Override
    public Set<FunctionAuthorization> getFunctionsAuthorizationInfo(Session session, QualifiedObjectPrefix prefix)
    {
        return functionOwners.keySet().stream()
                .filter(catalogSchemaFunctionName -> prefix.matches(
                        new QualifiedObjectName(
                                catalogSchemaFunctionName.getCatalogName(),
                                catalogSchemaFunctionName.getSchemaName(),
                                catalogSchemaFunctionName.getFunctionName())))
                .map(functionName -> {
                    Identity owner = functionOwners.get(functionName);
                    return new FunctionAuthorization(functionName.getSchemaFunctionName(), new TrinoPrincipal(USER, owner.getUser()));
                })
                .collect(toImmutableSet());
    }
}
