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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveViewNotSupportedException;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.GrantInfo;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;
import io.trino.spi.security.RoleGrant;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toSet;

public class SqlStandardAccessControlMetadata
        implements AccessControlMetadata
{
    private static final Set<String> RESERVED_ROLES = ImmutableSet.of("all", "default", "none");

    private final SqlStandardAccessControlMetadataMetastore metastore;

    public SqlStandardAccessControlMetadata(SqlStandardAccessControlMetadataMetastore metastore)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<HivePrincipal> grantor)
    {
        checkRoleIsNotReserved(role);
        metastore.createRole(role, null);
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        checkRoleIsNotReserved(role);
        metastore.dropRole(role);
    }

    private static void checkRoleIsNotReserved(String role)
    {
        // roles are case insensitive in Hive
        if (RESERVED_ROLES.contains(role.toLowerCase(ENGLISH))) {
            throw new TrinoException(ALREADY_EXISTS, "Role name cannot be one of the reserved roles: " + RESERVED_ROLES);
        }
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return ImmutableSet.copyOf(metastore.listRoles());
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, HivePrincipal principal)
    {
        return ImmutableSet.copyOf(metastore.listRoleGrants(principal));
    }

    @Override
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, Optional<HivePrincipal> grantor)
    {
        metastore.grantRoles(roles, grantees, adminOption, grantor.orElse(new HivePrincipal(USER, session.getUser())));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, Optional<HivePrincipal> grantor)
    {
        metastore.revokeRoles(roles, grantees, adminOption, grantor.orElse(new HivePrincipal(USER, session.getUser())));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, HivePrincipal principal)
    {
        return ThriftMetastoreUtil.listApplicableRoles(principal, metastore::listRoleGrants)
                .collect(toImmutableSet());
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return ThriftMetastoreUtil.listEnabledRoles(session.getIdentity(), metastore::listRoleGrants)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<HivePrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return metastore.getDatabaseOwner(schemaName);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        // Hive does not support the CREATE privilege, so ignore. Normally we would throw
        // an error for this, but when the Trino engine sees ALL_PRIVILEGES, it sends the
        // enumerated list of privileges instead of an Optional.empty
        privileges = privileges.stream()
                .filter(not(Privilege.CREATE::equals))
                .collect(toImmutableSet());

        metastore.grantTablePrivileges(
                schemaName,
                tableName,
                grantee,
                new HivePrincipal(USER, session.getUser()),
                privileges.stream()
                        .map(HivePrivilegeInfo::toHivePrivilege)
                        .collect(toSet()),
                grantOption);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        // Hive does not support the CREATE privilege, so ignore. Normally we would throw
        // an error for this, but when the Trino engine sees ALL_PRIVILEGES, it sends the
        // enumerated list of privileges instead of an Optional.empty
        privileges = privileges.stream()
                .filter(not(Privilege.CREATE::equals))
                .collect(toImmutableSet());

        metastore.revokeTablePrivileges(
                schemaName,
                tableName,
                grantee,
                new HivePrincipal(USER, session.getUser()),
                privileges.stream()
                        .map(HivePrivilegeInfo::toHivePrivilege)
                        .collect(toSet()),
                grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, List<SchemaTableName> tableNames)
    {
        Set<HivePrincipal> principals = ThriftMetastoreUtil.listEnabledPrincipals(session.getIdentity(), metastore::listRoleGrants)
                .collect(toImmutableSet());
        boolean isAdminRoleSet = hasAdminRole(principals);
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (SchemaTableName tableName : tableNames) {
            try {
                result.addAll(buildGrants(principals, isAdminRoleSet, tableName));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
            catch (HiveViewNotSupportedException e) {
                // table is an unsupported hive view but shouldn't fail listTablePrivileges.
            }
        }
        return result.build();
    }

    private List<GrantInfo> buildGrants(Set<HivePrincipal> principals, boolean isAdminRoleSet, SchemaTableName tableName)
    {
        if (isAdminRoleSet) {
            return buildGrants(tableName, Optional.empty());
        }
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (HivePrincipal grantee : principals) {
            result.addAll(buildGrants(tableName, Optional.of(grantee)));
        }
        return result.build();
    }

    private List<GrantInfo> buildGrants(SchemaTableName tableName, Optional<HivePrincipal> principal)
    {
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        Set<HivePrivilegeInfo> hivePrivileges = metastore.listTablePrivileges(tableName.getSchemaName(), tableName.getTableName(), principal);
        for (HivePrivilegeInfo hivePrivilege : hivePrivileges) {
            Set<PrivilegeInfo> prestoPrivileges = hivePrivilege.toPrivilegeInfo();
            for (PrivilegeInfo prestoPrivilege : prestoPrivileges) {
                GrantInfo grant = new GrantInfo(
                        prestoPrivilege,
                        hivePrivilege.getGrantee().toTrinoPrincipal(),
                        tableName,
                        Optional.of(hivePrivilege.getGrantor().toTrinoPrincipal()),
                        Optional.empty());
                result.add(grant);
            }
        }
        return result.build();
    }

    private static boolean hasAdminRole(Set<HivePrincipal> roles)
    {
        return roles.stream().anyMatch(principal -> principal.getName().equalsIgnoreCase(ADMIN_ROLE_NAME));
    }
}
