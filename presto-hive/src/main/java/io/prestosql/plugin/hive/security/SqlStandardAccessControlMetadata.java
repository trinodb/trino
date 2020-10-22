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
package io.prestosql.plugin.hive.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HiveViewNotSupportedException;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.PrivilegeInfo;
import io.prestosql.spi.security.RoleGrant;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.toHivePrivilege;
import static io.prestosql.plugin.hive.metastore.thrift.ThriftMetastoreUtil.listEnabledPrincipals;
import static io.prestosql.plugin.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.security.PrincipalType.ROLE;
import static io.prestosql.spi.security.PrincipalType.USER;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class SqlStandardAccessControlMetadata
        implements AccessControlMetadata
{
    private static final Set<String> RESERVED_ROLES = ImmutableSet.of("all", "default", "none");
    private static final String PUBLIC_ROLE_NAME = "public";

    private final SemiTransactionalHiveMetastore metastore;

    public SqlStandardAccessControlMetadata(SemiTransactionalHiveMetastore metastore)
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
            throw new PrestoException(ALREADY_EXISTS, "Role name cannot be one of the reserved roles: " + RESERVED_ROLES);
        }
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return ImmutableSet.copyOf(metastore.listRoles());
    }

    @Override
    public Set<RoleGrant> listAllRoleGrants(ConnectorSession session, Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
    {
        Set<String> actualRoles = roles.orElseGet(() -> metastore.listRoles());

        // choose more efficient path
        if (grantees.isPresent() && actualRoles.size() > grantees.get().size() * 2) {
            // 2x because we check two grantee types (ROLE or USER)
            return getRoleGrantsByGrantees(grantees.get(), limit);
        }
        return getRoleGrantsByRoles(actualRoles, limit);
    }

    private Set<RoleGrant> getRoleGrantsByGrantees(Set<String> grantees, OptionalLong limit)
    {
        ImmutableSet.Builder<RoleGrant> roleGrants = ImmutableSet.builder();
        int count = 0;
        for (String grantee : grantees) {
            for (PrincipalType type : new PrincipalType[] {USER, ROLE}) {
                if (limit.isPresent() && count >= limit.getAsLong()) {
                    return roleGrants.build();
                }
                for (RoleGrant grant : metastore.listRoleGrants(new HivePrincipal(type, grantee))) {
                    // Filter out the "public" role since it is not explicitly granted in Hive.
                    if (PUBLIC_ROLE_NAME.equals(grant.getRoleName())) {
                        continue;
                    }
                    count++;
                    roleGrants.add(grant);
                }
            }
        }
        return roleGrants.build();
    }

    private Set<RoleGrant> getRoleGrantsByRoles(Set<String> roles, OptionalLong limit)
    {
        ImmutableSet.Builder<RoleGrant> roleGrants = ImmutableSet.builder();
        int count = 0;
        for (String role : roles) {
            if (limit.isPresent() && count >= limit.getAsLong()) {
                break;
            }
            for (RoleGrant grant : metastore.listGrantedPrincipals(role)) {
                count++;
                roleGrants.add(grant);
            }
        }
        return roleGrants.build();
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
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<HivePrivilegeInfo> hivePrivilegeInfos = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(toHivePrivilege(privilege), grantOption, new HivePrincipal(USER, session.getUser()), new HivePrincipal(USER, session.getUser())))
                .collect(toSet());

        metastore.grantTablePrivileges(new HiveIdentity(session), schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, HivePrincipal grantee, boolean grantOption)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Set<HivePrivilegeInfo> hivePrivilegeInfos = privileges.stream()
                .map(privilege -> new HivePrivilegeInfo(toHivePrivilege(privilege), grantOption, new HivePrincipal(USER, session.getUser()), new HivePrincipal(USER, session.getUser())))
                .collect(toSet());

        metastore.revokeTablePrivileges(new HiveIdentity(session), schemaName, tableName, grantee, hivePrivilegeInfos);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, List<SchemaTableName> tableNames)
    {
        Set<HivePrincipal> principals = listEnabledPrincipals(metastore, session.getIdentity())
                .collect(toImmutableSet());
        boolean isAdminRoleSet = hasAdminRole(principals);
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (SchemaTableName tableName : tableNames) {
            try {
                result.addAll(buildGrants(session, principals, isAdminRoleSet, tableName));
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

    private List<GrantInfo> buildGrants(ConnectorSession session, Set<HivePrincipal> principals, boolean isAdminRoleSet, SchemaTableName tableName)
    {
        if (isAdminRoleSet) {
            return buildGrants(session, tableName, Optional.empty());
        }
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        for (HivePrincipal grantee : principals) {
            result.addAll(buildGrants(session, tableName, Optional.of(grantee)));
        }
        return result.build();
    }

    private List<GrantInfo> buildGrants(ConnectorSession session, SchemaTableName tableName, Optional<HivePrincipal> principal)
    {
        ImmutableList.Builder<GrantInfo> result = ImmutableList.builder();
        Set<HivePrivilegeInfo> hivePrivileges = metastore.listTablePrivileges(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName(), principal);
        for (HivePrivilegeInfo hivePrivilege : hivePrivileges) {
            Set<PrivilegeInfo> prestoPrivileges = hivePrivilege.toPrivilegeInfo();
            for (PrivilegeInfo prestoPrivilege : prestoPrivileges) {
                GrantInfo grant = new GrantInfo(
                        prestoPrivilege,
                        hivePrivilege.getGrantee().toPrestoPrincipal(),
                        tableName,
                        Optional.of(hivePrivilege.getGrantor().toPrestoPrincipal()),
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
