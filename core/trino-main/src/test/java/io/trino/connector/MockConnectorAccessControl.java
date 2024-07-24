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
package io.trino.connector;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.security.AllowAllAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static java.util.Objects.requireNonNull;

class MockConnectorAccessControl
        extends AllowAllAccessControl
{
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final Grants<String> schemaGrants;
    private final Grants<SchemaTableName> tableGrants;
    private final Function<SchemaTableName, ViewExpression> rowFilters;
    private final BiFunction<SchemaTableName, String, ViewExpression> columnMasks;

    MockConnectorAccessControl(
            Grants<String> schemaGrants,
            Grants<SchemaTableName> tableGrants,
            Function<SchemaTableName, ViewExpression> rowFilters,
            BiFunction<SchemaTableName, String, ViewExpression> columnMasks)
    {
        this.schemaGrants = requireNonNull(schemaGrants, "schemaGrants is null");
        this.tableGrants = requireNonNull(tableGrants, "tableGrants is null");
        this.rowFilters = requireNonNull(rowFilters, "rowFilters is null");
        this.columnMasks = requireNonNull(columnMasks, "columnMasks is null");
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames.stream()
                .filter(schema -> canAccessSchema(context.getIdentity(), schema))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee, boolean grantOption)
    {
        if (!schemaGrants.canGrant(context.getIdentity().getUser(), schemaName, privilege)) {
            denyGrantSchemaPrivilege(privilege.toString(), schemaName);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, TrinoPrincipal revokee, boolean grantOption)
    {
        if (!schemaGrants.canGrant(context.getIdentity().getUser(), schemaName, privilege)) {
            denyRevokeSchemaPrivilege(privilege.toString(), schemaName);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorSecurityContext context, Set<SchemaTableName> tableNames)
    {
        return tableNames.stream()
                .filter(tableName -> canAccessSchema(context.getIdentity(), tableName.getSchemaName()) || canAccessTable(context.getIdentity(), tableName))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee, boolean grantOption)
    {
        String user = context.getIdentity().getUser();
        if (!schemaGrants.canGrant(user, tableName.getSchemaName(), privilege) && !tableGrants.canGrant(user, tableName, privilege)) {
            denyGrantTablePrivilege(privilege.toString(), tableName.getTableName());
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal grantee)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorSecurityContext context, Privilege privilege, SchemaTableName tableName, TrinoPrincipal revokee, boolean grantOption)
    {
        String user = context.getIdentity().getUser();
        if (!schemaGrants.canGrant(user, tableName.getSchemaName(), privilege) && !tableGrants.canGrant(user, tableName, privilege)) {
            denyRevokeTablePrivilege(privilege.toString(), tableName.toString());
        }
    }

    @Override
    public List<ViewExpression> getRowFilters(ConnectorSecurityContext context, SchemaTableName tableName)
    {
        return Optional.ofNullable(rowFilters.apply(tableName))
                .map(ImmutableList::of)
                .orElseGet(ImmutableList::of);
    }

    @Override
    public Optional<ViewExpression> getColumnMask(ConnectorSecurityContext context, SchemaTableName tableName, String columnName, Type type)
    {
        return Optional.ofNullable(columnMasks.apply(tableName, columnName));
    }

    public void grantSchemaPrivileges(String schemaName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        schemaGrants.grant(grantee, schemaName, privileges, grantOption);
    }

    public void revokeSchemaPrivileges(String schemaName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
    {
        schemaGrants.revoke(revokee, schemaName, privileges, grantOption);
    }

    public void grantTablePrivileges(SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal grantee, boolean grantOption)
    {
        tableGrants.grant(grantee, tableName, privileges, grantOption);
    }

    public void revokeTablePrivileges(SchemaTableName tableName, Set<Privilege> privileges, TrinoPrincipal revokee, boolean grantOption)
    {
        tableGrants.revoke(revokee, tableName, privileges, grantOption);
    }

    private boolean canAccessSchema(ConnectorIdentity identity, String schemaName)
    {
        return schemaName.equalsIgnoreCase(INFORMATION_SCHEMA)
                || Arrays.stream(Privilege.values()).anyMatch(privilege -> schemaGrants.isAllowed(identity.getUser(), schemaName, privilege));
    }

    private boolean canAccessTable(ConnectorIdentity identity, SchemaTableName tableName)
    {
        return Arrays.stream(Privilege.values()).anyMatch(privilege -> tableGrants.isAllowed(identity.getUser(), tableName, privilege));
    }
}
