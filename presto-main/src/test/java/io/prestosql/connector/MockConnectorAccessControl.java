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
package io.prestosql.connector;

import io.prestosql.plugin.base.security.AllowAllAccessControl;
import io.prestosql.spi.connector.ConnectorSecurityContext;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;

import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.prestosql.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static java.util.Objects.requireNonNull;

class MockConnectorAccessControl
        extends AllowAllAccessControl
{
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final Grants<String> schemaGrants;

    MockConnectorAccessControl(Grants<String> schemaGrants)
    {
        this.schemaGrants = requireNonNull(schemaGrants, "schemaGrants is null");
    }

    @Override
    public Set<String> filterSchemas(ConnectorSecurityContext context, Set<String> schemaNames)
    {
        return schemaNames.stream()
                .filter(schema -> canAccessSchema(context.getIdentity(), schema))
                .collect(toImmutableSet());
    }

    @Override
    public void checkCanGrantSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, PrestoPrincipal grantee, boolean grantOption)
    {
        if (!schemaGrants.canGrant(context.getIdentity().getUser(), schemaName, privilege)) {
            denyGrantSchemaPrivilege(privilege.toString(), schemaName);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(ConnectorSecurityContext context, Privilege privilege, String schemaName, PrestoPrincipal revokee, boolean grantOption)
    {
        if (!schemaGrants.canGrant(context.getIdentity().getUser(), schemaName, privilege)) {
            denyRevokeSchemaPrivilege(privilege.toString(), schemaName);
        }
    }

    public void grantSchemaPrivileges(String schemaName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        schemaGrants.grant(grantee, schemaName, privileges, grantOption);
    }

    public void revokeSchemaPrivileges(String schemaName, Set<Privilege> privileges, PrestoPrincipal revokee, boolean grantOption)
    {
        schemaGrants.revoke(revokee, schemaName, privileges, grantOption);
    }

    private boolean canAccessSchema(ConnectorIdentity identity, String schema)
    {
        return schema.equalsIgnoreCase(INFORMATION_SCHEMA)
                || Arrays.stream(Privilege.values()).anyMatch(privilege -> schemaGrants.isAllowed(identity.getUser(), schema, privilege));
    }
}
