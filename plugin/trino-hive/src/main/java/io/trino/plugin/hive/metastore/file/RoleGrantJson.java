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
package io.trino.plugin.hive.metastore.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import static java.util.Objects.requireNonNull;

class RoleGrantJson
{
    private record GranteeJson(@JsonProperty("type") PrincipalType type, @JsonProperty("name") String name) {}

    private final TrinoPrincipal grantee;
    private final String roleName;
    private final boolean grantable;

    @JsonCreator
    RoleGrantJson(
            @JsonProperty("grantee") GranteeJson grantee,
            @JsonProperty("roleName") String roleName,
            @JsonProperty("grantable") boolean grantable)
    {
        requireNonNull(grantee, "grantee is null");
        this.grantee = new TrinoPrincipal(grantee.type(), grantee.name());
        this.roleName = requireNonNull(roleName, "roleName is null");
        this.grantable = grantable;
    }

    @JsonProperty
    public TrinoPrincipal getGrantee()
    {
        return grantee;
    }

    @JsonProperty
    public String getRoleName()
    {
        return roleName;
    }

    @JsonProperty
    public boolean isGrantable()
    {
        return grantable;
    }

    RoleGrant toRoleGrant()
    {
        return new RoleGrant(grantee, roleName, grantable);
    }

    static RoleGrantJson fromRoleGrant(RoleGrant grant)
    {
        TrinoPrincipal grantee = grant.getGrantee();
        return new RoleGrantJson(
                new GranteeJson(grantee.getType(), grantee.getName()),
                grant.getRoleName(),
                grant.isGrantable());
    }
}
