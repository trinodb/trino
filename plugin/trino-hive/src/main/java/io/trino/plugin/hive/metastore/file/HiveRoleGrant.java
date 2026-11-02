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
import io.trino.metastore.HivePrincipal;
import io.trino.spi.security.RoleGrant;

import static java.util.Objects.requireNonNull;

class HiveRoleGrant
{
    private final HivePrincipal grantee;
    private final String roleName;
    private final boolean grantable;

    @JsonCreator
    HiveRoleGrant(
            @JsonProperty("grantee") HivePrincipal grantee,
            @JsonProperty("roleName") String roleName,
            @JsonProperty("grantable") boolean grantable)
    {
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.roleName = requireNonNull(roleName, "roleName is null");
        this.grantable = grantable;
    }

    @JsonProperty
    public HivePrincipal getGrantee()
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
        return new RoleGrant(grantee.toTrinoPrincipal(), roleName, grantable);
    }

    static HiveRoleGrant fromRoleGrant(RoleGrant grant)
    {
        return new HiveRoleGrant(
                HivePrincipal.from(grant.getGrantee()),
                grant.getRoleName(),
                grant.isGrantable());
    }
}
