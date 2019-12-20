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
package io.prestosql.plugin.hive.metastore.file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo;
import io.prestosql.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;

import static io.prestosql.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;

public class PermissionMetadata
{
    private final HivePrivilege permission;
    private final boolean grantOption;
    private final HivePrincipal grantee;

    @JsonCreator
    public PermissionMetadata(
            @JsonProperty("permission") HivePrivilege permission,
            @JsonProperty("grantOption") boolean grantOption,
            @JsonProperty("grantee") HivePrincipal grantee)
    {
        this.permission = requireNonNull(permission, "permission is null");
        this.grantOption = grantOption;
        this.grantee = requireNonNull(grantee, "grantee is null");
    }

    @JsonProperty
    public HivePrivilege getPermission()
    {
        return permission;
    }

    @JsonProperty
    public boolean isGrantOption()
    {
        return grantOption;
    }

    @JsonProperty
    public HivePrincipal getGrantee()
    {
        return grantee;
    }

    public HivePrivilegeInfo toHivePrivilegeInfo()
    {
        return new HivePrivilegeInfo(permission, grantOption, new HivePrincipal(USER, "admin"), grantee);
    }
}
