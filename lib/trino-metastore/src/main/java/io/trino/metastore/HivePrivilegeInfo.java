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
package io.trino.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.PrivilegeInfo;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static io.trino.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static java.util.Objects.requireNonNull;

@Immutable
public class HivePrivilegeInfo
{
    public enum HivePrivilege
    {
        SELECT, INSERT, UPDATE, DELETE, OWNERSHIP
    }

    private final HivePrivilege hivePrivilege;
    private final boolean grantOption;
    private final HivePrincipal grantor;
    private final HivePrincipal grantee;

    @JsonCreator
    public HivePrivilegeInfo(
            @JsonProperty("hivePrivilege") HivePrivilege hivePrivilege,
            @JsonProperty("grantOption") boolean grantOption,
            @JsonProperty("grantor") HivePrincipal grantor,
            @JsonProperty("grantee") HivePrincipal grantee)
    {
        this.hivePrivilege = requireNonNull(hivePrivilege, "hivePrivilege is null");
        this.grantOption = grantOption;
        this.grantor = requireNonNull(grantor, "grantor is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
    }

    @JsonProperty
    public HivePrivilege getHivePrivilege()
    {
        return hivePrivilege;
    }

    @JsonProperty
    public boolean isGrantOption()
    {
        return grantOption;
    }

    @JsonProperty
    public HivePrincipal getGrantor()
    {
        return grantor;
    }

    @JsonProperty
    public HivePrincipal getGrantee()
    {
        return grantee;
    }

    public static HivePrivilege toHivePrivilege(Privilege privilege)
    {
        return switch (privilege) {
            case SELECT -> SELECT;
            case INSERT -> INSERT;
            case DELETE -> DELETE;
            case UPDATE -> UPDATE;
            // Hive does not support CREATE privilege
            default -> throw new IllegalArgumentException("Unexpected privilege: " + privilege);
        };
    }

    public boolean isContainedIn(HivePrivilegeInfo hivePrivilegeInfo)
    {
        return (getHivePrivilege() == hivePrivilegeInfo.getHivePrivilege() &&
                (isGrantOption() == hivePrivilegeInfo.isGrantOption() ||
                        (!isGrantOption() && hivePrivilegeInfo.isGrantOption())));
    }

    public Set<PrivilegeInfo> toPrivilegeInfo()
    {
        return switch (hivePrivilege) {
            case SELECT -> ImmutableSet.of(new PrivilegeInfo(Privilege.SELECT, isGrantOption()));
            case INSERT -> ImmutableSet.of(new PrivilegeInfo(Privilege.INSERT, isGrantOption()));
            case DELETE -> ImmutableSet.of(new PrivilegeInfo(Privilege.DELETE, isGrantOption()));
            case UPDATE -> ImmutableSet.of(new PrivilegeInfo(Privilege.UPDATE, isGrantOption()));
            case OWNERSHIP -> ImmutableSet.of();
        };
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hivePrivilege, grantOption, grantor, grantee);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HivePrivilegeInfo hivePrivilegeInfo = (HivePrivilegeInfo) o;
        return hivePrivilege == hivePrivilegeInfo.hivePrivilege &&
                grantOption == hivePrivilegeInfo.grantOption &&
                Objects.equals(grantor, hivePrivilegeInfo.grantor) &&
                Objects.equals(grantee, hivePrivilegeInfo.grantee);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privilege", hivePrivilege)
                .add("grantOption", grantOption)
                .add("grantor", grantor)
                .add("grantee", grantee)
                .toString();
    }
}
