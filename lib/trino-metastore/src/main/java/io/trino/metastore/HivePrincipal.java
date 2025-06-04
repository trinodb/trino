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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;

import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class HivePrincipal
{
    public static HivePrincipal from(ConnectorIdentity identity)
    {
        if (identity.getConnectorRole().isEmpty()) {
            return ofUser(identity.getUser());
        }
        SelectedRole.Type type = identity.getConnectorRole().get().getType();
        if (type == SelectedRole.Type.ALL) {
            return ofUser(identity.getUser());
        }
        checkArgument(type == SelectedRole.Type.ROLE, "Expected role type to be ALL or ROLE, but got: %s", type);
        return ofRole(identity.getConnectorRole().get().getRole().get());
    }

    private static HivePrincipal ofUser(String user)
    {
        return new HivePrincipal(PrincipalType.USER, user);
    }

    private static HivePrincipal ofRole(String role)
    {
        return new HivePrincipal(PrincipalType.ROLE, role);
    }

    public static Set<HivePrincipal> from(Set<TrinoPrincipal> trinoPrincipals)
    {
        return trinoPrincipals.stream()
                .map(HivePrincipal::from)
                .collect(toImmutableSet());
    }

    public static HivePrincipal from(TrinoPrincipal trinoPrincipal)
    {
        return new HivePrincipal(trinoPrincipal.getType(), trinoPrincipal.getName());
    }

    private final PrincipalType type;
    private final String name;

    @JsonCreator
    public HivePrincipal(@JsonProperty("type") PrincipalType type, @JsonProperty("name") String name)
    {
        this.type = requireNonNull(type, "type is null");
        this.name = canonicalName(type, name);
    }

    private static String canonicalName(PrincipalType type, String name)
    {
        requireNonNull(name, "name is null");
        return switch (type) {
            // In Hive user names are case sensitive
            case USER -> name;
            // In Hive role names are case insensitive
            case ROLE -> name.toLowerCase(ENGLISH);
        };
    }

    @JsonProperty
    public PrincipalType getType()
    {
        return type;
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
        HivePrincipal hivePrincipal = (HivePrincipal) o;
        return type == hivePrincipal.type &&
                Objects.equals(name, hivePrincipal.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return type + " " + name;
    }

    public TrinoPrincipal toTrinoPrincipal()
    {
        return new TrinoPrincipal(type, name);
    }
}
