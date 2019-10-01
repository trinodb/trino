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
package io.prestosql.spi.security;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Identity
{
    private final String user;
    private final Optional<Principal> principal;
    private final Map<String, SelectedRole> roles;
    private final Map<String, String> extraCredentials;

    @Deprecated
    public Identity(String user, Optional<Principal> principal)
    {
        this(user, principal, emptyMap());
    }

    @Deprecated
    public Identity(String user, Optional<Principal> principal, Map<String, SelectedRole> roles)
    {
        this(user, principal, roles, emptyMap());
    }

    @Deprecated
    public Identity(String user, Optional<Principal> principal, Map<String, SelectedRole> roles, Map<String, String> extraCredentials)
    {
        this.user = requireNonNull(user, "user is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.roles = unmodifiableMap(new HashMap<>(requireNonNull(roles, "roles is null")));
        this.extraCredentials = unmodifiableMap(new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null")));
    }

    public String getUser()
    {
        return user;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Map<String, SelectedRole> getRoles()
    {
        return roles;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public ConnectorIdentity toConnectorIdentity()
    {
        return new ConnectorIdentity(user, principal, Optional.empty(), extraCredentials);
    }

    public ConnectorIdentity toConnectorIdentity(String catalog)
    {
        requireNonNull(catalog, "catalog is null");
        return new ConnectorIdentity(user, principal, Optional.ofNullable(roles.get(catalog)), extraCredentials);
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
        Identity identity = (Identity) o;
        return Objects.equals(user, identity.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Identity{");
        sb.append("user='").append(user).append('\'');
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        sb.append(", roles=").append(roles);
        sb.append(", extraCredentials=").append(extraCredentials.keySet());
        sb.append('}');
        return sb.toString();
    }

    public static Identity ofUser(String user)
    {
        return new Builder(user).build();
    }

    public static Builder forUser(String user)
    {
        return new Builder(user);
    }

    public static Builder from(Identity identity)
    {
        return new Builder(identity.getUser())
                .withPrincipal(identity.getPrincipal())
                .withRoles(identity.getRoles())
                .withExtraCredentials(identity.getExtraCredentials());
    }

    public static class Builder
    {
        private final String user;
        private Optional<Principal> principal = Optional.empty();
        private Map<String, SelectedRole> roles = new HashMap<>();
        private Map<String, String> extraCredentials = new HashMap<>();

        public Builder(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public Builder withPrincipal(Principal principal)
        {
            return withPrincipal(Optional.of(principal));
        }

        public Builder withPrincipal(Optional<Principal> principal)
        {
            this.principal = requireNonNull(principal, "principal is null");
            return this;
        }

        public Builder withRole(String catalog, SelectedRole role)
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(role, "role is null");
            if (this.roles.put(catalog, role) != null) {
                throw new IllegalStateException("There is already role set for " + catalog);
            }
            return this;
        }

        public Builder withRoles(Map<String, SelectedRole> roles)
        {
            this.roles = new HashMap<>(requireNonNull(roles, "roles is null"));
            return this;
        }

        public Builder withExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials = new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null"));
            return this;
        }

        public Identity build()
        {
            return new Identity(user, principal, roles, extraCredentials);
        }
    }
}
