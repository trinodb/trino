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
package io.trino.spi.security;

import java.security.Principal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class ConnectorIdentity
{
    private final String user;
    private final Set<String> groups;
    private final Optional<Principal> principal;
    private final Set<String> enabledSystemRoles;
    private final Optional<SelectedRole> connectorRole;
    private final Map<String, String> extraCredentials;

    @Deprecated
    public ConnectorIdentity(String user, Optional<Principal> principal, Optional<SelectedRole> connectorRole)
    {
        this(user, principal, connectorRole, emptyMap());
    }

    @Deprecated
    public ConnectorIdentity(String user, Optional<Principal> principal, Optional<SelectedRole> connectorRole, Map<String, String> extraCredentials)
    {
        this(user, emptySet(), principal, emptySet(), connectorRole, extraCredentials);
    }

    private ConnectorIdentity(
            String user,
            Set<String> groups,
            Optional<Principal> principal,
            Set<String> enabledSystemRoles,
            Optional<SelectedRole> connectorRole,
            Map<String, String> extraCredentials)
    {
        this.user = requireNonNull(user, "user is null");
        this.groups = Set.copyOf(requireNonNull(groups, "groups is null"));
        this.principal = requireNonNull(principal, "principal is null");
        this.enabledSystemRoles = Set.copyOf(requireNonNull(enabledSystemRoles, "enabledSystemRoles is null"));
        this.connectorRole = requireNonNull(connectorRole, "connectorRole is null");
        this.extraCredentials = Map.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
    }

    public String getUser()
    {
        return user;
    }

    public Set<String> getGroups()
    {
        return groups;
    }

    public Optional<Principal> getPrincipal()
    {
        return principal;
    }

    public Set<String> getEnabledSystemRoles()
    {
        return enabledSystemRoles;
    }

    /**
     * @deprecated Use getConnectorRole
     */
    @Deprecated
    public Optional<SelectedRole> getRole()
    {
        return getConnectorRole();
    }

    public Optional<SelectedRole> getConnectorRole()
    {
        return connectorRole;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorIdentity{");
        sb.append("user='").append(user).append('\'');
        sb.append(", groups=").append(groups);
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        sb.append(", enabledSystemroles=").append(enabledSystemRoles);
        connectorRole.ifPresent(role -> sb.append(", connectorRole=").append(role));
        sb.append(", extraCredentials=").append(extraCredentials.keySet());
        sb.append('}');
        return sb.toString();
    }

    public static ConnectorIdentity ofUser(String user)
    {
        return new Builder(user).build();
    }

    public static Builder forUser(String user)
    {
        return new Builder(user);
    }

    public static class Builder
    {
        private final String user;
        private Set<String> groups = emptySet();
        private Optional<Principal> principal = Optional.empty();
        private Set<String> enabledSystemRoles = new HashSet<>();
        private Optional<SelectedRole> connectorRole = Optional.empty();
        private Map<String, String> extraCredentials = new HashMap<>();

        private Builder(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public Builder withGroups(Set<String> groups)
        {
            this.groups = Set.copyOf(requireNonNull(groups, "groups is null"));
            return this;
        }

        public Builder withPrincipal(Principal principal)
        {
            return withPrincipal(Optional.of(requireNonNull(principal, "principal is null")));
        }

        public Builder withPrincipal(Optional<Principal> principal)
        {
            this.principal = requireNonNull(principal, "principal is null");
            return this;
        }

        public Builder withEnabledSystemRoles(Set<String> enabledSystemRoles)
        {
            this.enabledSystemRoles = new HashSet<>(requireNonNull(enabledSystemRoles, "enabledSystemRoles is null"));
            return this;
        }

        /**
         * @deprecated Use withConnectorRole
         */
        @Deprecated
        public Builder withRole(SelectedRole role)
        {
            return withConnectorRole(role);
        }

        /**
         * @deprecated Use withConnectorRole
         */
        @Deprecated
        public Builder withRole(Optional<SelectedRole> role)
        {
            return withConnectorRole(role);
        }

        public Builder withConnectorRole(SelectedRole connectorRole)
        {
            return withConnectorRole(Optional.of(requireNonNull(connectorRole, "connectorRole is null")));
        }

        public Builder withConnectorRole(Optional<SelectedRole> connectorRole)
        {
            this.connectorRole = requireNonNull(connectorRole, "connectorRole is null");
            return this;
        }

        public Builder withExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials = new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null"));
            return this;
        }

        public ConnectorIdentity build()
        {
            return new ConnectorIdentity(user, groups, principal, enabledSystemRoles, connectorRole, extraCredentials);
        }
    }
}
