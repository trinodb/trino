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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class Identity
{
    private final String user;
    private final Set<String> groups;
    private final Optional<Principal> principal;
    private final Set<String> enabledRoles;
    private final Map<String, SelectedRole> catalogRoles;
    private final Map<String, String> extraCredentials;
    private final Optional<Runnable> onDestroy;

    private Identity(
            String user,
            Set<String> groups,
            Optional<Principal> principal,
            Set<String> enabledRoles,
            Map<String, SelectedRole> catalogRoles,
            Map<String, String> extraCredentials,
            Optional<Runnable> onDestroy)
    {
        this.user = requireNonNull(user, "user is null");
        this.groups = Set.copyOf(requireNonNull(groups, "groups is null"));
        this.principal = requireNonNull(principal, "principal is null");
        this.enabledRoles = Set.copyOf(requireNonNull(enabledRoles, "enabledRoles is null"));
        this.catalogRoles = Map.copyOf(requireNonNull(catalogRoles, "connectorRoles is null"));
        this.extraCredentials = Map.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
        this.onDestroy = requireNonNull(onDestroy, "onDestroy is null");
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

    public Set<String> getEnabledRoles()
    {
        return enabledRoles;
    }

    /**
     * @deprecated Use getConnectorRoles
     */
    @Deprecated
    public Map<String, SelectedRole> getRoles()
    {
        return getCatalogRoles();
    }

    public Map<String, SelectedRole> getCatalogRoles()
    {
        return catalogRoles;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    public ConnectorIdentity toConnectorIdentity()
    {
        return ConnectorIdentity.forUser(user)
                .withGroups(groups)
                .withPrincipal(principal)
                .withEnabledSystemRoles(enabledRoles)
                .withExtraCredentials(extraCredentials)
                .build();
    }

    public ConnectorIdentity toConnectorIdentity(String catalog)
    {
        return ConnectorIdentity.forUser(user)
                .withGroups(groups)
                .withPrincipal(principal)
                .withEnabledSystemRoles(enabledRoles)
                .withConnectorRole(Optional.ofNullable(catalogRoles.get(catalog)))
                .withExtraCredentials(extraCredentials)
                .build();
    }

    public void destroy()
    {
        onDestroy.ifPresent(Runnable::run);
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
        return Objects.equals(user, identity.user) &&
               Objects.equals(groups, identity.groups) &&
               Objects.equals(principal, identity.principal) &&
               Objects.equals(enabledRoles, identity.enabledRoles) &&
               Objects.equals(catalogRoles, identity.catalogRoles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user, groups, principal, enabledRoles, catalogRoles);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Identity{");
        sb.append("user='").append(user).append('\'');
        if (!groups.isEmpty()) {
            sb.append(", groups=").append(groups);
        }
        principal.ifPresent(principal -> sb.append(", principal=").append(principal));
        if (!catalogRoles.isEmpty()) {
            sb.append(", catalogRoles=").append(catalogRoles);
        }
        if (!enabledRoles.isEmpty()) {
            sb.append(", enabledRoles=").append(enabledRoles);
        }
        // Do not print any internal credential keys
        List<String> filteredCredentials = extraCredentials.keySet().stream()
                .filter(key -> !key.contains("$internal"))
                .collect(toCollection(ArrayList::new));
        if (filteredCredentials.size() != extraCredentials.size()) {
            filteredCredentials.add("...");
        }
        if (!extraCredentials.isEmpty()) {
            sb.append(", extraCredentials=").append(filteredCredentials);
        }
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
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledRoles(identity.enabledRoles)
                .withConnectorRoles(identity.getCatalogRoles())
                .withExtraCredentials(identity.getExtraCredentials());
    }

    public static class Builder
    {
        private String user;
        private Set<String> groups = new HashSet<>();
        private Optional<Principal> principal = Optional.empty();
        private Set<String> enabledRoles = new HashSet<>();
        private Map<String, SelectedRole> connectorRoles = new HashMap<>();
        private Map<String, String> extraCredentials = new HashMap<>();
        private Optional<Runnable> onDestroy = Optional.empty();

        public Builder(String user)
        {
            this.user = requireNonNull(user, "user is null");
        }

        public Builder withUser(String user)
        {
            this.user = requireNonNull(user, "user is null");
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

        public Builder withEnabledRoles(Set<String> enabledRoles)
        {
            this.enabledRoles = new HashSet<>(requireNonNull(enabledRoles, "enabledRoles is null"));
            return this;
        }

        /**
         * @deprecated Use withConnectorRole
         */
        @Deprecated
        public Builder withRole(String catalog, SelectedRole role)
        {
            return withConnectorRole(catalog, role);
        }

        /**
         * @deprecated Use withConnectorRoles
         */
        @Deprecated
        public Builder withRoles(Map<String, SelectedRole> roles)
        {
            return withConnectorRoles(roles);
        }

        /**
         * @deprecated Use withAdditionalConnectorRoles
         */
        @Deprecated
        public Builder withAdditionalRoles(Map<String, SelectedRole> roles)
        {
            return withAdditionalConnectorRoles(roles);
        }

        public Builder withConnectorRole(String catalog, SelectedRole role)
        {
            requireNonNull(catalog, "catalog is null");
            requireNonNull(role, "role is null");
            if (this.connectorRoles.put(catalog, role) != null) {
                throw new IllegalStateException("There is already role set for " + catalog);
            }
            return this;
        }

        public Builder withConnectorRoles(Map<String, SelectedRole> roles)
        {
            this.connectorRoles = new HashMap<>(requireNonNull(roles, "roles is null"));
            return this;
        }

        public Builder withAdditionalConnectorRoles(Map<String, SelectedRole> roles)
        {
            this.connectorRoles.putAll(requireNonNull(roles, "roles is null"));
            return this;
        }

        public Builder withExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials = new HashMap<>(requireNonNull(extraCredentials, "extraCredentials is null"));
            return this;
        }

        public Builder withAdditionalExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials.putAll(requireNonNull(extraCredentials, "extraCredentials is null"));
            return this;
        }

        public void withOnDestroy(Runnable onDestroy)
        {
            requireNonNull(onDestroy, "onDestroy is null");
            if (this.onDestroy.isPresent()) {
                throw new IllegalStateException("Destroy callback already set");
            }
            this.onDestroy = Optional.of(new InvokeOnceRunnable(onDestroy));
        }

        public Builder withGroups(Set<String> groups)
        {
            this.groups = new HashSet<>(requireNonNull(groups, "groups is null"));
            return this;
        }

        public Builder withAdditionalGroups(Set<String> groups)
        {
            this.groups.addAll(requireNonNull(groups, "groups is null"));
            return this;
        }

        public Identity build()
        {
            return new Identity(user, groups, principal, enabledRoles, connectorRoles, extraCredentials, onDestroy);
        }
    }

    private static final class InvokeOnceRunnable
            implements Runnable
    {
        private final Runnable delegate;
        private final AtomicBoolean invoked = new AtomicBoolean();

        public InvokeOnceRunnable(Runnable delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void run()
        {
            if (invoked.compareAndSet(false, true)) {
                delegate.run();
            }
        }
    }
}
