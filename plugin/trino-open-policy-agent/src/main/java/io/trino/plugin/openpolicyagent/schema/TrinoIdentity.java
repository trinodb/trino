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
package io.trino.plugin.openpolicyagent.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

@JsonInclude(NON_NULL)
public record TrinoIdentity(
        @NotNull String user,
        @NotNull Set<String> groups,
        @NotNull Set<String> enabledRoles,
        @NotNull Map<String, OpaSelectedRole> catalogRoles,
        @NotNull Map<String, String> extraCredentials)
{
    public static TrinoIdentity fromTrinoIdentity(Identity identity)
    {
        return new TrinoIdentity(
                identity.getUser(),
                identity.getGroups(),
                identity.getEnabledRoles(),
                identity
                        .getCatalogRoles()
                        .entrySet()
                        .stream()
                        .map((entry) -> Map.entry(
                                entry.getKey(), OpaSelectedRole.fromTrinoSelectedRole(entry.getValue())))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)),
                identity.getExtraCredentials());
    }

    public record OpaSelectedRole(String type, String role)
    {
        public static OpaSelectedRole fromTrinoSelectedRole(SelectedRole role)
        {
            return new OpaSelectedRole(role.getType().name(), role.getRole().orElse(null));
        }
    }

    public TrinoIdentity
    {
        requireNonNull(user, "user is null");
        groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
        enabledRoles = ImmutableSet.copyOf(requireNonNull(enabledRoles, "enabledRoles is null"));
        catalogRoles = ImmutableMap.copyOf(requireNonNull(catalogRoles, "catalogRoles is null"));
        extraCredentials = ImmutableMap.copyOf(requireNonNull(extraCredentials, "extraCredentials is null"));
    }
}
