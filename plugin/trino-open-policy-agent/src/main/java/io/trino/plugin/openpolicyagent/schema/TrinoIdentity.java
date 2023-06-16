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
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TrinoIdentity(
        String user,
        Set<String> groups,
        Set<String> enabledRoles,
        Map<String, OpaSelectedRole> catalogRoles,
        Map<String, String> extraCredentials)
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
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)),
                identity.getExtraCredentials());
    }

    public record OpaSelectedRole(String type, String role)
    {
        public static OpaSelectedRole fromTrinoSelectedRole(SelectedRole role)
        {
            return new OpaSelectedRole(role.getType().name(), role.getRole().orElse(null));
        }
    }
}
