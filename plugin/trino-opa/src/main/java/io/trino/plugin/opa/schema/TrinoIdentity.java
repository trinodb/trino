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
package io.trino.plugin.opa.schema;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.Identity;

import java.security.Principal;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record TrinoIdentity(
        String user,
        Set<String> groups,
        Optional<Principal> principal)
{
    public static TrinoIdentity fromTrinoIdentity(Identity identity, boolean includeUserPrincipal)
    {
        return new TrinoIdentity(
                identity.getUser(),
                identity.getGroups(),
                includeUserPrincipal ? identity.getPrincipal() : Optional.empty());
    }

    public TrinoIdentity
    {
        requireNonNull(user, "user is null");
        groups = ImmutableSet.copyOf(requireNonNull(groups, "groups is null"));
        requireNonNull(principal, "principal is null");
    }
}
