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
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class AuthenticatedUser
{
    private final Optional<String> username;
    private final Principal principal;

    public static AuthenticatedUser forPrincipal(Principal principal)
    {
        return new AuthenticatedUser(Optional.of(principal.getName()), principal);
    }

    public AuthenticatedUser(Optional<String> username, Principal principal)
    {
        this.username = requireNonNull(username, "username is null");
        this.principal = requireNonNull(principal, "principal is null");
    }

    public Optional<String> getUsername()
    {
        return username;
    }

    public Principal getPrincipal()
    {
        return principal;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", AuthenticatedUser.class.getSimpleName() + "[", "]")
                .add("username=" + username.orElse(""))
                .add("principal=" + principal)
                .toString();
    }
}
