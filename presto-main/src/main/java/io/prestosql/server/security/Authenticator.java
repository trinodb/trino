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
package io.prestosql.server.security;

import javax.servlet.http.HttpServletRequest;

import java.security.Principal;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface Authenticator
{
    AuthenticatedPrincipal authenticate(HttpServletRequest request)
            throws AuthenticationException;

    final class AuthenticatedPrincipal
    {
        private final String user;
        private final Principal principal;

        public AuthenticatedPrincipal(String user, Principal principal)
        {
            this.user = requireNonNull(user, "user is null");
            this.principal = requireNonNull(principal, "principal is null");
        }

        public String getUser()
        {
            return user;
        }

        public Principal getPrincipal()
        {
            return principal;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("user", user)
                    .add("principal", principal)
                    .toString();
        }
    }
}
