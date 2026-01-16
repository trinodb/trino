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
import java.util.List;
import java.util.Optional;

public interface HeaderAuthenticator
{
    interface Headers
    {
        // argument is treated as case insensitive
        List<String> getHeader(String name);
    }

    /**
     * Authenticate the provided headers.
     *
     * @return the authenticated entity
     * @throws AccessDeniedException if not allowed
     */
    @Deprecated
    Principal createAuthenticatedPrincipal(Headers headers);

    /**
     * If implemented and authenticated; provide an {@link Identity}.
     *
     * @param headers headers used to authenticate
     * @return the authenticated {@link Identity}
     */
    default Optional<Identity> createAuthenticatedIdentity(Headers headers)
    {
        return Optional.empty();
    }
}
