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
package io.trino.server.security;

import io.trino.client.ProtocolDetectionException;

import javax.ws.rs.core.MultivaluedMap;

import java.util.Optional;

import static io.trino.client.ProtocolHeaders.detectProtocol;

public final class AuthenticationUtils
{
    private AuthenticationUtils() {}

    /**
     * When username from authentication header matches the `x-trino-user` header, we assume that the client does
     * not want to force the runtime username, and only wanted to communicate the authentication user.
     */
    public static void checkAndRewriteUserHeaderToMappedUserWhenRequired(
            String username,
            MultivaluedMap<String, String> headers,
            String userName)
    {
        checkAndRewriteUserHeaderToMappedUserWhenRequired(username, headers, userName, Optional.empty());
    }

    /**
     * @deprecated due to alternative user header name deprecation.
     * Use {@link AuthenticationUtils#checkAndRewriteUserHeaderToMappedUserWhenRequired(String, MultivaluedMap, String)}
     */
    @Deprecated
    public static void checkAndRewriteUserHeaderToMappedUserWhenRequired(
            String username,
            MultivaluedMap<String, String> headers,
            String authenticatedUser,
            Optional<String> alternativeUserHeaderName)
    {
        String userHeader;
        try {
            userHeader = detectProtocol(alternativeUserHeaderName, headers.keySet()).requestUser();
        }
        catch (ProtocolDetectionException ignored) {
            // this shouldn't fail here, but ignore and it will be handled elsewhere
            return;
        }
        if (username.equals(headers.getFirst(userHeader))) {
            headers.putSingle(userHeader, authenticatedUser);
        }
    }
}
