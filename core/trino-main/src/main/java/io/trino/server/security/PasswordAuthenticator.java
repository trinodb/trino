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

import com.google.inject.Inject;
import io.trino.client.ProtocolDetectionException;
import io.trino.server.ProtocolConfig;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedMap;

import java.security.Principal;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static io.trino.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticator
        implements Authenticator
{
    private final PasswordAuthenticatorManager authenticatorManager;
    private final UserMapping userMapping;
    private final Optional<String> alternateHeaderName;

    @Inject
    public PasswordAuthenticator(PasswordAuthenticatorManager authenticatorManager, PasswordAuthenticatorConfig config, ProtocolConfig protocolConfig)
    {
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());

        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        BasicAuthCredentials basicAuthCredentials = extractBasicAuthCredentials(request)
                .orElseThrow(() -> needAuthentication(null));
        String user = basicAuthCredentials.getUser();
        String password = basicAuthCredentials.getPassword()
                .orElseThrow(() -> new AuthenticationException("Malformed credentials: password is empty"));

        AuthenticationException exception = null;
        for (io.trino.spi.security.PasswordAuthenticator authenticator : authenticatorManager.getAuthenticators()) {
            try {
                Principal principal = authenticator.createAuthenticatedPrincipal(user, password);
                String authenticatedUser = userMapping.mapUser(principal.toString());

                // rewrite the original "unmapped" user header to the mapped user (see method Javadoc for more details)
                rewriteUserHeaderToMappedUser(basicAuthCredentials, request.getHeaders(), authenticatedUser);
                return Identity.forUser(authenticatedUser)
                        .withPrincipal(principal)
                        .build();
            }
            catch (UserMappingException | AccessDeniedException e) {
                if (exception == null) {
                    exception = needAuthentication(e.getMessage());
                }
                else {
                    exception.addSuppressed(needAuthentication(e.getMessage()));
                }
            }
            catch (RuntimeException e) {
                throw new RuntimeException("Authentication error", e);
            }
        }

        verify(exception != null, "exception not set");
        throw exception;
    }

    /**
     * When the user in the basic authentication header matches the x-trino-user header, we assume that the client does
     * not want to force the runtime user name, and only wanted to communicate the authentication user.
     */
    private void rewriteUserHeaderToMappedUser(BasicAuthCredentials basicAuthCredentials, MultivaluedMap<String, String> headers, String authenticatedUser)
    {
        String userHeader;
        try {
            userHeader = getUserHeader(headers);
        }
        catch (ProtocolDetectionException ignored) {
            // this shouldn't fail here, but ignore and it will be handled elsewhere
            return;
        }
        if (basicAuthCredentials.getUser().equals(headers.getFirst(userHeader))) {
            headers.putSingle(userHeader, authenticatedUser);
        }
    }

    // Extract this out in a method so that the logic of preferring originalUser and fallback on user remains in one place
    private String getUserHeader(MultivaluedMap<String, String> headers)
            throws ProtocolDetectionException
    {
        String userHeader = detectProtocol(alternateHeaderName, headers.keySet()).requestOriginalUser();
        if (headers.getFirst(userHeader) == null || headers.getFirst(userHeader).isEmpty()) {
            userHeader = detectProtocol(alternateHeaderName, headers.keySet()).requestUser();
        }
        return userHeader;
    }

    private static AuthenticationException needAuthentication(String message)
    {
        return new AuthenticationException(message, BasicAuthCredentials.AUTHENTICATE_HEADER);
    }
}
