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
import io.trino.client.ProtocolHeaders;
import io.trino.server.ProtocolConfig;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.client.ProtocolHeaders.detectProtocol;
import static io.trino.server.security.BasicAuthCredentials.extractBasicAuthCredentials;
import static io.trino.server.security.UserMapping.createUserMapping;

public class InsecureAuthenticator
        implements Authenticator
{
    private final UserMapping userMapping;
    private final Optional<String> alternateHeaderName;

    @Inject
    public InsecureAuthenticator(InsecureAuthenticatorConfig config, ProtocolConfig protocolConfig)
    {
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        Optional<BasicAuthCredentials> basicAuthCredentials = extractBasicAuthCredentials(request);

        String user;
        if (basicAuthCredentials.isPresent()) {
            if (basicAuthCredentials.get().getPassword().isPresent()) {
                throw new AuthenticationException("Password not allowed for insecure authentication", BasicAuthCredentials.AUTHENTICATE_HEADER);
            }
            user = basicAuthCredentials.get().getUser();
        }
        else {
            try {
                ProtocolHeaders protocolHeaders = detectProtocol(alternateHeaderName, request.getHeaders().keySet());
                user = emptyToNull(request.getHeaders().getFirst(protocolHeaders.requestUser()));
            }
            catch (ProtocolDetectionException e) {
                // ignored
                user = null;
            }
        }

        if (user == null) {
            throw new AuthenticationException("Basic authentication or " + TRINO_HEADERS.requestUser() + " must be sent", BasicAuthCredentials.AUTHENTICATE_HEADER);
        }

        try {
            String authenticatedUser = userMapping.mapUser(user);
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(new BasicPrincipal(user))
                    .build();
        }
        catch (UserMappingException e) {
            throw new AuthenticationException(e.getMessage());
        }
    }
}
