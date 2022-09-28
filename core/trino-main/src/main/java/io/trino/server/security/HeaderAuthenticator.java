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
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;

import javax.ws.rs.container.ContainerRequestContext;

import java.security.Principal;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class HeaderAuthenticator
        implements Authenticator
{
    private final HeaderAuthenticatorManager authenticatorManager;
    private final UserMapping userMapping;

    @Inject
    public HeaderAuthenticator(HeaderAuthenticatorConfig authenticatorConfig, HeaderAuthenticatorManager authenticatorManager)
    {
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");

        this.userMapping = createUserMapping(authenticatorConfig.getUserMappingPattern(), authenticatorConfig.getUserMappingFile());

        this.authenticatorManager.setRequired();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request) throws AuthenticationException
    {
        AuthenticationException exception = null;
        Map<String, List<String>> lowerCasedHeaders = request.getHeaders().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(Locale.ENGLISH), Map.Entry::getValue));

        for (io.trino.spi.security.HeaderAuthenticator authenticator : this.authenticatorManager.getAuthenticators()) {
            try {
                Principal principal = authenticator.createAuthenticatedPrincipal(name -> lowerCasedHeaders.get(name.toLowerCase(Locale.ENGLISH)));
                String authenticatedUser = this.userMapping.mapUser(principal.toString());

                return Identity.forUser(authenticatedUser)
                        .withPrincipal(principal)
                        .build();
            }
            catch (UserMappingException | AccessDeniedException e) {
                if (exception == null) {
                    exception = new AuthenticationException(e.getMessage());
                }
                else {
                    exception.addSuppressed(new AuthenticationException(e.getMessage()));
                }
            }
            catch (RuntimeException e) {
                throw new RuntimeException("Authentication error", e);
            }
        }

        verify(exception != null, "exception is not set");
        throw exception;
    }
}
