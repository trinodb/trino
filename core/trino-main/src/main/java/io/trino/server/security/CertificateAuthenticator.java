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

import com.google.common.collect.ImmutableList;
import io.trino.spi.security.Identity;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.List;

import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class CertificateAuthenticator
        implements Authenticator
{
    private static final String X509_ATTRIBUTE = "javax.servlet.request.X509Certificate";

    private final CertificateAuthenticatorManager authenticatorManager;
    private final UserMapping userMapping;

    @Inject
    public CertificateAuthenticator(CertificateAuthenticatorManager authenticatorManager, CertificateConfig config)
    {
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        this.authenticatorManager = requireNonNull(authenticatorManager, "authenticatorManager is null");
        authenticatorManager.setRequired();
    }

    @Override
    public Identity authenticate(ContainerRequestContext request)
            throws AuthenticationException
    {
        Object attribute = request.getProperty(X509_ATTRIBUTE);
        if (attribute == null) {
            throw new AuthenticationException(null);
        }
        List<X509Certificate> certificates = ImmutableList.copyOf((X509Certificate[]) attribute);
        if (certificates.isEmpty()) {
            throw new AuthenticationException(null);
        }

        try {
            Principal principal = authenticatorManager.getAuthenticator().authenticate(certificates);
            String authenticatedUser = userMapping.mapUser(principal.toString());
            return Identity.forUser(authenticatedUser)
                    .withPrincipal(principal)
                    .build();
        }
        catch (UserMappingException e) {
            throw new AuthenticationException(e.getMessage());
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }
    }
}
