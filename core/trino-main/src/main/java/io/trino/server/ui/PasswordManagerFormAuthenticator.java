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
package io.trino.server.ui;

import io.airlift.log.Logger;
import io.trino.server.security.PasswordAuthenticatorConfig;
import io.trino.server.security.PasswordAuthenticatorManager;
import io.trino.server.security.SecurityConfig;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.PasswordAuthenticator;

import javax.inject.Inject;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

import static io.trino.server.security.UserMapping.createUserMapping;
import static java.util.Objects.requireNonNull;

public class PasswordManagerFormAuthenticator
        implements FormAuthenticator
{
    private static final Logger log = Logger.get(PasswordManagerFormAuthenticator.class);
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final UserMapping userMapping;
    private final boolean insecureAuthenticationOverHttpAllowed;

    @Inject
    public PasswordManagerFormAuthenticator(PasswordAuthenticatorManager passwordAuthenticatorManager, PasswordAuthenticatorConfig config, SecurityConfig securityConfig)
    {
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        passwordAuthenticatorManager.setRequired();
        this.userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        this.insecureAuthenticationOverHttpAllowed = requireNonNull(securityConfig, "securityConfig is null").isInsecureAuthenticationOverHttpAllowed();
    }

    @Override
    public boolean isLoginEnabled(boolean secure)
    {
        // unsecured requests support username-only authentication (no password)
        // secured requests require a password authenticator
        if (secure) {
            return true;
        }
        return insecureAuthenticationOverHttpAllowed;
    }

    @Override
    public boolean isPasswordAllowed(boolean secure)
    {
        return secure;
    }

    @Override
    public Optional<String> isValidCredential(String username, String password, boolean secure)
    {
        if (username == null) {
            return Optional.empty();
        }

        if (!secure) {
            return Optional.of(username)
                    .filter(user -> insecureAuthenticationOverHttpAllowed && password == null);
        }

        List<PasswordAuthenticator> authenticators = passwordAuthenticatorManager.getAuthenticators();
        for (PasswordAuthenticator authenticator : authenticators) {
            try {
                Principal principal = authenticator.createAuthenticatedPrincipal(username, password);
                String authenticatedUser = userMapping.mapUser(principal.toString());
                return Optional.of(authenticatedUser);
            }
            catch (AccessDeniedException | UserMappingException e) {
                // Try another one
            }
            catch (RuntimeException e) {
                log.debug(e, "Error authenticating user for Web UI");
            }
        }
        return Optional.empty();
    }
}
