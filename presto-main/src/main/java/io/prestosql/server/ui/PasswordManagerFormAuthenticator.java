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
package io.prestosql.server.ui;

import io.prestosql.server.security.PasswordAuthenticatorManager;
import io.prestosql.server.security.SecurityConfig;
import io.prestosql.spi.security.AccessDeniedException;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class PasswordManagerFormAuthenticator
        implements FormAuthenticator
{
    private final PasswordAuthenticatorManager passwordAuthenticatorManager;
    private final boolean insecureAuthenticationOverHttpAllowed;

    @Inject
    public PasswordManagerFormAuthenticator(PasswordAuthenticatorManager passwordAuthenticatorManager, SecurityConfig securityConfig)
    {
        this.passwordAuthenticatorManager = requireNonNull(passwordAuthenticatorManager, "passwordAuthenticatorManager is null");
        passwordAuthenticatorManager.setRequired();
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
    public boolean isValidCredential(String username, String password, boolean secure)
    {
        if (username == null) {
            return false;
        }

        if (!secure) {
            return insecureAuthenticationOverHttpAllowed && password == null;
        }

        try {
            passwordAuthenticatorManager.getAuthenticator().createAuthenticatedPrincipal(username, password);
            return true;
        }
        catch (AccessDeniedException e) {
            return false;
        }
    }
}
