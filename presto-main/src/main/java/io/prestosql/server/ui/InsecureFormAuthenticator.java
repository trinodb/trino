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

import io.prestosql.server.security.SecurityConfig;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class InsecureFormAuthenticator
        implements FormAuthenticator
{
    private final boolean insecureAuthenticationOverHttpAllowed;

    @Inject
    public InsecureFormAuthenticator(SecurityConfig securityConfig)
    {
        insecureAuthenticationOverHttpAllowed = requireNonNull(securityConfig, "securityConfig is null").isInsecureAuthenticationOverHttpAllowed();
    }

    @Override
    public boolean isLoginEnabled(boolean secure)
    {
        return secure || insecureAuthenticationOverHttpAllowed;
    }

    @Override
    public boolean isPasswordAllowed(boolean secure)
    {
        return false;
    }

    @Override
    public boolean isValidCredential(String username, String password, boolean secure)
    {
        if (username == null) {
            return false;
        }

        return isLoginEnabled(secure) && password == null;
    }
}
