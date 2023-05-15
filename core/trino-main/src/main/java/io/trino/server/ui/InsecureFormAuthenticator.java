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

import com.google.inject.Inject;
import io.trino.server.security.InsecureAuthenticatorConfig;
import io.trino.server.security.SecurityConfig;
import io.trino.server.security.UserMapping;
import io.trino.server.security.UserMappingException;

import java.util.Optional;

import static io.trino.server.security.UserMapping.createUserMapping;

public class InsecureFormAuthenticator
        implements FormAuthenticator
{
    private final UserMapping userMapping;
    private final boolean insecureAuthenticationOverHttpAllowed;

    @Inject
    public InsecureFormAuthenticator(InsecureAuthenticatorConfig config, SecurityConfig securityConfig)
    {
        userMapping = createUserMapping(config.getUserMappingPattern(), config.getUserMappingFile());
        insecureAuthenticationOverHttpAllowed = securityConfig.isInsecureAuthenticationOverHttpAllowed();
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
    public Optional<String> isValidCredential(String username, String password, boolean secure)
    {
        if (username == null) {
            return Optional.empty();
        }

        if (isLoginEnabled(secure) && password == null) {
            try {
                return Optional.of(userMapping.mapUser(username));
            }
            catch (UserMappingException ignored) {
            }
        }
        return Optional.empty();
    }
}
