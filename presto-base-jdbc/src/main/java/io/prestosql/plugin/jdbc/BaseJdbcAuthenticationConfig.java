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
package io.prestosql.plugin.jdbc;

import io.airlift.configuration.Config;
import io.prestosql.plugin.jdbc.credential.CredentialProviderType;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static io.prestosql.plugin.jdbc.credential.CredentialProviderType.INLINE;
import static java.util.Objects.requireNonNull;

public class BaseJdbcAuthenticationConfig
{
    private String userCredentialName;
    private String passwordCredentialName;
    private CredentialProviderType credentialProviderType = INLINE;

    @Nullable
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("user-credential-name")
    public BaseJdbcAuthenticationConfig setUserCredentialName(String userCredentialName)
    {
        this.userCredentialName = userCredentialName;
        return this;
    }

    @Nullable
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }

    @Config("password-credential-name")
    public BaseJdbcAuthenticationConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    @NotNull
    public CredentialProviderType getCredentialProviderType()
    {
        return credentialProviderType;
    }

    @Config("credential-provider.type")
    public BaseJdbcAuthenticationConfig setCredentialProviderType(CredentialProviderType credentialProviderType)
    {
        this.credentialProviderType = requireNonNull(credentialProviderType, "credentialProviderType is null");
        return this;
    }
}
