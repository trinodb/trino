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
package io.prestosql.plugin.jdbc.credential.keystore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

public class KeyStoreBasedCredentialProviderConfig
{
    private String keyStoreFilePath;
    private String keyStoreType;
    private String keyStorePassword;
    private String userCredentialName;
    private String passwordForUserCredentialName;
    private String passwordCredentialName;
    private String passwordForPasswordCredentialName;

    @Config("keystore-file-path")
    public KeyStoreBasedCredentialProviderConfig setKeyStoreFilePath(String keyStoreFilePath)
    {
        this.keyStoreFilePath = keyStoreFilePath;
        return this;
    }

    @NotNull
    public String getKeyStoreFilePath()
    {
        return keyStoreFilePath;
    }

    @Config("keystore-type")
    public KeyStoreBasedCredentialProviderConfig setKeyStoreType(String keyStoreType)
    {
        this.keyStoreType = keyStoreType;
        return this;
    }

    @NotNull
    public String getKeyStoreType()
    {
        return keyStoreType;
    }

    @Config("keystore-password")
    @ConfigSecuritySensitive
    public KeyStoreBasedCredentialProviderConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    @Config("keystore-user-credential-password")
    public KeyStoreBasedCredentialProviderConfig setPasswordForUserCredentialName(String passwordForUserCredentialName)
    {
        this.passwordForUserCredentialName = passwordForUserCredentialName;
        return this;
    }

    @NotNull
    public String getPasswordForUserCredentialName()
    {
        return passwordForUserCredentialName;
    }

    @Config("keystore-password-credential-password")
    public KeyStoreBasedCredentialProviderConfig setPasswordForPasswordCredentialName(String passwordForPasswordCredentialName)
    {
        this.passwordForPasswordCredentialName = passwordForPasswordCredentialName;
        return this;
    }

    public String getPasswordForPasswordCredentialName()
    {
        return passwordForPasswordCredentialName;
    }

    @NotNull
    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("keystore-user-credential-name")
    public KeyStoreBasedCredentialProviderConfig setUserCredentialName(String userCredntialName)
    {
        this.userCredentialName = userCredntialName;
        return this;
    }

    @NotNull
    public String getUserCredentialName()
    {
        return userCredentialName;
    }

    @Config("keystore-password-credential-name")
    public KeyStoreBasedCredentialProviderConfig setPasswordCredentialName(String passwordCredentialName)
    {
        this.passwordCredentialName = passwordCredentialName;
        return this;
    }

    @NotNull
    public String getPasswordCredentialName()
    {
        return passwordCredentialName;
    }
}
