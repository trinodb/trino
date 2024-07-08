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
package io.trino.configuration.keystore;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

public class KeystoreConfigurationResolverConfig
{
    private String keyStoreFilePath;
    private String keyStoreType;
    private String keyStorePassword;

    @Config("keystore-file-path")
    public KeystoreConfigurationResolverConfig setKeyStoreFilePath(String keyStoreFilePath)
    {
        this.keyStoreFilePath = keyStoreFilePath;
        return this;
    }

    @NotNull
    @FileExists
    public String getKeyStoreFilePath()
    {
        return keyStoreFilePath;
    }

    @Config("keystore-type")
    public KeystoreConfigurationResolverConfig setKeyStoreType(String keyStoreType)
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
    public KeystoreConfigurationResolverConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }
}
