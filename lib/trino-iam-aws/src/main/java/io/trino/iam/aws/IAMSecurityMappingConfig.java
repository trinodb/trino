package io.trino.iam.aws;
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
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;

public class IAMSecurityMappingConfig
{
    protected File configFile;
    protected URI configUri;
    protected String jsonPointer = "";
    protected String roleCredentialName;
    protected Duration refreshPeriod;
    protected String colonReplacement;

    public Optional<@FileExists File> getConfigFile()
    {
        return Optional.ofNullable(configFile);
    }

    public IAMSecurityMappingConfig setConfigFileInternal(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    public Optional<URI> getConfigUri()
    {
        return Optional.ofNullable(configUri);
    }

    public IAMSecurityMappingConfig setConfigUriInternal(URI configUri)
    {
        this.configUri = configUri;
        return this;
    }

    @NotNull
    public String getJsonPointer()
    {
        return jsonPointer;
    }

    public Optional<String> getRoleCredentialName()
    {
        return Optional.ofNullable(roleCredentialName);
    }

    public IAMSecurityMappingConfig setRoleCredentialNameInternal(String roleCredentialName)
    {
        this.roleCredentialName = roleCredentialName;
        return this;
    }

    public Optional<Duration> getRefreshPeriod()
    {
        return Optional.ofNullable(refreshPeriod);
    }

    public Optional<String> getColonReplacement()
    {
        return Optional.ofNullable(colonReplacement);
    }

    public IAMSecurityMappingConfig setColonReplacementInternal(String colonReplacement)
    {
        this.colonReplacement = colonReplacement;
        return this;
    }

    public boolean validateMappingsConfig()
    {
        return (configFile == null) != (configUri == null);
    }
}
