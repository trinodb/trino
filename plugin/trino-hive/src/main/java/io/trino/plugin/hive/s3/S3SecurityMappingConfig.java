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
package io.trino.plugin.hive.s3;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class S3SecurityMappingConfig
{
    private String configFilePath;
    private String jsonPointer = "";
    private String roleCredentialName;
    private String kmsKeyIdCredentialName;
    private Duration refreshPeriod;
    private String colonReplacement;

    public Optional<String> getConfigFilePath()
    {
        return Optional.ofNullable(configFilePath);
    }

    @Config("hive.s3.security-mapping.config-file")
    @ConfigDescription("JSON configuration file containing security mappings")
    public S3SecurityMappingConfig setConfigFilePath(String configFilePath)
    {
        this.configFilePath = configFilePath;
        return this;
    }

    @NotNull
    public String getJsonPointer()
    {
        return jsonPointer;
    }

    @Config("hive.s3.security-mapping.json-pointer")
    @ConfigDescription("JSON pointer (RFC 6901) to mappings inside JSON config")
    public S3SecurityMappingConfig setJsonPointer(String jsonPointer)
    {
        this.jsonPointer = jsonPointer;
        return this;
    }

    public Optional<String> getRoleCredentialName()
    {
        return Optional.ofNullable(roleCredentialName);
    }

    @Config("hive.s3.security-mapping.iam-role-credential-name")
    @ConfigDescription("Name of the extra credential used to provide IAM role")
    public S3SecurityMappingConfig setRoleCredentialName(String roleCredentialName)
    {
        this.roleCredentialName = roleCredentialName;
        return this;
    }

    public Optional<String> getKmsKeyIdCredentialName()
    {
        return Optional.ofNullable(kmsKeyIdCredentialName);
    }

    @Config("hive.s3.security-mapping.kms-key-id-credential-name")
    @ConfigDescription("Name of the extra credential used to provide KMS Key ID")
    public S3SecurityMappingConfig setKmsKeyIdCredentialName(String kmsKeyIdCredentialName)
    {
        this.kmsKeyIdCredentialName = kmsKeyIdCredentialName;
        return this;
    }

    public Optional<Duration> getRefreshPeriod()
    {
        return Optional.ofNullable(refreshPeriod);
    }

    @Config("hive.s3.security-mapping.refresh-period")
    @ConfigDescription("How often to refresh the security mapping configuration")
    public S3SecurityMappingConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    public Optional<String> getColonReplacement()
    {
        return Optional.ofNullable(colonReplacement);
    }

    @Config("hive.s3.security-mapping.colon-replacement")
    @ConfigDescription("Value used in place of colon for IAM role name in extra credentials")
    public S3SecurityMappingConfig setColonReplacement(String colonReplacement)
    {
        this.colonReplacement = colonReplacement;
        return this;
    }
}
