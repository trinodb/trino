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
package io.prestosql.plugin.hive.s3;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;

import java.io.File;
import java.util.Optional;

public class S3SecurityMappingConfig
{
    private File configFile;
    private String roleCredentialName;
    private Duration refreshPeriod;
    private String colonReplacement;

    public Optional<@FileExists File> getConfigFile()
    {
        return Optional.ofNullable(configFile);
    }

    @Config("hive.s3.security-mapping.config-file")
    @ConfigDescription("JSON configuration file containing security mappings")
    public S3SecurityMappingConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
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
