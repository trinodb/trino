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
package io.trino.plugin.hive.metastore.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.trino.iam.aws.IAMSecurityMappingConfig;
import jakarta.validation.constraints.AssertTrue;

import java.io.File;
import java.net.URI;

public class GlueSecurityMappingConfig
        extends IAMSecurityMappingConfig
{
    @Config("hive.metastore.glue.security-mapping.config-file")
    @ConfigDescription("Path to the JSON security mappings file")
    public GlueSecurityMappingConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    @Config("hive.metastore.glue.security-mapping.config-uri")
    @ConfigDescription("HTTP URI of the JSON security mappings")
    public GlueSecurityMappingConfig setConfigUri(URI configUri)
    {
        this.configUri = configUri;
        return this;
    }

    @Config("hive.metastore.glue.security-mapping.json-pointer")
    @ConfigDescription("JSON pointer (RFC 6901) to mappings inside JSON config")
    public GlueSecurityMappingConfig setJsonPointer(String jsonPointer)
    {
        this.jsonPointer = jsonPointer;
        return this;
    }

    @Config("hive.metastore.glue.security-mapping.iam-role-credential-name")
    @ConfigDescription("Name of the extra credential used to provide IAM role")
    public GlueSecurityMappingConfig setRoleCredentialName(String roleCredentialName)
    {
        this.roleCredentialName = roleCredentialName;
        return this;
    }

    @Config("hive.metastore.glue.security-mapping.refresh-period")
    @ConfigDescription("How often to refresh the security mapping configuration")
    public GlueSecurityMappingConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    @Config("hive.metastore.glue.security-mapping.colon-replacement")
    @ConfigDescription("Value used in place of colon for IAM role name in extra credentials")
    public GlueSecurityMappingConfig setColonReplacement(String colonReplacement)
    {
        this.colonReplacement = colonReplacement;
        return this;
    }

    @AssertTrue(message = "Exactly one of hive.metastore.glue.security-mapping.config-file or hive.metastore.glue.security-mapping.config-uri must be set")
    public boolean validateMappingsConfig()
    {
        return super.validateMappingsConfig();
    }
}
