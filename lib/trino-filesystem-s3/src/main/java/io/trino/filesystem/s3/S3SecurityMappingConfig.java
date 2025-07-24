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
package io.trino.filesystem.s3;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.trino.iam.aws.IAMSecurityMappingConfig;
import jakarta.validation.constraints.AssertTrue;

import java.io.File;
import java.net.URI;
import java.util.Optional;

public class S3SecurityMappingConfig
        extends IAMSecurityMappingConfig
{
    private String kmsKeyIdCredentialName;
    private String sseCustomerKeyCredentialName;

    @Config("s3.security-mapping.config-file")
    @ConfigDescription("Path to the JSON security mappings file")
    public S3SecurityMappingConfig setConfigFile(File configFile)
    {
        this.configFile = configFile;
        return this;
    }

    @Config("s3.security-mapping.config-uri")
    @ConfigDescription("HTTP URI of the JSON security mappings")
    public S3SecurityMappingConfig setConfigUri(URI configUri)
    {
        this.configUri = configUri;
        return this;
    }

    @Config("s3.security-mapping.json-pointer")
    @ConfigDescription("JSON pointer (RFC 6901) to mappings inside JSON config")
    public S3SecurityMappingConfig setJsonPointer(String jsonPointer)
    {
        this.jsonPointer = jsonPointer;
        return this;
    }

    @Config("s3.security-mapping.iam-role-credential-name")
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

    @Config("s3.security-mapping.kms-key-id-credential-name")
    @ConfigDescription("Name of the extra credential used to provide KMS Key ID")
    public S3SecurityMappingConfig setKmsKeyIdCredentialName(String kmsKeyIdCredentialName)
    {
        this.kmsKeyIdCredentialName = kmsKeyIdCredentialName;
        return this;
    }

    public Optional<String> getSseCustomerKeyCredentialName()
    {
        return Optional.ofNullable(sseCustomerKeyCredentialName);
    }

    @Config("s3.security-mapping.sse-customer-key-credential-name")
    @ConfigDescription("Name of the extra credential used to provide SSE Customer key")
    public S3SecurityMappingConfig setSseCustomerKeyCredentialName(String sseCustomerKeyCredentialName)
    {
        this.sseCustomerKeyCredentialName = sseCustomerKeyCredentialName;
        return this;
    }

    @Config("s3.security-mapping.refresh-period")
    @ConfigDescription("How often to refresh the security mapping configuration")
    public S3SecurityMappingConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    @Config("s3.security-mapping.colon-replacement")
    @ConfigDescription("Value used in place of colon for IAM role name in extra credentials")
    public S3SecurityMappingConfig setColonReplacement(String colonReplacement)
    {
        this.colonReplacement = colonReplacement;
        return this;
    }

    @AssertTrue(message = "Exactly one of s3.security-mapping.config-file or s3.security-mapping.config-uri must be set")
    public boolean validateMappingsConfig()
    {
        return super.validateMappingsConfig();
    }
}
