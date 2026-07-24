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
package io.trino.plugin.base.encryption;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import java.net.URI;
import java.util.Optional;

public class AwsKmsConfig
{
    private Optional<String> region = Optional.empty();
    private Optional<URI> endpoint = Optional.empty();
    private Optional<String> stsRegion = Optional.empty();
    private Optional<URI> stsEndpoint = Optional.empty();
    private Optional<String> iamRole = Optional.empty();
    private Optional<String> externalId = Optional.empty();
    private Optional<String> accessKey = Optional.empty();
    private Optional<String> secretKey = Optional.empty();

    public Optional<String> getRegion()
    {
        return region;
    }

    @Config("aws.kms.region")
    @ConfigDescription("AWS Region for KMS")
    public AwsKmsConfig setRegion(String region)
    {
        this.region = Optional.ofNullable(region);
        return this;
    }

    public Optional<URI> getEndpoint()
    {
        return endpoint;
    }

    @Config("aws.kms.endpoint")
    @ConfigDescription("KMS API endpoint URL")
    public AwsKmsConfig setEndpoint(URI endpoint)
    {
        this.endpoint = Optional.ofNullable(endpoint);
        return this;
    }

    public Optional<String> getStsRegion()
    {
        return stsRegion;
    }

    @Config("aws.kms.sts.region")
    @ConfigDescription("AWS STS signing region for KMS authentication")
    public AwsKmsConfig setStsRegion(String stsRegion)
    {
        this.stsRegion = Optional.ofNullable(stsRegion);
        return this;
    }

    public Optional<URI> getStsEndpoint()
    {
        return stsEndpoint;
    }

    @Config("aws.kms.sts.endpoint")
    @ConfigDescription("AWS STS endpoint for KMS authentication")
    public AwsKmsConfig setStsEndpoint(URI stsEndpoint)
    {
        this.stsEndpoint = Optional.ofNullable(stsEndpoint);
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("aws.kms.iam-role")
    @ConfigDescription("ARN of an IAM role to assume when connecting to KMS")
    public AwsKmsConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public Optional<String> getExternalId()
    {
        return externalId;
    }

    @Config("aws.kms.external-id")
    @ConfigDescription("External ID for the IAM role trust policy when connecting to KMS")
    public AwsKmsConfig setExternalId(String externalId)
    {
        this.externalId = Optional.ofNullable(externalId);
        return this;
    }

    public Optional<String> getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.kms.access-key")
    @ConfigDescription("AWS KMS access key")
    public AwsKmsConfig setAccessKey(String accessKey)
    {
        this.accessKey = Optional.ofNullable(accessKey);
        return this;
    }

    public Optional<String> getSecretKey()
    {
        return secretKey;
    }

    @Config("aws.kms.secret-key")
    @ConfigDescription("AWS KMS secret key")
    @ConfigSecuritySensitive
    public AwsKmsConfig setSecretKey(String secretKey)
    {
        this.secretKey = Optional.ofNullable(secretKey);
        return this;
    }
}
