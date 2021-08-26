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
package io.trino.plugin.elasticsearch;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.NotNull;

import java.util.Optional;

@DefunctConfig("elasticsearch.aws.use-instance-credentials")
public class AwsSecurityConfig
{
    private String accessKey;
    private String secretKey;
    private String region;
    private String iamRole;
    private String externalId;

    @NotNull
    public Optional<String> getAccessKey()
    {
        return Optional.ofNullable(accessKey);
    }

    @Config("elasticsearch.aws.access-key")
    public AwsSecurityConfig setAccessKey(String key)
    {
        this.accessKey = key;
        return this;
    }

    @NotNull
    public Optional<String> getSecretKey()
    {
        return Optional.ofNullable(secretKey);
    }

    @Config("elasticsearch.aws.secret-key")
    @ConfigSecuritySensitive
    public AwsSecurityConfig setSecretKey(String key)
    {
        this.secretKey = key;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    @Config("elasticsearch.aws.region")
    public AwsSecurityConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    @NotNull
    public Optional<String> getIamRole()
    {
        return Optional.ofNullable(iamRole);
    }

    @Config("elasticsearch.aws.iam-role")
    @ConfigDescription("Optional AWS IAM role to assume for authenticating. If set, this role will be used to get credentials to sign requests to ES.")
    public AwsSecurityConfig setIamRole(String iamRole)
    {
        this.iamRole = iamRole;
        return this;
    }

    @NotNull
    public Optional<String> getExternalId()
    {
        return Optional.ofNullable(externalId);
    }

    @Config("elasticsearch.aws.external-id")
    @ConfigDescription("Optional external id to pass to AWS STS while assuming a role")
    public AwsSecurityConfig setExternalId(String externalId)
    {
        this.externalId = externalId;
        return this;
    }
}
