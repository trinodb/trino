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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.aws.AwsProperties;

import java.util.Optional;

public class IcebergRestCatalogSigV4Config
{
    private String signingName = AwsProperties.REST_SIGNING_NAME_DEFAULT;
    private boolean stsWebIdentity;
    private String stsRoleArn;
    private String stsPolicy;
    private Integer stsDurationSeconds;

    @NotNull
    public String getSigningName()
    {
        return signingName;
    }

    @Config("iceberg.rest-catalog.signing-name")
    @ConfigDescription("AWS SigV4 signing service name")
    public IcebergRestCatalogSigV4Config setSigningName(String signingName)
    {
        this.signingName = signingName;
        return this;
    }

    public boolean isStsWebIdentity()
    {
        return stsWebIdentity;
    }

    @Config("iceberg.rest-catalog.sts-web-identity")
    @ConfigDescription("Exchange the authenticated user's OIDC token for per-user STS credentials via AssumeRoleWithWebIdentity")
    public IcebergRestCatalogSigV4Config setStsWebIdentity(boolean stsWebIdentity)
    {
        this.stsWebIdentity = stsWebIdentity;
        return this;
    }

    public Optional<String> getStsRoleArn()
    {
        return Optional.ofNullable(stsRoleArn);
    }

    @Config("iceberg.rest-catalog.sts-role-arn")
    @ConfigDescription("IAM role ARN to assume via AssumeRoleWithWebIdentity")
    public IcebergRestCatalogSigV4Config setStsRoleArn(String stsRoleArn)
    {
        this.stsRoleArn = stsRoleArn;
        return this;
    }

    public Optional<String> getStsPolicy()
    {
        return Optional.ofNullable(stsPolicy);
    }

    @Config("iceberg.rest-catalog.sts-policy")
    @ConfigDescription("Inline IAM session policy JSON to attach to the AssumeRoleWithWebIdentity request")
    public IcebergRestCatalogSigV4Config setStsPolicy(String stsPolicy)
    {
        this.stsPolicy = stsPolicy;
        return this;
    }

    public Optional<Integer> getStsDurationSeconds()
    {
        return Optional.ofNullable(stsDurationSeconds);
    }

    @Config("iceberg.rest-catalog.sts-duration-seconds")
    @ConfigDescription("Fixed STS session duration in seconds; if unset, the duration is derived from the token's exp claim")
    public IcebergRestCatalogSigV4Config setStsDurationSeconds(Integer stsDurationSeconds)
    {
        this.stsDurationSeconds = stsDurationSeconds;
        return this;
    }

    @AssertTrue(message = "iceberg.rest-catalog.sts-role-arn and iceberg.rest-catalog.sts-policy cannot both be set")
    public boolean isStsRoleArnAndPolicyMutuallyExclusive()
    {
        return stsRoleArn == null || stsPolicy == null;
    }
}
