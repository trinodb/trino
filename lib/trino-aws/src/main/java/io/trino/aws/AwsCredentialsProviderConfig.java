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
package io.trino.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import jakarta.annotation.PostConstruct;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AwsCredentialsProviderConfig
{
    public static final String DEFAULT_ROLE_SESSION_NAME = "trino-session";

    private Optional<URI> serviceUri = Optional.empty();
    private Optional<String> customCredentialsProvider = Optional.empty();
    private CustomProviderResolver customProviderResolver = providerClass -> Class.forName(providerClass)
            .asSubclass(AWSCredentialsProvider.class)
            .getConstructor()
            .newInstance();
    private Optional<String> accessKey = Optional.empty();
    private Optional<String> secretKey = Optional.empty();
    private Optional<String> sessionToken = Optional.empty();
    private Optional<String> iamRole = Optional.empty();
    private String iamRoleSessionName = DEFAULT_ROLE_SESSION_NAME;
    private Optional<String> region = Optional.empty();
    private Optional<String> stsRegion = Optional.empty();
    private Optional<String> stsEndpoint = Optional.empty();
    private Optional<String> proxyApiId = Optional.empty();
    private Optional<String> externalId = Optional.empty();

    public Optional<URI> getServiceUri()
    {
        return serviceUri;
    }

    @LegacyConfig("endpoint-url")
    @Config("service-uri")
    @ConfigDescription("URI of the particular AWS service we want to authenticate with")
    public AwsCredentialsProviderConfig setServiceUri(URI serviceUri)
    {
        this.serviceUri = Optional.ofNullable(serviceUri);
        return this;
    }

    public Optional<String> getCustomCredentialsProvider()
    {
        return customCredentialsProvider;
    }

    @LegacyConfig("aws-credentials-provider")
    @Config("credentials-provider")
    @ConfigDescription("Fully qualified name of the Java class to use for obtaining AWS credentials")
    public AwsCredentialsProviderConfig setCustomCredentialsProvider(String customCredentialsProvider)
    {
        this.customCredentialsProvider = Optional.ofNullable(customCredentialsProvider);
        return this;
    }

    public CustomProviderResolver getCustomProviderResolver()
    {
        return customProviderResolver;
    }

    public AwsCredentialsProviderConfig setCustomProviderResolver(CustomProviderResolver customProviderResolver)
    {
        this.customProviderResolver = requireNonNull(customProviderResolver, "customProviderResolver is null");
        return this;
    }

    public Optional<String> getAccessKey()
    {
        return accessKey;
    }

    @LegacyConfig("aws-access-key")
    @Config("access-key")
    @ConfigDescription("AWS access key")
    public AwsCredentialsProviderConfig setAccessKey(String accessKey)
    {
        this.accessKey = Optional.ofNullable(accessKey);
        return this;
    }

    public Optional<String> getSecretKey()
    {
        return secretKey;
    }

    @LegacyConfig("aws-secret-key")
    @Config("secret-key")
    @ConfigDescription("AWS secret key")
    @ConfigSecuritySensitive
    public AwsCredentialsProviderConfig setSecretKey(String secretKey)
    {
        this.secretKey = Optional.ofNullable(secretKey);
        return this;
    }

    public Optional<String> getSessionToken()
    {
        return sessionToken;
    }

    @Config("session-token")
    @ConfigDescription("AWS session token")
    public AwsCredentialsProviderConfig setSessionToken(String sessionToken)
    {
        this.sessionToken = Optional.ofNullable(sessionToken);
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("iam-role")
    @ConfigDescription("ARN of an IAM role to assume when authenticating")
    public AwsCredentialsProviderConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public String getIamRoleSessionName()
    {
        return iamRoleSessionName;
    }

    @Config("iam-session-name")
    @ConfigDescription("Name of the AWS session for the purpose of assuming the IAM role")
    public AwsCredentialsProviderConfig setIamRoleSessionName(String iamRoleSessionName)
    {
        this.iamRoleSessionName = requireNonNull(iamRoleSessionName, "iamRoleSessionName is null");
        return this;
    }

    public Optional<String> getRegion()
    {
        return region;
    }

    @LegacyConfig("region-name")
    @Config("region")
    @ConfigDescription("AWS Region of the role to assume when authenticating")
    public AwsCredentialsProviderConfig setRegion(String region)
    {
        this.region = Optional.ofNullable(region);
        return this;
    }

    public Optional<String> getStsEndpoint()
    {
        return stsEndpoint;
    }

    @LegacyConfig("sts.endpoing")
    @Config("sts-endpoint")
    @ConfigDescription("URI of the STS endpoint - will override the AWS default")
    public AwsCredentialsProviderConfig setStsEndpoint(String stsEndpoint)
    {
        this.stsEndpoint = Optional.ofNullable(stsEndpoint);
        return this;
    }

    public Optional<String> getStsRegion()
    {
        return stsRegion;
    }

    @LegacyConfig("sts.region")
    @Config("sts-region")
    @ConfigDescription("AWS Region of the STS endpoint")
    public AwsCredentialsProviderConfig setStsRegion(String stsRegion)
    {
        this.stsRegion = Optional.ofNullable(stsRegion);
        return this;
    }

    public Optional<String> getProxyApiId()
    {
        return proxyApiId;
    }

    @Config("proxy-api-id")
    @ConfigDescription("ID of Proxy API")
    public AwsCredentialsProviderConfig setProxyApiId(String proxyApiId)
    {
        this.proxyApiId = Optional.ofNullable(proxyApiId);
        return this;
    }

    public Optional<String> getExternalId()
    {
        return externalId;
    }

    @Config("external-id")
    @ConfigDescription("External ID for the IAM role trust policy when authenticating")
    public AwsCredentialsProviderConfig setExternalId(String externalId)
    {
        this.externalId = Optional.ofNullable(externalId);
        return this;
    }

    @PostConstruct
    public void validate()
    {
        if (getProxyApiId().isPresent()) {
            checkState(getRegion().isPresent() && getServiceUri().isPresent(),
                    "Both region and endpoint URL must be provided when proxy API ID is present");
        }
    }
}
