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
package io.trino.plugin.dynamodb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class DynamoDbConfig
{
    private Optional<String> awsAccessKey = Optional.empty();
    private Optional<String> awsSecretKey = Optional.empty();
    private String awsRegion;
    private Optional<String> iamRole = Optional.empty();
    private Optional<String> externalId = Optional.empty();
    private int scanSegments = 1;
    private Optional<URI> endpointOverride = Optional.empty();

    public Optional<String> getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("dynamodb.aws-access-key")
    @ConfigDescription("AWS access key ID for authenticating with DynamoDB")
    public DynamoDbConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = Optional.ofNullable(awsAccessKey);
        return this;
    }

    public Optional<String> getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("dynamodb.aws-secret-key")
    @ConfigDescription("AWS secret access key for authenticating with DynamoDB")
    @ConfigSecuritySensitive
    public DynamoDbConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = Optional.ofNullable(awsSecretKey);
        return this;
    }

    @NotNull
    public String getAwsRegion()
    {
        return awsRegion;
    }

    @Config("dynamodb.aws-region")
    @ConfigDescription("AWS region where the DynamoDB instance is located (e.g. us-east-1)")
    public DynamoDbConfig setAwsRegion(String awsRegion)
    {
        this.awsRegion = awsRegion;
        return this;
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    @Config("dynamodb.aws-iam-role")
    @ConfigDescription("ARN of the IAM role to assume when connecting to DynamoDB")
    public DynamoDbConfig setIamRole(String iamRole)
    {
        this.iamRole = Optional.ofNullable(iamRole);
        return this;
    }

    public Optional<String> getExternalId()
    {
        return externalId;
    }

    @Config("dynamodb.aws-external-id")
    @ConfigDescription("External ID to use when assuming the IAM role (for cross-account access)")
    public DynamoDbConfig setExternalId(String externalId)
    {
        this.externalId = Optional.ofNullable(externalId);
        return this;
    }

    @Min(1)
    public int getScanSegments()
    {
        return scanSegments;
    }

    @Config("dynamodb.scan-segments")
    @ConfigDescription("Number of parallel scan segments per table; higher values increase parallelism for large tables")
    public DynamoDbConfig setScanSegments(int scanSegments)
    {
        this.scanSegments = scanSegments;
        return this;
    }

    public Optional<URI> getEndpointOverride()
    {
        return endpointOverride;
    }

    @Config("dynamodb.endpoint-override")
    @ConfigDescription("Override the DynamoDB endpoint URL (for testing with DynamoDB Local)")
    public DynamoDbConfig setEndpointOverride(URI endpointOverride)
    {
        this.endpointOverride = Optional.ofNullable(endpointOverride);
        return this;
    }
}
