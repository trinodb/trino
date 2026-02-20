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
package io.trino.plugin.iceberg.encryption;

import org.apache.iceberg.aws.AwsClientFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TestingLocalStackAwsClientFactory
        implements AwsClientFactory
{
    public static final String KMS_ENDPOINT_PROPERTY = "localstack.kms.endpoint";

    private URI kmsEndpoint;

    @Override
    public void initialize(Map<String, String> properties)
    {
        String endpoint = properties.get(KMS_ENDPOINT_PROPERTY);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new IllegalArgumentException("Missing required property: " + KMS_ENDPOINT_PROPERTY);
        }
        kmsEndpoint = URI.create(endpoint);
    }

    @Override
    public S3Client s3()
    {
        throw unsupportedClient("s3");
    }

    @Override
    public S3AsyncClient s3Async()
    {
        throw unsupportedClient("s3Async");
    }

    @Override
    public GlueClient glue()
    {
        throw unsupportedClient("glue");
    }

    @Override
    public KmsClient kms()
    {
        return KmsClient.builder()
                .endpointOverride(requireNonNull(kmsEndpoint, "Factory must be initialized before use"))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Override
    public DynamoDbClient dynamo()
    {
        throw unsupportedClient("dynamo");
    }

    private static UnsupportedOperationException unsupportedClient(String clientName)
    {
        return new UnsupportedOperationException("Testing factory supports only kms client, not " + clientName);
    }
}
