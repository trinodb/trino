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
package io.trino.plugin.kinesis;

import com.google.inject.Inject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Creates and manages AWS clients for this connector.
 * <p>
 * Note: credentials can be supplied explicitly through the configuration.  However when these are
 * omitted, the default AWS provider chain is used (which includes instance profile credentials).
 */
public class KinesisClientManager
        implements KinesisClientProvider
{
    private final KinesisClient client;
    private final S3Client amazonS3Client;
    private final DynamoDbAsyncClient dynamoDbClient;              // for Checkpointing

    @Inject
    public KinesisClientManager(KinesisConfig config)
    {
        AwsCredentialsProvider credentialsProvider;
        if (!isNullOrEmpty(config.getAccessKey()) && !isNullOrEmpty(config.getSecretKey())) {
            AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
            credentialsProvider = StaticCredentialsProvider.create(awsCredentials);
        }
        else {
            credentialsProvider = DefaultCredentialsProvider.create();
        }

        this.client = KinesisClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(config.getAwsRegion()))
                .httpClient(ApacheHttpClient.create())
                .build();
        this.amazonS3Client = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(config.getAwsRegion()))
                .crossRegionAccessEnabled(true)
                .httpClient(ApacheHttpClient.create())
                .build();

        this.dynamoDbClient = DynamoDbAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(config.getAwsRegion()))
                .httpClient(NettyNioAsyncHttpClient.create())
                .build();
    }

    @Override
    public KinesisClient getClient()
    {
        return client;
    }

    @Override
    public DynamoDbAsyncClient getDynamoDbClient()
    {
        return dynamoDbClient;
    }

    @Override
    public S3Client getS3Client()
    {
        return amazonS3Client;
    }
}
