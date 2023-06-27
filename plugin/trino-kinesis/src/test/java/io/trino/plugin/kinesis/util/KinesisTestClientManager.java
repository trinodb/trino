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
package io.trino.plugin.kinesis.util;

import io.trino.plugin.kinesis.KinesisClientProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Test implementation of KinesisClientProvider that incorporates a mock Kinesis client.
 */
public class KinesisTestClientManager
        implements KinesisClientProvider
{
    private KinesisClient client = new MockKinesisClient();
    private final DynamoDbAsyncClient dynamoDBClient;
    private final S3Client amazonS3Client;

    public KinesisTestClientManager()
    {
        this.dynamoDBClient = DynamoDbAsyncClient.builder()
                .httpClient(NettyNioAsyncHttpClient.create())
                .build();
        this.amazonS3Client = S3Client.builder()
                .httpClient(ApacheHttpClient.create())
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
        return this.dynamoDBClient;
    }

    @Override
    public S3Client getS3Client()
    {
        return amazonS3Client;
    }
}
