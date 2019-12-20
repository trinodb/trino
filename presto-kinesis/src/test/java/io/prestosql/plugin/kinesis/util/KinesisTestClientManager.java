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
package io.prestosql.plugin.kinesis.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import io.prestosql.plugin.kinesis.KinesisClientProvider;

/**
 * Test implementation of KinesisClientProvider that incorporates a mock Kinesis client.
 */
public class KinesisTestClientManager
        implements KinesisClientProvider
{
    private AmazonKinesisClient client = new MockKinesisClient();
    private final AmazonDynamoDBClient dynamoDBClient;
    private final AmazonS3Client amazonS3Client;

    public KinesisTestClientManager()
    {
        this.dynamoDBClient = new AmazonDynamoDBClient();
        this.amazonS3Client = new AmazonS3Client();
    }

    @Override
    public AmazonKinesisClient getClient()
    {
        return client;
    }

    @Override
    public AmazonDynamoDBClient getDynamoDbClient()
    {
        return this.dynamoDBClient;
    }

    @Override
    public AmazonS3Client getS3Client()
    {
        return amazonS3Client;
    }
}
