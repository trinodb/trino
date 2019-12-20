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
package io.prestosql.plugin.kinesis;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Inject;

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
    private final AmazonKinesisClient client;
    private final AmazonS3Client amazonS3Client;
    private final AmazonDynamoDBClient dynamoDbClient;              // for Checkpointing

    @Inject
    public KinesisClientManager(KinesisConfig config)
    {
        if (!isNullOrEmpty(config.getAccessKey()) && !isNullOrEmpty(config.getSecretKey())) {
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey());
            this.client = new AmazonKinesisClient(awsCredentials);
            this.amazonS3Client = new AmazonS3Client(awsCredentials);
            this.dynamoDbClient = new AmazonDynamoDBClient(awsCredentials);
        }
        else {
            DefaultAWSCredentialsProviderChain defaultChain = new DefaultAWSCredentialsProviderChain();
            this.client = new AmazonKinesisClient(defaultChain);
            this.amazonS3Client = new AmazonS3Client(defaultChain);
            this.dynamoDbClient = new AmazonDynamoDBClient(defaultChain);
        }

        this.client.setEndpoint("kinesis." + config.getAwsRegion() + ".amazonaws.com");
        this.dynamoDbClient.setEndpoint("dynamodb." + config.getAwsRegion() + ".amazonaws.com");
    }

    @Override
    public AmazonKinesisClient getClient()
    {
        return client;
    }

    @Override
    public AmazonDynamoDBClient getDynamoDbClient()
    {
        return dynamoDbClient;
    }

    @Override
    public AmazonS3Client getS3Client()
    {
        return amazonS3Client;
    }
}
