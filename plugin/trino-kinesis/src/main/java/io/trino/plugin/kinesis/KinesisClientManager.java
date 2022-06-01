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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.s3.AmazonS3;
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
    private final AmazonKinesis client;
    private final AmazonS3 amazonS3Client;
    private final AmazonDynamoDB dynamoDbClient;              // for Checkpointing

    @Inject
    public KinesisClientManager(KinesisConfig config)
    {
        AWSCredentialsProvider awsCredentials;
        if (isNullOrEmpty(config.getAccessKey()) || isNullOrEmpty(config.getSecretKey())) {
            awsCredentials = new DefaultAWSCredentialsProviderChain();
        }
        else {
            awsCredentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getAccessKey(), config.getSecretKey()));
        }
        this.client = AmazonKinesisClient.builder()
                .withCredentials(awsCredentials)
                .withRegion(config.getAwsRegion())
                .build();
        this.amazonS3Client = AmazonS3Client.builder()
                .withCredentials(awsCredentials)
                .withRegion(config.getAwsRegion())
                .build();
        this.dynamoDbClient = AmazonDynamoDBClient.builder()
                .withCredentials(awsCredentials)
                .withRegion(config.getAwsRegion())
                .build();
    }

    @Override
    public AmazonKinesis getClient()
    {
        return client;
    }

    @Override
    public AmazonDynamoDB getDynamoDbClient()
    {
        return dynamoDbClient;
    }

    @Override
    public AmazonS3 getS3Client()
    {
        return amazonS3Client;
    }
}
