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
package com.qubole.presto.kinesis;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.inject.Inject;
import io.airlift.log.Logger;

/**
 * Creates and manages AWS clients for this connector.
 * <p>
 * Note: credentials can be supplied explicitly through the configuration.  However when these are
 * omitted, the default AWS provider chain is used (which includes instance profile credentials).
 */
public class KinesisClientManager
        implements KinesisClientProvider
{
    private static final Logger log = Logger.get(KinesisClientManager.class);
    private final AmazonKinesisClient client;
    private final KinesisAwsCredentials kinesisAwsCredentials;
    private final AmazonS3Client amazonS3Client;
    private final AmazonDynamoDBClient dynamoDBClient;              // for Checkpointing

    @Inject
    KinesisClientManager(KinesisConnectorConfig kinesisConnectorConfig)
    {
        log.info("Creating new client for Consumer");
        if (nonEmpty(kinesisConnectorConfig.getAccessKey()) && nonEmpty(kinesisConnectorConfig.getSecretKey())) {
            this.kinesisAwsCredentials = new KinesisAwsCredentials(kinesisConnectorConfig.getAccessKey(), kinesisConnectorConfig.getSecretKey());
            this.client = new AmazonKinesisClient(this.kinesisAwsCredentials);
            this.amazonS3Client = new AmazonS3Client(this.kinesisAwsCredentials);
            this.dynamoDBClient = new AmazonDynamoDBClient(this.kinesisAwsCredentials);
        }
        else {
            this.kinesisAwsCredentials = null;
            DefaultAWSCredentialsProviderChain defaultChain = new DefaultAWSCredentialsProviderChain();
            this.client = new AmazonKinesisClient(defaultChain);
            this.amazonS3Client = new AmazonS3Client(defaultChain);
            this.dynamoDBClient = new AmazonDynamoDBClient(defaultChain);
        }

        this.client.setEndpoint("kinesis." + kinesisConnectorConfig.getAwsRegion() + ".amazonaws.com");
        this.dynamoDBClient.setEndpoint("dynamodb." + kinesisConnectorConfig.getAwsRegion() + ".amazonaws.com");
    }

    @Override
    public AmazonKinesisClient getClient()
    {
        return client;
    }

    @Override
    public AmazonDynamoDBClient getDynamoDBClient()
    {
        return dynamoDBClient;
    }

    @Override
    public AmazonS3Client getS3Client()
    {
        return amazonS3Client;
    }

    @Override
    public DescribeStreamRequest getDescribeStreamRequest()
    {
        return new DescribeStreamRequest();
    }

    public boolean nonEmpty(String str)
    {
        return str != null && !str.isEmpty();
    }
}
