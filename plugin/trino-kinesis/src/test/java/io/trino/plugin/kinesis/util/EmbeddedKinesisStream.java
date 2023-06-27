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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DeleteStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.io.Closeable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.services.kinesis.model.StreamStatus.ACTIVE;

public class EmbeddedKinesisStream
        implements Closeable
{
    private final KinesisClient amazonKinesisClient;

    public EmbeddedKinesisStream(String accessKey, String secretKey)
    {
        AwsBasicCredentials awsCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        this.amazonKinesisClient = KinesisClient.builder()
                .httpClient(ApacheHttpClient.create())
                .credentialsProvider(StaticCredentialsProvider.create(awsCredentials))
                .build();
    }

    @Override
    public void close()
    {
        amazonKinesisClient.close();
    }

    private StreamStatus checkStreamStatus(String streamName)
    {
        DescribeStreamSummaryRequest describeStreamRequest = DescribeStreamSummaryRequest.builder()
                .streamName(streamName)
                .build();
        return amazonKinesisClient.describeStreamSummary(describeStreamRequest)
                .streamDescriptionSummary()
                .streamStatus();
    }

    public void createStream(int shardCount, String streamName)
    {
        CreateStreamRequest createStreamRequest = CreateStreamRequest
                .builder()
                .streamName(streamName)
                .shardCount(shardCount)
                .build();

        amazonKinesisClient.createStream(createStreamRequest);
        try {
            while (!checkStreamStatus(streamName).equals(ACTIVE)) {
                MILLISECONDS.sleep(1000);
            }
        }
        catch (Exception ignored) {
        }
    }

    public KinesisClient getKinesisClient()
    {
        return amazonKinesisClient;
    }

    public void deleteStream(String streamName)
    {
        DeleteStreamRequest deleteStreamRequest = DeleteStreamRequest.builder()
                .streamName(streamName)
                .build();
        amazonKinesisClient.deleteStream(deleteStreamRequest);
    }
}
