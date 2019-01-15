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
package com.qubole.presto.kinesis.util;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.qubole.presto.kinesis.KinesisAwsCredentials;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EmbeddedKinesisStream
        implements Closeable
{
    private KinesisAwsCredentials awsCredentials;
    private AmazonKinesisClient amazonKinesisClient;
    private ArrayList<String> streamsCreated = new ArrayList<String>();

    public EmbeddedKinesisStream(String accessKey, String secretKey)
    {
        this.awsCredentials = new KinesisAwsCredentials(accessKey, secretKey);
        this.amazonKinesisClient = new AmazonKinesisClient(awsCredentials);
    }

    @Override
    public void close()
            throws IOException
    {
    }

    public void createStreams(String... streamNames)
    {
        createStreams(2, streamNames);
    }

    public void createStreams(int shardCount, String... streamNames)
    {
        for (String streamName : streamNames) {
            createStream(shardCount, streamName);
        }
    }

    private String checkStreamStatus(String streamName)
    {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);

        StreamDescription streamDescription = amazonKinesisClient.describeStream(describeStreamRequest).getStreamDescription();
        return streamDescription.getStreamStatus();
    }

    public void createStream(int shardCount, String streamName)
    {
        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName(streamName);
        createStreamRequest.setShardCount(shardCount);

        amazonKinesisClient.createStream(createStreamRequest);
        try {
            while (checkStreamStatus(streamName).equals("ACTIVE") == false) {
                MILLISECONDS.sleep(1000);
            }
        }
        catch (Exception e) {
        }

        streamsCreated.add(streamName);
    }

    public AmazonKinesisClient getKinesisClient()
    {
        return amazonKinesisClient;
    }

    public void delteStream(String streamName)
    {
        DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
        deleteStreamRequest.setStreamName(streamName);
        amazonKinesisClient.deleteStream(deleteStreamRequest);
        if (streamsCreated.contains(streamName)) {
            streamsCreated.remove(streamName);
        }
    }
}
