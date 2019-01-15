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

import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import javax.inject.Named;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Split data chunk from kinesis Stream to multiple small chunks for parallelization and distribution to multiple Presto workers.
 * By default, each shard of Kinesis Stream forms one Kinesis Split
 */
public class KinesisSplitManager
        implements ConnectorSplitManager
{
    /**
     * Max age of the shard cache (currently 24 hours).
     */
    public static final long MAX_CACHE_AGE_MILLIS = 24 * 3600 * 1000;

    private final String connectorId;
    private final KinesisHandleResolver handleResolver;
    private final KinesisClientProvider clientManager;

    private Map<String, InternalStreamDescription> streamMap = Collections.synchronizedMap(new HashMap<String, InternalStreamDescription>());

    /**
     * Cache the result of a Kinesis describe stream call so we don't need to retrieve
     * the shards for every single query.
     */
    public static class InternalStreamDescription
    {
        private String streamName = "";
        private List<Shard> shards = new ArrayList<>();
        private long createTimeStamp;

        public InternalStreamDescription(String aName)
        {
            this.streamName = aName;
            this.createTimeStamp = System.currentTimeMillis();
        }

        public long getCreateTimeStamp()
        {
            return this.createTimeStamp;
        }

        public String getStreamName()
        {
            return streamName;
        }

        public List<Shard> getShards()
        {
            return shards;
        }

        public void addShard(Shard s)
        {
            this.shards.add(s);
        }

        public void addAllShards(List<Shard> shards)
        {
            this.shards.addAll(shards);
        }
    }

    @Inject
    public KinesisSplitManager(@Named("connectorId") String connectorId,
            KinesisHandleResolver handleResolver,
            KinesisClientProvider clientManager)
    {
        this.connectorId = connectorId;
        this.handleResolver = handleResolver;
        this.clientManager = clientManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, ConnectorSplitManager.SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KinesisTableLayoutHandle kinesislayout = handleResolver.convertLayout(layout);
        KinesisTableHandle kinesisTableHandle = kinesislayout.getTable();

        InternalStreamDescription desc = this.getStreamDescription(kinesisTableHandle.getStreamName());

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (Shard shard : desc.getShards()) {
            KinesisSplit split = new KinesisSplit(connectorId,
                    kinesisTableHandle.getStreamName(),
                    kinesisTableHandle.getMessageDataFormat(),
                    shard.getShardId(),
                    shard.getSequenceNumberRange().getStartingSequenceNumber(),
                    shard.getSequenceNumberRange().getEndingSequenceNumber());
            builder.add(split);
        }

        return new FixedSplitSource(builder.build());
    }

    /**
     * Internal method to retrieve the stream description and get the shards from AWS.
     * <p>
     * Gets from the internal cache unless not yet created or too old.
     *
     * @param streamName
     * @return
     */
    protected InternalStreamDescription getStreamDescription(String streamName)
    {
        InternalStreamDescription desc = this.streamMap.get(streamName);
        if (desc == null || System.currentTimeMillis() - desc.getCreateTimeStamp() >= MAX_CACHE_AGE_MILLIS) {
            desc = new InternalStreamDescription(streamName);

            DescribeStreamRequest describeStreamRequest = clientManager.getDescribeStreamRequest();
            describeStreamRequest.setStreamName(streamName);

            // Collect shards from Kinesis
            String exclusiveStartShardId = null;
            List<Shard> shards = new ArrayList<>();
            do {
                describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
                DescribeStreamResult describeStreamResult = clientManager.getClient().describeStream(describeStreamRequest);

                String streamStatus = describeStreamResult.getStreamDescription().getStreamStatus();
                if (!streamStatus.equals("ACTIVE") && !streamStatus.equals("UPDATING")) {
                    throw new ResourceNotFoundException("Stream not Active");
                }

                desc.addAllShards(describeStreamResult.getStreamDescription().getShards());

                if (describeStreamResult.getStreamDescription().getHasMoreShards() && (shards.size() > 0)) {
                    exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
                }
                else {
                    exclusiveStartShardId = null;
                }
            }
            while (exclusiveStartShardId != null);

            this.streamMap.put(streamName, desc);
        }

        return desc;
    }
}
