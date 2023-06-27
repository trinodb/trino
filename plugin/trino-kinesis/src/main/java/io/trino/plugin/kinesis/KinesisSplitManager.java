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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Split data chunk from kinesis Stream to multiple small chunks for parallelization and distribution to multiple Trino workers.
 * By default, each shard of Kinesis Stream forms one Kinesis Split
 */
public class KinesisSplitManager
        implements ConnectorSplitManager
{
    public static final long MAX_CACHE_AGE_MILLIS = new Duration(1, DAYS).toMillis();

    private final KinesisClientProvider clientManager;

    private final Map<String, InternalStreamDescription> streamMap = Collections.synchronizedMap(new HashMap<>());

    /**
     * Cache the result of a Kinesis describe stream call so we don't need to retrieve
     * the shards for every single query.
     */
    public static class InternalStreamDescription
    {
        private final String streamName;
        private final List<Shard> shards = new ArrayList<>();
        private final long createTimeStamp;

        public InternalStreamDescription(String streamName)
        {
            this.streamName = requireNonNull(streamName);
            this.createTimeStamp = System.currentTimeMillis();
        }

        public long getCreateTimeStamp()
        {
            return createTimeStamp;
        }

        public String getStreamName()
        {
            return streamName;
        }

        public List<Shard> getShards()
        {
            return shards;
        }

        public void addAllShards(List<Shard> shards)
        {
            this.shards.addAll(shards);
        }
    }

    @Inject
    public KinesisSplitManager(KinesisClientProvider clientManager)
    {
        this.clientManager = clientManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        KinesisTableHandle kinesisTableHandle = (KinesisTableHandle) table;

        InternalStreamDescription description = this.getStreamDescription(kinesisTableHandle.getStreamName());

        ImmutableList.Builder<ConnectorSplit> builder = ImmutableList.builder();
        for (Shard shard : description.getShards()) {
            KinesisSplit split = new KinesisSplit(
                    kinesisTableHandle.getStreamName(),
                    kinesisTableHandle.getMessageDataFormat(),
                    kinesisTableHandle.getCompressionCodec(),
                    shard.shardId(),
                    shard.sequenceNumberRange().startingSequenceNumber(),
                    shard.sequenceNumberRange().endingSequenceNumber());
            builder.add(split);
        }

        return new FixedSplitSource(builder.build());
    }

    /**
     * Internal method to retrieve the stream description and get the shards from AWS.
     * <p>
     * Gets from the internal cache unless not yet created or too old.
     */
    protected InternalStreamDescription getStreamDescription(String streamName)
    {
        InternalStreamDescription internalStreamDescription = this.streamMap.get(streamName);
        if (internalStreamDescription == null || System.currentTimeMillis() - internalStreamDescription.getCreateTimeStamp() >= MAX_CACHE_AGE_MILLIS) {
            internalStreamDescription = new InternalStreamDescription(streamName);

            DescribeStreamRequest.Builder describeStreamRequest = DescribeStreamRequest.builder()
                    .streamName(streamName);

            // Collect shards from Kinesis
            String exclusiveStartShardId = null;

            do {
                describeStreamRequest.exclusiveStartShardId(exclusiveStartShardId);
                DescribeStreamResponse describeStreamResult = clientManager.getClient().describeStream(describeStreamRequest.build());

                String streamStatus = describeStreamResult.streamDescription().streamStatusAsString();
                if (!streamStatus.equals("ACTIVE") && !streamStatus.equals("UPDATING")) {
                    throw ResourceNotFoundException.builder().message("Stream not Active").build();
                }

                internalStreamDescription.addAllShards(describeStreamResult.streamDescription().shards());
                int shardSize = internalStreamDescription.getShards().size();
                if (describeStreamResult.streamDescription().hasMoreShards() && (shardSize > 0)) {
                    exclusiveStartShardId = internalStreamDescription.getShards().get(shardSize - 1).shardId();
                }
                else {
                    exclusiveStartShardId = null;
                }
            }
            while (exclusiveStartShardId != null);

            this.streamMap.put(streamName, internalStreamDescription);
        }

        return internalStreamDescription;
    }
}
