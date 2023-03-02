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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * Kinesis vertion of ConnectorSplit. KinesisConnector fetch the data from kinesis stream and splits the big chunk to multiple split.
 * By default, one shard data is one KinesisSplit.
 */
public class KinesisSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(KinesisSplit.class);

    private final String streamName;
    private final String messageDataFormat;
    private final KinesisCompressionCodec compressionCodec;
    private final String shardId;
    private final String start;
    private final String end;

    @JsonCreator
    public KinesisSplit(
            @JsonProperty("streamName") String streamName,
            @JsonProperty("messageDataFormat") String messageDataFormat,
            @JsonProperty("compressionCodec") KinesisCompressionCodec compressionCodec,
            @JsonProperty("shardId") String shardId,
            @JsonProperty("start") String start,
            @JsonProperty("end") String end)
    {
        this.streamName = requireNonNull(streamName, "streamName is null");
        this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
        this.shardId = shardId;
        this.start = start;
        this.end = end;
    }

    @JsonProperty
    public String getStart()
    {
        return start;
    }

    @JsonProperty
    public String getEnd()
    {
        return end;
    }

    @JsonProperty
    public String getStreamName()
    {
        return streamName;
    }

    @JsonProperty
    public String getMessageDataFormat()
    {
        return messageDataFormat;
    }

    @JsonProperty
    public KinesisCompressionCodec getCompressionCodec()
    {
        return compressionCodec;
    }

    @JsonProperty
    public String getShardId()
    {
        return shardId;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(streamName)
                + estimatedSizeOf(messageDataFormat)
                + estimatedSizeOf(shardId)
                + estimatedSizeOf(start)
                + estimatedSizeOf(end);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", streamName)
                .add("messageDataFormat", messageDataFormat)
                .add("compressionCodec", compressionCodec)
                .add("shardId", shardId)
                .add("start", start)
                .add("end", end)
                .toString();
    }
}
