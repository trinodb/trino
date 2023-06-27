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

import io.trino.testing.ResourcePresence;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.CreateStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamRequest;
import software.amazon.awssdk.services.kinesis.model.ListTagsForStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Mock kinesis client for testing that is primarily used for reading from the
 * stream as we do here in Trino.
 * <p>
 * This is to help prove that the API is being used correctly and debug any
 * issues that arise without incurring AWS load and charges.  It is far from a complete
 * implementation of Kinesis.
 * <p>
 */
public class MockKinesisClient
        implements KinesisClient
{
    private final List<InternalStream> streams = new ArrayList<>();

    /*
    Shard can not be extended in the new AWS SDK v2 as it is declared final.
    Create our own class and convert to Shard whenever needed.
     */
    public static class InternalShard
    {
        private String shardId;
        private String parentShardId;
        private String adjacentParentShardId;
        private HashKeyRange hashKeyRange;
        private SequenceNumberRange sequenceNumberRange;
        private List<Record> recs = new ArrayList<>();
        private int index;

        public InternalShard(String streamName, int index)
        {
            this.index = index;
            this.setShardId(streamName + "_" + this.index);
        }

        public static Shard convertInternalShardToShard(InternalShard internalShard)
        {
            return Shard.builder()
                    .shardId(internalShard.getShardId())
                    .parentShardId(internalShard.getParentShardId())
                    .adjacentParentShardId(internalShard.getAdjacentParentShardId())
                    .hashKeyRange(internalShard.getHashKeyRange())
                    .sequenceNumberRange(internalShard.getSequenceNumberRange())
                    .build();
        }

        public void setShardId(String shardId)
        {
            this.shardId = shardId;
        }

        public String getShardId()
        {
            return this.shardId;
        }

        public InternalShard withShardId(String shardId)
        {
            this.setShardId(shardId);
            return this;
        }

        public void setParentShardId(String parentShardId)
        {
            this.parentShardId = parentShardId;
        }

        public String getParentShardId()
        {
            return this.parentShardId;
        }

        public InternalShard withParentShardId(String parentShardId)
        {
            this.setParentShardId(parentShardId);
            return this;
        }

        public void setAdjacentParentShardId(String adjacentParentShardId)
        {
            this.adjacentParentShardId = adjacentParentShardId;
        }

        public String getAdjacentParentShardId()
        {
            return this.adjacentParentShardId;
        }

        public InternalShard withAdjacentParentShardId(String adjacentParentShardId)
        {
            this.setAdjacentParentShardId(adjacentParentShardId);
            return this;
        }

        public void setHashKeyRange(HashKeyRange hashKeyRange)
        {
            this.hashKeyRange = hashKeyRange;
        }

        public HashKeyRange getHashKeyRange()
        {
            return this.hashKeyRange;
        }

        public InternalShard withHashKeyRange(HashKeyRange hashKeyRange)
        {
            this.setHashKeyRange(hashKeyRange);
            return this;
        }

        public void setSequenceNumberRange(SequenceNumberRange sequenceNumberRange)
        {
            this.sequenceNumberRange = sequenceNumberRange;
        }

        public SequenceNumberRange getSequenceNumberRange()
        {
            return this.sequenceNumberRange;
        }

        public InternalShard withSequenceNumberRange(SequenceNumberRange sequenceNumberRange)
        {
            this.setSequenceNumberRange(sequenceNumberRange);
            return this;
        }

        public List<Record> getRecords()
        {
            return recs;
        }

        public List<Record> getRecordsFrom(ShardIterator iterator)
        {
            List<Record> returnRecords = new ArrayList<>();

            for (Record record : this.recs) {
                if (parseInt(record.sequenceNumber()) >= iterator.recordIndex) {
                    returnRecords.add(record);
                }
            }

            return returnRecords;
        }

        public int getIndex()
        {
            return index;
        }

        public void addRecord(Record rec)
        {
            recs.add(rec);
        }

        public void clearRecords()
        {
            recs.clear();
        }
    }

    @Override
    public String serviceName()
    {
        return "MockKinesisClient";
    }

    public static class InternalStream
    {
        private final String streamName;
        private final String streamAmazonResourceName;
        private String streamStatus = "CREATING";
        private List<InternalShard> shards = new ArrayList<>();
        private int sequenceNo = 100;
        private int nextShard;

        public InternalStream(String streamName, int shardCount, boolean isActive)
        {
            this.streamName = streamName;
            this.streamAmazonResourceName = "local:fake.stream:" + streamName;
            if (isActive) {
                this.streamStatus = "ACTIVE";
            }

            for (int i = 0; i < shardCount; i++) {
                InternalShard newShard = new InternalShard(this.streamName, i);
                newShard.setSequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber("100").endingSequenceNumber("999").build());
                this.shards.add(newShard);
            }
        }

        public String getStreamName()
        {
            return streamName;
        }

        public String getStreamAmazonResourceName()
        {
            return streamAmazonResourceName;
        }

        public String getStreamStatus()
        {
            return streamStatus;
        }

        public List<InternalShard> getShards()
        {
            return shards;
        }

        public List<InternalShard> getShardsFrom(String afterShardId)
        {
            String[] comps = afterShardId.split("_");
            if (comps.length == 2) {
                List<InternalShard> returnArray = new ArrayList<>();
                int afterIndex = parseInt(comps[1]);
                if (shards.size() > afterIndex + 1) {
                    for (InternalShard shard : shards) {
                        if (shard.getIndex() > afterIndex) {
                            returnArray.add(shard);
                        }
                    }
                }

                return returnArray;
            }
            return new ArrayList<>();
        }

        public PutRecordResponse putRecord(SdkBytes data, String partitionKey)
        {
            // Create record and insert into the shards.  Initially just do it
            // on a round robin basis.
            long timestamp = System.currentTimeMillis() - 50000;
            Record record = Record.builder()
                    .data(data)
                    .partitionKey(partitionKey)
                    .sequenceNumber(String.valueOf(sequenceNo))
                    .approximateArrivalTimestamp(Instant.ofEpochMilli(timestamp))
                    .build();

            if (nextShard == shards.size()) {
                nextShard = 0;
            }
            InternalShard shard = shards.get(nextShard);
            shard.addRecord(record);

            PutRecordResponse result = PutRecordResponse.builder()
                    .sequenceNumber(String.valueOf(sequenceNo))
                    .shardId(shard.getShardId()).build();

            nextShard++;
            sequenceNo++;

            return result;
        }
    }

    public static class ShardIterator
    {
        public final String streamId;
        public final int shardIndex;
        public int recordIndex;

        public ShardIterator(String streamId, int shardIndex, int recordIndex)
        {
            this.streamId = streamId;
            this.shardIndex = shardIndex;
            this.recordIndex = recordIndex;
        }

        public String makeString()
        {
            return this.streamId + "_" + this.shardIndex + "_" + this.recordIndex;
        }

        public static ShardIterator fromStreamAndShard(String streamName, String shardId)
        {
            ShardIterator newInst = null;
            String[] comps = shardId.split("_");
            if (streamName.equals(comps[0]) && comps[1].matches("[0-9]+")) {
                newInst = new ShardIterator(comps[0], parseInt(comps[1]), 0);
            }

            return newInst;
        }

        public static ShardIterator fromString(String input)
        {
            ShardIterator newInst = null;
            String[] comps = input.split("_");
            if (comps.length == 3) {
                if (comps[1].matches("[0-9]+") && comps[2].matches("[0-9]+")) {
                    newInst = new ShardIterator(comps[0], parseInt(comps[1]), parseInt(comps[2]));
                }
            }

            return newInst;
        }
    }

    protected InternalStream getStream(String name)
    {
        InternalStream foundStream = null;
        for (InternalStream stream : this.streams) {
            if (stream.getStreamName().equals(name)) {
                foundStream = stream;
                break;
            }
        }
        return foundStream;
    }

    protected List<Shard> getShards(InternalStream theStream)
    {
        List<Shard> externalList = new ArrayList<>();
        for (InternalShard intshard : theStream.getShards()) {
            externalList.add(InternalShard.convertInternalShardToShard(intshard));
        }

        return externalList;
    }

    protected List<Shard> getShards(InternalStream theStream, String fromShardId)
    {
        List<Shard> externalList = new ArrayList<>();
        for (InternalShard intshard : theStream.getShardsFrom(fromShardId)) {
            externalList.add(InternalShard.convertInternalShardToShard(intshard));
        }

        return externalList;
    }

    @Override
    public PutRecordResponse putRecord(PutRecordRequest putRecordRequest)
            throws SdkException
    {
        // Setup method to add a new record:
        InternalStream theStream = this.getStream(putRecordRequest.streamName());
        if (theStream != null) {
            return theStream.putRecord(putRecordRequest.data(), putRecordRequest.partitionKey());
        }
        throw SdkException.create("This stream does not exist!", null);
    }

    @Override
    public CreateStreamResponse createStream(CreateStreamRequest createStreamRequest)
            throws SdkException
    {
        // Setup method to create a new stream:
        InternalStream stream = new InternalStream(createStreamRequest.streamName(), createStreamRequest.shardCount(), true);
        this.streams.add(stream);
        return CreateStreamResponse.builder().build();
    }

    public CreateStreamResponse createStream(String streamName, Integer integer)
            throws SdkException
    {
        return this.createStream(CreateStreamRequest.builder().streamName(streamName).shardCount(integer).build());
    }

    @Override
    public PutRecordsResponse putRecords(PutRecordsRequest putRecordsRequest)
            throws SdkException
    {
        // Setup method to add a batch of new records:
        InternalStream theStream = this.getStream(putRecordsRequest.streamName());
        if (theStream != null) {
            PutRecordsResponse.Builder result = PutRecordsResponse.builder();
            List<PutRecordsResultEntry> resultList = new ArrayList<>();
            for (PutRecordsRequestEntry entry : putRecordsRequest.records()) {
                PutRecordResponse putResult = theStream.putRecord(entry.data(), entry.partitionKey());
                resultList.add(PutRecordsResultEntry.builder().shardId(putResult.shardId()).sequenceNumber(putResult.sequenceNumber()).build());
            }

            result.records(resultList);
            return result.build();
        }
        throw SdkException.create("This stream does not exist!", null);
    }

    @Override
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest)
            throws SdkException
    {
        InternalStream theStream = this.getStream(describeStreamRequest.streamName());
        if (theStream == null) {
            throw SdkException.create("This stream does not exist!", null);
        }

        StreamDescription.Builder desc = StreamDescription.builder();
        desc = desc.streamName(theStream.getStreamName()).streamStatus(theStream.getStreamStatus()).streamARN(theStream.getStreamAmazonResourceName());

        if (describeStreamRequest.exclusiveStartShardId() == null || describeStreamRequest.exclusiveStartShardId().isEmpty()) {
            desc.shards(this.getShards(theStream));
            desc.hasMoreShards(false);
        }
        else {
            // Filter from given shard Id, or may not have any more
            String startId = describeStreamRequest.exclusiveStartShardId();
            desc.shards(this.getShards(theStream, startId));
            desc.hasMoreShards(false);
        }

        return DescribeStreamResponse.builder().streamDescription(desc.build()).build();
    }

    @Override
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest)
            throws SdkException
    {
        ShardIterator iter = ShardIterator.fromStreamAndShard(getShardIteratorRequest.streamName(), getShardIteratorRequest.shardId());
        if (iter == null) {
            throw SdkException.create("Bad stream or shard iterator!", null);
        }

        InternalStream theStream = this.getStream(iter.streamId);
        if (theStream == null) {
            throw SdkException.create("Unknown stream or bad shard iterator!", null);
        }

        String seqAsString = getShardIteratorRequest.startingSequenceNumber();
        if (seqAsString != null && !seqAsString.isEmpty() && getShardIteratorRequest.shardIteratorType().toString().equals("AFTER_SEQUENCE_NUMBER")) {
            int sequence = parseInt(seqAsString);
            iter.recordIndex = sequence + 1;
        }
        else {
            iter.recordIndex = 100;
        }

        return GetShardIteratorResponse.builder().shardIterator(iter.makeString()).build();
    }

    @Override
    public GetRecordsResponse getRecords(GetRecordsRequest getRecordsRequest)
            throws SdkException
    {
        ShardIterator iterator = ShardIterator.fromString(getRecordsRequest.shardIterator());
        if (iterator == null) {
            throw SdkException.create("Bad shard iterator.", null);
        }

        // TODO: incorporate maximum batch size (getRecordsRequest.getLimit)
        InternalStream stream = this.getStream(iterator.streamId);
        if (stream == null) {
            throw SdkException.create("Unknown stream or bad shard iterator.", null);
        }

        InternalShard shard = stream.getShards().get(iterator.shardIndex);

        GetRecordsResponse.Builder result = GetRecordsResponse.builder();
        if (iterator.recordIndex == 100) {
            List<Record> recs = shard.getRecords();
            result.records(recs); // NOTE: getting all for now
            result.nextShardIterator(getNextShardIterator(iterator, recs).makeString());
            result.millisBehindLatest(100L);
        }
        else {
            List<Record> recs = shard.getRecordsFrom(iterator);
            result.records(recs); // may be empty
            result.nextShardIterator(getNextShardIterator(iterator, recs).makeString());
            result.millisBehindLatest(100L);
        }

        return result.build();
    }

    protected ShardIterator getNextShardIterator(ShardIterator previousIter, List<Record> records)
    {
        if (records.isEmpty()) {
            return previousIter;
        }

        Record rec = records.getLast();
        int lastSeq = Integer.parseInt(rec.sequenceNumber());
        return new ShardIterator(previousIter.streamId, previousIter.shardIndex, lastSeq + 1);
    }

    //// Unsupported methods

    @Override
    public ListTagsForStreamResponse listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest)
            throws SdkException
    {
        return null;
    }

    @Override
    public ListStreamsResponse listStreams(ListStreamsRequest listStreamsRequest)
            throws SdkException
    {
        return null;
    }

    @Override
    public ListStreamsResponse listStreams()
            throws SdkException
    {
        return null;
    }

    @Override
    public void close()
    {
        streams.clear();
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return !streams.isEmpty();
    }
}
