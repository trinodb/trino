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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
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
        extends AmazonKinesisClient
{
    private List<InternalStream> streams = new ArrayList<>();

    public static class InternalShard
            extends Shard
    {
        private List<Record> recs = new ArrayList<>();
        private int index;

        public InternalShard(String streamName, int index)
        {
            super();
            this.index = index;
            this.setShardId(streamName + "_" + this.index);
        }

        public List<Record> getRecords()
        {
            return recs;
        }

        public List<Record> getRecordsFrom(ShardIterator iterator)
        {
            List<Record> returnRecords = new ArrayList<>();

            for (Record record : this.recs) {
                if (parseInt(record.getSequenceNumber()) >= iterator.recordIndex) {
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
                newShard.setSequenceNumberRange((new SequenceNumberRange()).withStartingSequenceNumber("100").withEndingSequenceNumber("999"));
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
            else {
                return new ArrayList<>();
            }
        }

        public PutRecordResult putRecord(ByteBuffer data, String partitionKey)
        {
            // Create record and insert into the shards.  Initially just do it
            // on a round robin basis.
            long timestamp = System.currentTimeMillis() - 50000;
            Record record = new Record();
            record = record.withData(data).withPartitionKey(partitionKey).withSequenceNumber(String.valueOf(sequenceNo));
            record.setApproximateArrivalTimestamp(new Date(timestamp));

            if (nextShard == shards.size()) {
                nextShard = 0;
            }
            InternalShard shard = shards.get(nextShard);
            shard.addRecord(record);

            PutRecordResult result = new PutRecordResult();
            result.setSequenceNumber(String.valueOf(sequenceNo));
            result.setShardId(shard.getShardId());

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

    public MockKinesisClient()
    {
        super();
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
            externalList.add(intshard);
        }

        return externalList;
    }

    protected List<Shard> getShards(InternalStream theStream, String fromShardId)
    {
        List<Shard> externalList = new ArrayList<>();
        for (InternalShard intshard : theStream.getShardsFrom(fromShardId)) {
            externalList.add(intshard);
        }

        return externalList;
    }

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest)
            throws AmazonClientException
    {
        // Setup method to add a new record:
        InternalStream theStream = this.getStream(putRecordRequest.getStreamName());
        if (theStream != null) {
            return theStream.putRecord(putRecordRequest.getData(), putRecordRequest.getPartitionKey());
        }
        else {
            throw new AmazonClientException("This stream does not exist!");
        }
    }

    @Override
    public CreateStreamResult createStream(CreateStreamRequest createStreamRequest)
            throws AmazonClientException
    {
        // Setup method to create a new stream:
        InternalStream stream = new InternalStream(createStreamRequest.getStreamName(), createStreamRequest.getShardCount(), true);
        this.streams.add(stream);
        return new CreateStreamResult();
    }

    @Override
    public CreateStreamResult createStream(String streamName, Integer integer)
            throws AmazonClientException
    {
        return this.createStream((new CreateStreamRequest()).withStreamName(streamName).withShardCount(integer));
    }

    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest)
            throws AmazonClientException
    {
        // Setup method to add a batch of new records:
        InternalStream theStream = this.getStream(putRecordsRequest.getStreamName());
        if (theStream != null) {
            PutRecordsResult result = new PutRecordsResult();
            List<PutRecordsResultEntry> resultList = new ArrayList<>();
            for (PutRecordsRequestEntry entry : putRecordsRequest.getRecords()) {
                PutRecordResult putResult = theStream.putRecord(entry.getData(), entry.getPartitionKey());
                resultList.add((new PutRecordsResultEntry()).withShardId(putResult.getShardId()).withSequenceNumber(putResult.getSequenceNumber()));
            }

            result.setRecords(resultList);
            return result;
        }
        else {
            throw new AmazonClientException("This stream does not exist!");
        }
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest)
            throws AmazonClientException
    {
        InternalStream theStream = this.getStream(describeStreamRequest.getStreamName());
        if (theStream == null) {
            throw new AmazonClientException("This stream does not exist!");
        }

        StreamDescription desc = new StreamDescription();
        desc = desc.withStreamName(theStream.getStreamName()).withStreamStatus(theStream.getStreamStatus()).withStreamARN(theStream.getStreamAmazonResourceName());

        if (describeStreamRequest.getExclusiveStartShardId() == null || describeStreamRequest.getExclusiveStartShardId().isEmpty()) {
            desc.setShards(this.getShards(theStream));
            desc.setHasMoreShards(false);
        }
        else {
            // Filter from given shard Id, or may not have any more
            String startId = describeStreamRequest.getExclusiveStartShardId();
            desc.setShards(this.getShards(theStream, startId));
            desc.setHasMoreShards(false);
        }

        DescribeStreamResult result = new DescribeStreamResult();
        result = result.withStreamDescription(desc);
        return result;
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest)
            throws AmazonClientException
    {
        ShardIterator iter = ShardIterator.fromStreamAndShard(getShardIteratorRequest.getStreamName(), getShardIteratorRequest.getShardId());
        if (iter == null) {
            throw new AmazonClientException("Bad stream or shard iterator!");
        }

        InternalStream theStream = this.getStream(iter.streamId);
        if (theStream == null) {
            throw new AmazonClientException("Unknown stream or bad shard iterator!");
        }

        String seqAsString = getShardIteratorRequest.getStartingSequenceNumber();
        if (seqAsString != null && !seqAsString.isEmpty() && getShardIteratorRequest.getShardIteratorType().equals("AFTER_SEQUENCE_NUMBER")) {
            int sequence = parseInt(seqAsString);
            iter.recordIndex = sequence + 1;
        }
        else {
            iter.recordIndex = 100;
        }

        GetShardIteratorResult result = new GetShardIteratorResult();
        return result.withShardIterator(iter.makeString());
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest)
            throws AmazonClientException
    {
        ShardIterator iterator = ShardIterator.fromString(getRecordsRequest.getShardIterator());
        if (iterator == null) {
            throw new AmazonClientException("Bad shard iterator.");
        }

        // TODO: incorporate maximum batch size (getRecordsRequest.getLimit)
        InternalStream stream = this.getStream(iterator.streamId);
        if (stream == null) {
            throw new AmazonClientException("Unknown stream or bad shard iterator.");
        }

        InternalShard shard = stream.getShards().get(iterator.shardIndex);

        GetRecordsResult result;
        if (iterator.recordIndex == 100) {
            result = new GetRecordsResult();
            List<Record> recs = shard.getRecords();
            result.setRecords(recs); // NOTE: getting all for now
            result.setNextShardIterator(getNextShardIterator(iterator, recs).makeString());
            result.setMillisBehindLatest(100L);
        }
        else {
            result = new GetRecordsResult();
            List<Record> recs = shard.getRecordsFrom(iterator);
            result.setRecords(recs); // may be empty
            result.setNextShardIterator(getNextShardIterator(iterator, recs).makeString());
            result.setMillisBehindLatest(100L);
        }

        return result;
    }

    protected ShardIterator getNextShardIterator(ShardIterator previousIter, List<Record> records)
    {
        if (records.size() == 0) {
            return previousIter;
        }

        Record rec = records.get(records.size() - 1);
        int lastSeq = Integer.valueOf(rec.getSequenceNumber());
        return new ShardIterator(previousIter.streamId, previousIter.shardIndex, lastSeq + 1);
    }

    //// Unsupported methods

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest)
            throws AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams()
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1)
            throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public PutRecordResult putRecord(String s, ByteBuffer byteBuffer, String s1, String s2)
            throws AmazonServiceException, AmazonClientException
    {
        throw new UnsupportedOperationException("MockKinesisClient doesn't support this.");
    }

    @Override
    public DescribeStreamResult describeStream(String streamName)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, Integer integer, String exclusiveStartShardId)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType, String startingSequenceNumber)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams(String exclusiveStartStreamName)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName)
            throws AmazonServiceException, AmazonClientException
    {
        return null;
    }

    @Override
    public void shutdown()
    {
        return; // Nothing to shutdown here
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest)
    {
        return null;
    }
}
