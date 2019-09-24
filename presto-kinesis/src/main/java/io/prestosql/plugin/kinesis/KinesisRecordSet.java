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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getBatchSize;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getCheckpointLogicalName;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getIterationNumber;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getIteratorOffsetSeconds;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getIteratorStartTimestamp;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.getMaxBatches;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.isCheckpointEnabled;
import static io.prestosql.plugin.kinesis.KinesisSessionProperties.isIteratorFromTimestamp;
import static java.util.Objects.requireNonNull;

public class KinesisRecordSet
        implements RecordSet
{
    /**
     * Indicates how close to current we want to be before stopping the fetch of records in a query.
     */
    private static final int MILLIS_BEHIND_LIMIT = 10000;

    private static final Logger log = Logger.get(KinesisRecordSet.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final KinesisSplit split;
    private final ConnectorSession session;
    private final KinesisClientProvider clientManager;

    private final RowDecoder messageDecoder;

    private final List<KinesisColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final int batchSize;
    private final int maxBatches;
    private final int fetchAttempts;
    private final long sleepTime;

    private final boolean isLogBatches;

    // for checkpointing
    private final boolean checkpointEnabled;
    private final KinesisShardCheckpointer shardCheckpointer;
    private String lastReadSequenceNumber;

    KinesisRecordSet(
            KinesisSplit split,
            ConnectorSession session,
            KinesisClientProvider clientManager,
            List<KinesisColumnHandle> columnHandles,
            RowDecoder messageDecoder,
            KinesisConfig kinesisConfig)
    {
        this.split = requireNonNull(split, "split is null");
        this.session = requireNonNull(session, "session is null");
        requireNonNull(kinesisConfig, "kinesisConfig is null");
        long dynamoReadCapacity = kinesisConfig.getDynamoReadCapacity();
        long dynamoWriteCapacity = kinesisConfig.getDynamoWriteCapacity();
        long checkPointIntervalMillis = kinesisConfig.getCheckpointInterval().toMillis();
        this.isLogBatches = kinesisConfig.isLogBatches();

        this.clientManager = requireNonNull(clientManager, "clientManager is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.messageDecoder = messageDecoder;

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (KinesisColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();

        // Note: these default to what is in the configuration if not given in the session
        this.batchSize = getBatchSize(session);
        this.maxBatches = getMaxBatches(this.session);

        this.fetchAttempts = kinesisConfig.getFetchAttempts();
        this.sleepTime = kinesisConfig.getSleepTime().toMillis();

        this.checkpointEnabled = isCheckpointEnabled(session);
        this.lastReadSequenceNumber = null;

        // Initialize checkpoint related code
        if (checkpointEnabled) {
            AmazonDynamoDBClient dynamoDBClient = clientManager.getDynamoDbClient();
            String dynamoDBTable = split.getStreamName();
            int curIterationNumber = getIterationNumber(session);
            String sessionLogicalName = getCheckpointLogicalName(session);

            String logicalProcessName = null;
            if (sessionLogicalName != null) {
                logicalProcessName = sessionLogicalName;
            }

            shardCheckpointer = new KinesisShardCheckpointer(
                    dynamoDBClient,
                    dynamoDBTable,
                    split,
                    logicalProcessName,
                    curIterationNumber,
                    checkPointIntervalMillis,
                    dynamoReadCapacity,
                    dynamoWriteCapacity);

            lastReadSequenceNumber = shardCheckpointer.getLastReadSeqNumber();
        }
        else {
            shardCheckpointer = null;
        }
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KinesisRecordCursor();
    }

    public class KinesisRecordCursor
            implements RecordCursor
    {
        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];
        // TODO: total bytes here only includes records we iterate through, not total read from Kinesis.
        // This may not be an issue, but if total vs. completed is an important signal to Presto then
        // the implementation below could be a problem.  Need to investigate.
        private long batchesRead;
        private long messagesRead;
        private long totalBytes;
        private long totalMessages;
        private long lastReadTime;
        private String shardIterator;
        private List<Record> kinesisRecords;
        private Iterator<Record> listIterator;
        private GetRecordsRequest getRecordsRequest;
        private GetRecordsResult getRecordsResult;

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        /**
         * Advances the cursor by one position, retrieving more records from Kinesis if needed.
         * <p>
         * We retrieve records from Kinesis in batches, using the getRecordsRequest.  After a
         * getRecordsRequest we keep iterating through that list of records until we run out.  Then
         * we will get another batch unless we've hit the limit or have caught up.
         */
        @Override
        public boolean advanceNextPosition()
        {
            if (shardIterator == null && getRecordsRequest == null) {
                getIterator(); // first shard iterator
                log.debug("Starting read.  Retrieved first shard iterator from AWS Kinesis.");
            }

            if (getRecordsRequest == null || (!listIterator.hasNext() && shouldGetMoreRecords())) {
                getKinesisRecords();
            }

            if (listIterator.hasNext()) {
                return nextRow();
            }
            else {
                log.debug("Read all of the records from the shard:  %d batches and %d messages and %d total bytes.", batchesRead, totalMessages, totalBytes);
                return false;
            }
        }

        private boolean shouldGetMoreRecords()
        {
            return shardIterator != null && batchesRead < maxBatches && getMillisBehindLatest() > MILLIS_BEHIND_LIMIT;
        }

        /**
         * Retrieves the next batch of records from Kinesis using the shard iterator.
         * <p>
         * Most of the time this results in one getRecords call.  However we allow for
         * a call to return an empty list, and we'll try again if we are far enough
         * away from the latest record.
         */
        private void getKinesisRecords()
                throws ResourceNotFoundException
        {
            // Normally this loop will execute once, but we have to allow for the odd Kinesis
            // behavior, per the docs:
            // A single call to getRecords might return an empty record list, even when the shard contains
            // more records at later sequence numbers
            boolean fetchedRecords = false;
            int attempts = 0;
            while (!fetchedRecords && attempts < fetchAttempts) {
                Duration duration = nanosSince(lastReadTime);
                if (duration.toMillis() <= sleepTime) {
                    try {
                        Thread.sleep(duration.toMillis());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("thread interrupted");
                    }
                }
                getRecordsRequest = new GetRecordsRequest();
                getRecordsRequest.setShardIterator(shardIterator);
                getRecordsRequest.setLimit(batchSize);

                getRecordsResult = clientManager.getClient().getRecords(getRecordsRequest);
                lastReadTime = System.nanoTime();

                shardIterator = getRecordsResult.getNextShardIterator();
                kinesisRecords = getRecordsResult.getRecords();
                if (isLogBatches) {
                    log.info("Fetched %d records from Kinesis.  MillisBehindLatest=%d", kinesisRecords.size(), getRecordsResult.getMillisBehindLatest());
                }

                fetchedRecords = (kinesisRecords.size() > 0 || getMillisBehindLatest() <= MILLIS_BEHIND_LIMIT);
                attempts++;
            }

            listIterator = kinesisRecords.iterator();
            batchesRead++;
            messagesRead += kinesisRecords.size();
        }

        private boolean nextRow()
        {
            Record currentRecord = listIterator.next();
            String partitionKey = currentRecord.getPartitionKey();
            log.debug("Reading record with partition key %s", partitionKey);

            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer message = currentRecord.getData();
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }
            totalBytes += messageData.length;
            totalMessages++;

            log.debug("Fetching %d bytes from current record. %d messages read so far", messageData.length, totalMessages);

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData, null);

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();
            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    KinesisInternalFieldDescription fieldDescription = KinesisInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case SHARD_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(split.getShardId().getBytes()));
                            break;
                        case SEGMENT_START_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(split.getStart().getBytes()));
                            break;
                        case SEGMENT_COUNT_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(totalMessages));
                            break;
                        case SHARD_SEQUENCE_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(currentRecord.getSequenceNumber().getBytes()));
                            break;
                        case MESSAGE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                            break;
                        case MESSAGE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                            break;
                        case MESSAGE_VALID_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }

            decodedValue.ifPresent(currentRowValuesMap::putAll);
            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true;
        }

        /**
         * Protect against possibly null values if this isn't set (not expected)
         */
        private long getMillisBehindLatest()
        {
            if (getRecordsResult != null && getRecordsResult.getMillisBehindLatest() != null) {
                return getRecordsResult.getMillisBehindLatest();
            }
            return MILLIS_BEHIND_LIMIT + 1;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        @Override
        public void close()
        {
            log.info("Closing cursor - read complete.  Total read: %d batches %d messages, processed: %d messages and %d bytes.",
                    batchesRead, messagesRead, totalMessages, totalBytes);
            if (checkpointEnabled && lastReadSequenceNumber != null) {
                shardCheckpointer.checkpoint(lastReadSequenceNumber);
            }
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        private void getIterator()
                throws ResourceNotFoundException
        {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
            getShardIteratorRequest.setStreamName(split.getStreamName());
            getShardIteratorRequest.setShardId(split.getShardId());

            // Explanation: when we have a sequence number from a prior read or checkpoint, always use it.
            // Otherwise, decide if starting at a timestamp or the trim horizon based on configuration.
            // If starting at a timestamp, use the session variable STARTING_TIMESTAMP when given, otherwise
            // fallback on starting at STARTING_OFFSET_SECONDS from timestamp.
            if (lastReadSequenceNumber == null) {
                if (isIteratorFromTimestamp(session)) {
                    getShardIteratorRequest.setShardIteratorType("AT_TIMESTAMP");
                    long iteratorStartTimestamp = getIteratorStartTimestamp(session);
                    if (iteratorStartTimestamp == 0) {
                        long startTimestamp = System.currentTimeMillis() - (getIteratorOffsetSeconds(session) * 1000);
                        getShardIteratorRequest.setTimestamp(new Date(startTimestamp));
                    }
                    else {
                        getShardIteratorRequest.setTimestamp(new Date(iteratorStartTimestamp));
                    }
                }
                else {
                    getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");
                }
            }
            else {
                getShardIteratorRequest.setShardIteratorType("AFTER_SEQUENCE_NUMBER");
                getShardIteratorRequest.setStartingSequenceNumber(lastReadSequenceNumber);
            }

            GetShardIteratorResult getShardIteratorResult = clientManager.getClient().getShardIterator(getShardIteratorRequest);
            shardIterator = getShardIteratorResult.getShardIterator();
        }
    }
}
