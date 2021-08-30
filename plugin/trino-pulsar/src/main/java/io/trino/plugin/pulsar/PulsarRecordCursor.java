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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.plugin.pulsar.util.CacheSizeAllocator;
import io.trino.plugin.pulsar.util.NoStrictCacheSizeAllocator;
import io.trino.plugin.pulsar.util.NullCacheSizeAllocator;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.shade.io.netty.buffer.ByteBuf;
import org.apache.pulsar.shade.io.netty.util.IllegalReferenceCountException;
import org.apache.pulsar.shade.org.apache.pulsar.common.api.raw.MessageParser;
import org.apache.pulsar.shade.org.apache.pulsar.common.api.raw.RawMessage;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.shade.org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.shade.org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.pulsar.common.schema.SchemaType;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.SpscArrayQueue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.decoder.FieldValueProviders.bytesValueProvider;
import static io.trino.decoder.FieldValueProviders.longValueProvider;
import static io.trino.plugin.pulsar.PulsarErrorCode.PULSAR_MANAGEDLEDGER_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * A cursor to read records from Pulsar topic to serve Trino query.
 */
public class PulsarRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(PulsarRecordCursor.class);

    private List<PulsarColumnHandle> columnHandles;
    private PulsarSplit pulsarSplit;
    private PulsarConnectorConfig pulsarConnectorConfig;
    private PulsarReadOnlyCursor cursor;
    private SpscArrayQueue<RawMessage> messageQueue;
    private CacheSizeAllocator messageQueueCacheSizeAllocator;
    private SpscArrayQueue<Entry> entryQueue;
    private CacheSizeAllocator entryQueueCacheSizeAllocator;
    private RawMessage currentMessage;
    private int maxBatchSize;
    private long completedBytes;
    private ReadEntries readEntries;
    private DeserializeEntries deserializeEntries;
    private TopicName topicName;
    private PulsarConnectorMetricsTracker metricsTracker;
    private boolean readOffloaded;

    // Stats total execution time of split
    private long startTime;

    // Used to make sure we don't finish before all entries are processed since entries that have been dequeued
    // but not been deserialized and added messages to the message queue can be missed if we just check if the queues
    // are empty or not
    private final long splitSize;
    private long entriesProcessed;
    private int partition = -1;

    private SchemaInfoProvider schemaInfoProvider;

    private FieldValueProvider[] currentRowValues;

    PulsarDispatchingRowDecoderFactory decoderFactory;

    public PulsarRecordCursor(
            List<PulsarColumnHandle> columnHandles,
            PulsarSplit pulsarSplit,
            PulsarConnectorConfig pulsarConnectorConfig,
            PulsarDispatchingRowDecoderFactory decoderFactory,
            PulsarConnectorCache pulsarConnectorManagedLedgerFactory)
    {
        splitSize = pulsarSplit.getSplitSize();
        // Set start time for split
        startTime = System.nanoTime();
        OffloadPoliciesImpl offloadPolicies = pulsarSplit.getOffloadPoliciesOriginType();
        if (offloadPolicies != null) {
            offloadPolicies.setOffloadersDirectory(pulsarConnectorConfig.getOffloadersDirectory());
            offloadPolicies.setManagedLedgerOffloadMaxThreads(
                    pulsarConnectorConfig.getManagedLedgerOffloadMaxThreads());
        }
        initialize(columnHandles, pulsarSplit, pulsarConnectorConfig,
                pulsarConnectorManagedLedgerFactory.getManagedLedgerFactory(),
                pulsarConnectorManagedLedgerFactory.getManagedLedgerConfig(
                        TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                                pulsarSplit.getTableName()).getNamespaceObject(), offloadPolicies,
                        pulsarConnectorConfig),
                new PulsarConnectorMetricsTracker());
        this.decoderFactory = decoderFactory;
        initEntryCacheSizeAllocator(pulsarConnectorConfig);
    }

    private void initialize(
            List<PulsarColumnHandle> columnHandles,
            PulsarSplit pulsarSplit,
            PulsarConnectorConfig pulsarConnectorConfig,
            ManagedLedgerFactory managedLedgerFactory,
            ManagedLedgerConfig managedLedgerConfig,
            PulsarConnectorMetricsTracker pulsarConnectorMetricsTracker)
    {
        this.columnHandles = columnHandles;
        currentRowValues = new FieldValueProvider[columnHandles.size()];
        this.pulsarSplit = pulsarSplit;
        partition = TopicName.getPartitionIndex(pulsarSplit.getTableName());
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        maxBatchSize = pulsarConnectorConfig.getMaxEntryReadBatchSize();
        messageQueue = new SpscArrayQueue<>(pulsarConnectorConfig.getMaxSplitMessageQueueSize());
        entryQueue = new SpscArrayQueue<>(pulsarConnectorConfig.getMaxSplitEntryQueueSize());
        topicName = TopicName.get("persistent",
                NamespaceName.get(pulsarSplit.getSchemaName()),
                pulsarSplit.getTableName());
        this.metricsTracker = pulsarConnectorMetricsTracker;
        readOffloaded = pulsarConnectorConfig.getManagedLedgerOffloadDriver() != null;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
        initEntryCacheSizeAllocator(pulsarConnectorConfig);
        schemaInfoProvider = new PulsarSchemaInfoProvider(this.topicName, this.pulsarConnectorConfig);

        if (log.isDebugEnabled()) {
            log.debug("Initializing split with parameters: %s", pulsarSplit);
        }

        try {
            cursor = getCursor(TopicName.get("persistent", NamespaceName.get(pulsarSplit.getSchemaName()),
                    pulsarSplit.getTableName()), pulsarSplit.getStartPosition(), managedLedgerFactory, managedLedgerConfig);
        }
        catch (ManagedLedgerException | InterruptedException e) {
            close();
            throw new TrinoException(PULSAR_MANAGEDLEDGER_ERROR, "fail to open read only cursor", e);
        }
    }

    private PulsarReadOnlyCursor getCursor(
            TopicName topicName,
            Position startPosition,
            ManagedLedgerFactory managedLedgerFactory,
            ManagedLedgerConfig managedLedgerConfig) throws ManagedLedgerException, InterruptedException
    {
        PulsarReadOnlyCursor cursor = new PulsarReadOnlyCursorWrapper(managedLedgerFactory.openReadOnlyCursor(topicName.getPersistenceNamingEncoding(),
                startPosition, managedLedgerConfig));

        return cursor;
    }

    @Override
    public long getCompletedBytes()
    {
        return this.completedBytes;
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

    @VisibleForTesting
    public void setPulsarSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider)
    {
        this.schemaInfoProvider = schemaInfoProvider;
    }

    @VisibleForTesting
    class DeserializeEntries
            implements Runnable
    {
        protected volatile boolean isRunning;

        private final Thread thread;

        public DeserializeEntries()
        {
            this.thread = new Thread(this, "derserialize-thread-split-" + pulsarSplit.getSplitId());
        }

        public void interrupt()
        {
            isRunning = false;
            thread.interrupt();
        }

        public void start()
        {
            this.thread.start();
        }

        @Override
        public void run()
        {
            isRunning = true;
            while (isRunning) {
                int read = entryQueue.drain(new MessagePassingQueue.Consumer<Entry>()
                {
                    @Override
                    public void accept(Entry entry)
                    {
                        try {
                            entryQueueCacheSizeAllocator.release(entry.getLength());
                            LongAdder messageDeserialized = new LongAdder();

                            long bytes = entry.getDataBuffer().readableBytes();
                            completedBytes += bytes;
                            // register stats for bytes read
                            metricsTracker.recordByteRead(bytes);

                            // check if we have processed all entries in this split
                            if (((PositionImpl) entry.getPosition()).compareTo(pulsarSplit.getEndPosition()) >= 0) {
                                return;
                            }

                            // set start time for time deserializing entries for stats
                            long entryDeserializeStart = System.nanoTime();

                            try {
                                MessageParser.parseMessage(TopicName.get(topicName.getDomain().toString(),
                                                topicName.getNamespace(), topicName.getTenant(), topicName.getLocalName()),
                                        entry.getLedgerId(), entry.getEntryId(),
                                        entry.getDataBuffer(), (message) -> {
                                        try {
                                            // start time for message queue read
                                            long msgQueueEnqueueStart = System.nanoTime();

                                            while (true) {
                                                if (!haveAvailableCacheSize(messageQueueCacheSizeAllocator, messageQueue) || !messageQueue.offer(message)) {
                                                    Thread.sleep(1);
                                                }
                                                else {
                                                    messageQueueCacheSizeAllocator.allocate(message.getData().readableBytes());
                                                    break;
                                                }
                                            }

                                            // stats for how long a read from message queue took
                                            metricsTracker.recordMessageQueueEnqueueWaitTime(Duration.nanosSince(msgQueueEnqueueStart));
                                            // stats for number of messages read
                                            metricsTracker.incrNumberOfMessagesDeserialized();
                                            messageDeserialized.increment();
                                        }
                                        catch (InterruptedException e) {
                                            //no-op
                                        }
                                    }, pulsarConnectorConfig.getMaxMessageSize());
                            }
                            catch (IOException e) {
                                throw new RuntimeException(format("fail to parse message from pulsar topic %s", topicName.toString()), e);
                            }
                            // stats for time spend deserializing entries
                            metricsTracker.recordEntryDeserializeTime(Duration.nanosSince(entryDeserializeStart));

                            // stats for num messages per entry
                            metricsTracker.recordNumberOfMessageDeserializedPerEntry(messageDeserialized.longValue());
                        }
                        finally {
                            entriesProcessed++;
                            entry.release();
                        }
                    }
                });

                if (read <= 0) {
                    try {
                        Thread.sleep(1);
                    }
                    catch (InterruptedException e) {
                        return;
                    }
                }
            }
        }
    }

    @VisibleForTesting
    class ReadEntries
            implements AsyncCallbacks.ReadEntriesCallback
    {
        // indicate whether there are any additional entries left to read
        private volatile boolean isDone;

        //num of outstanding read requests
        // set to 1 because we can only read one batch a time
        private final AtomicLong outstandingReadsRequests = new AtomicLong(1);

        public void run()
        {
            if (outstandingReadsRequests.get() > 0) {
                if (!cursor.hasMoreEntries() || ((PositionImpl) cursor.getReadPosition())
                        .compareTo(pulsarSplit.getEndPosition()) >= 0) {
                    isDone = true;
                }
                else {
                    int batchSize = Math.min(maxBatchSize, entryQueue.capacity() - entryQueue.size());

                    if (batchSize > 0) {
                        // check if ledger is offloaded
                        if (!readOffloaded && cursor.getCurrentLedgerInfo().hasOffloadContext()) {
                            log.info(
                                    "Ledger %s is offloaded for topic %s. Ignoring it because offloader is not configured",
                                    cursor.getCurrentLedgerInfo().getLedgerId(), pulsarSplit.getTableName());

                            long numEntries = cursor.getCurrentLedgerInfo().getEntries();
                            long entriesToSkip =
                                    (numEntries - ((PositionImpl) cursor.getReadPosition()).getEntryId()) + 1;
                            cursor.skipEntries(toIntExact((entriesToSkip)));

                            entriesProcessed += entriesToSkip;
                        }
                        else {
                            if (!haveAvailableCacheSize(entryQueueCacheSizeAllocator, entryQueue)) {
                                metricsTracker.recordReadFailure();
                                return;
                            }
                            // if the available size is invalid and the entry queue size is 0, read one entry
                            outstandingReadsRequests.decrementAndGet();
                            cursor.asyncReadEntries(batchSize, entryQueueCacheSizeAllocator.getAvailableCacheSize(),
                                    this, System.nanoTime(), PositionImpl.latest);
                        }

                        // stats for successful read request
                        metricsTracker.recordReadSuccess();
                    }
                    else {
                        // stats for failed read request because entry queue is full
                        metricsTracker.recordReadFailure();
                    }
                }
            }
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx)
        {
            entryQueue.fill(new MessagePassingQueue.Supplier<Entry>()
            {
                private int i;

                @Override
                public Entry get()
                {
                    Entry entry = entries.get(i);
                    i++;
                    entryQueueCacheSizeAllocator.allocate(entry.getLength());
                    return entry;
                }
            }, entries.size());

            outstandingReadsRequests.incrementAndGet();

            //set read latency stats for success
            metricsTracker.recordReadLatency(Duration.nanosSince((long) ctx));
            //stats for number of entries read
            metricsTracker.incrNumberOfEntriesPerBatch(entries.size());
        }

        public boolean hasFinished()
        {
            return messageQueue.isEmpty() && isDone && outstandingReadsRequests.get() >= 1 && splitSize <= entriesProcessed;
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx)
        {
            if (log.isDebugEnabled()) {
                log.debug(exception, "fail to read entries from topic %s", topicName.toString());
            }
            outstandingReadsRequests.incrementAndGet();

            //set read latency stats for failed
            metricsTracker.recordReadLatency(Duration.nanosSince((long) ctx));
        }
    }

    /**
     * Check the queue has available cache size quota or not.
     * 1. If the CacheSizeAllocator is NullCacheSizeAllocator, return true.
     * 2. If the available cache size > 0, return true.
     * 3. If the available cache size is invalid and the queue size == 0, return true, ensure not block the query.
     */
    private boolean haveAvailableCacheSize(CacheSizeAllocator cacheSizeAllocator, SpscArrayQueue queue)
    {
        if (cacheSizeAllocator instanceof NullCacheSizeAllocator) {
            return true;
        }
        return cacheSizeAllocator.getAvailableCacheSize() > 0 || queue.size() == 0;
    }

    public FieldValueProvider[] getCurrentRowValues()
    {
        return currentRowValues;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (readEntries == null) {
            // start deserialize thread
            deserializeEntries = new DeserializeEntries();
            deserializeEntries.start();

            readEntries = new ReadEntries();
            readEntries.run();
        }

        while (true) {
            if (readEntries.hasFinished()) {
                return false;
            }

            if ((messageQueue.capacity() - messageQueue.size()) > 0) {
                readEntries.run();
            }

            currentMessage = messageQueue.poll();
            if (currentMessage != null) {
                messageQueueCacheSizeAllocator.release(currentMessage.getData().readableBytes());
                break;
            }
            else {
                try {
                    Thread.sleep(1);
                    // stats for time spent wait to read from message queue because its empty
                    metricsTracker.recordMessageQueueDequeueWaitTime();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //start time for deseralizing record
        long recordDeserializeStart = System.nanoTime();

        SchemaInfo schemaInfo = getBytesSchemaInfo(pulsarSplit.getSchemaType(), pulsarSplit.getSchemaName());
        try {
            if (schemaInfo == null) {
                schemaInfo = schemaInfoProvider.getSchemaByVersion(currentMessage.getSchemaVersion()).get();
            }
        }
        catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

        if (schemaInfo.getType().equals(SchemaType.KEY_VALUE)) {
            ByteBuf keyByteBuf;
            ByteBuf valueByteBuf;

            KeyValueEncodingType keyValueEncodingType = KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
            if (Objects.equals(keyValueEncodingType, KeyValueEncodingType.INLINE)) {
                ByteBuf dataPayload = currentMessage.getData();
                int keyLength = dataPayload.readInt();
                keyByteBuf = dataPayload.readSlice(keyLength);
                int valueLength = dataPayload.readInt();
                valueByteBuf = dataPayload.readSlice(valueLength);
            }
            else {
                keyByteBuf = currentMessage.getKeyBytes().get();
                valueByteBuf = currentMessage.getData();
            }

            KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
            Set<DecoderColumnHandle> keyColumnHandles = columnHandles.stream()
                    .filter(col -> !col.isInternal())
                    .filter(col -> PulsarColumnHandle.HandleKeyValueType.KEY
                            .equals(col.getHandleKeyValueType()))
                    .collect(toImmutableSet());
            PulsarRowDecoder keyDecoder = null;
            if (keyColumnHandles.size() > 0) {
                keyDecoder = decoderFactory.createRowDecoder(topicName,
                        kvSchemaInfo.getKey(), keyColumnHandles);
            }

            Set<DecoderColumnHandle> valueColumnHandles = columnHandles.stream()
                    .filter(col -> !col.isInternal())
                    .filter(col -> PulsarColumnHandle.HandleKeyValueType.VALUE
                            .equals(col.getHandleKeyValueType()))
                    .collect(toImmutableSet());
            PulsarRowDecoder valueDecoder = null;
            if (valueColumnHandles.size() > 0) {
                valueDecoder = decoderFactory.createRowDecoder(topicName,
                        kvSchemaInfo.getValue(),
                        valueColumnHandles);
            }

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey;
            if (keyColumnHandles.size() > 0) {
                decodedKey = keyDecoder.decodeRow(keyByteBuf);
                decodedKey.ifPresent(currentRowValuesMap::putAll);
            }
            if (valueColumnHandles.size() > 0) {
                Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue =
                        valueDecoder.decodeRow(valueByteBuf);
                decodedValue.ifPresent(currentRowValuesMap::putAll);
            }
        }
        else {
            PulsarRowDecoder messageDecoder = decoderFactory.createRowDecoder(topicName,
                    schemaInfo,
                    columnHandles.stream()
                            .filter(col -> !col.isInternal())
                            .filter(col -> PulsarColumnHandle.HandleKeyValueType.NONE
                                    .equals(col.getHandleKeyValueType()))
                            .collect(toImmutableSet()));
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue =
                    messageDecoder.decodeRow(currentMessage.getData());
            decodedValue.ifPresent(currentRowValuesMap::putAll);
        }

        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                if (PulsarInternalColumn.PARTITION.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, longValueProvider(this.partition));
                }
                else if (PulsarInternalColumn.EVENT_TIME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, PulsarFieldValueProviders.timeValueProvider(
                            PulsarConnectorUtils.roundToTrinoTime(currentMessage.getEventTime()), currentMessage.getEventTime() == 0));
                }
                else if (PulsarInternalColumn.PUBLISH_TIME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, PulsarFieldValueProviders.timeValueProvider(
                            PulsarConnectorUtils.roundToTrinoTime(currentMessage.getPublishTime()), currentMessage.getPublishTime() == 0));
                }
                else if (PulsarInternalColumn.MESSAGE_ID.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, bytesValueProvider(
                            currentMessage.getMessageId().toString().getBytes(StandardCharsets.UTF_8)));
                }
                else if (PulsarInternalColumn.SEQUENCE_ID.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle, longValueProvider(currentMessage.getSequenceId()));
                }
                else if (PulsarInternalColumn.PRODUCER_NAME.getName().equals(columnHandle.getName())) {
                    currentRowValuesMap.put(columnHandle,
                            bytesValueProvider(currentMessage.getProducerName().getBytes(StandardCharsets.UTF_8)));
                }
                else if (PulsarInternalColumn.KEY.getName().equals(columnHandle.getName())) {
                    String key = currentMessage.getKey().orElse(null);
                    currentRowValuesMap.put(columnHandle, bytesValueProvider(key == null ? null :
                            key.getBytes(StandardCharsets.UTF_8)));
                }
                else if (PulsarInternalColumn.PROPERTIES.getName().equals(columnHandle.getName())) {
                    try {
                        currentRowValuesMap.put(columnHandle, bytesValueProvider(
                                new ObjectMapper().writeValueAsBytes(currentMessage.getProperties())));
                    }
                    catch (JsonProcessingException e) {
                        throw new RuntimeException("Fail to decode Pulsar properties column", e);
                    }
                }
                else {
                    throw new IllegalArgumentException("unknown internal field " + columnHandle.getName());
                }
            }
        }
        try {
            currentMessage.release();
        }
        catch (IllegalReferenceCountException e) {
        }

        for (int i = 0; i < columnHandles.size(); i++) {
            PulsarColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }

        metricsTracker.incrNumberOfRecordDeserialized();

        // stats for time spend deserializing
        metricsTracker.recordRecordDeserializeStart(Duration.nanosSince(recordDeserializeStart));

        return true;
    }

    private SchemaInfo getBytesSchemaInfo(SchemaType schemaType, String schemaName)
    {
        if (!schemaType.equals(SchemaType.BYTES) && !schemaType.equals(SchemaType.NONE)) {
            return null;
        }
        if (schemaName.equals(Schema.BYTES.getSchemaInfo().getName())) {
            return Schema.BYTES.getSchemaInfo();
        }
        else if (schemaName.equals(Schema.BYTEBUFFER.getSchemaInfo().getName())) {
            return Schema.BYTEBUFFER.getSchemaInfo();
        }
        else {
            return Schema.BYTES.getSchemaInfo();
        }
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

    private FieldValueProvider getFieldValueProvider(int fieldIndex,
                                                     Class<?> expectedType)
    {
        checkArgument(fieldIndex < columnHandles.size(), "Invalid field index");
        checkFieldType(fieldIndex, expectedType);
        return currentRowValues[fieldIndex];
    }

    @Override
    public Object getObject(int field)
    {
        return getFieldValueProvider(field, Block.class).getBlock();
    }

    @Override
    public boolean isNull(int field)
    {
        FieldValueProvider provider = currentRowValues[field];
        return provider == null || provider.isNull();
    }

    @Override
    public void close()
    {
        if (messageQueue != null) {
            messageQueue.drain(RawMessage::release);
        }

        if (entryQueue != null) {
            entryQueue.drain(Entry::release);
        }

        if (deserializeEntries != null) {
            deserializeEntries.interrupt();
        }
        if (cursor != null) {
            try {
                cursor.close();
            }
            catch (Exception e) {
                log.error(e);
            }
        }

        metricsTracker.recordTotalExecutionTime(Duration.nanosSince(startTime));
    }

    private void checkFieldType(int field, Class<?> expected)
    {
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private void initEntryCacheSizeAllocator(PulsarConnectorConfig connectorConfig)
    {
        if (connectorConfig.getMaxSplitQueueSizeBytes() >= 0) {
            entryQueueCacheSizeAllocator = new NoStrictCacheSizeAllocator(
                    connectorConfig.getMaxSplitQueueSizeBytes() / 2);
            messageQueueCacheSizeAllocator = new NoStrictCacheSizeAllocator(
                    connectorConfig.getMaxSplitQueueSizeBytes() / 2);
        }
        else {
            entryQueueCacheSizeAllocator = new NullCacheSizeAllocator();
            messageQueueCacheSizeAllocator = new NullCacheSizeAllocator();
        }
    }
}
