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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.trino.plugin.pulsar.mock.MockCursor;
import io.trino.plugin.pulsar.mock.MockManagedLedgerFactory;
import io.trino.plugin.pulsar.mock.MockPulsarConnectorManagedLedgerFactory;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPulsarRecordCursor
        extends TestPulsarConnector
{
    static final String KEY_SCHEMA_COLUMN_PREFIX = "__key.";
    static final String PRIMITIVE_COLUMN_NAME = "__value__";

    static class Foo
    {
        public String getField1()
        {
            return field1;
        }

        public void setField1(String field1)
        {
            this.field1 = field1;
        }

        public Integer getField2()
        {
            return field2;
        }

        private String field1;

        public void setField2(Integer field2)
        {
            this.field2 = field2;
        }

        private Integer field2;
    }

    static class Boo
    {
        private String field1;
        private Boolean field2;
        private Double field3;

        public String getField1()
        {
            return field1;
        }

        public void setField1(String field1)
        {
            this.field1 = field1;
        }

        public Boolean getField2()
        {
            return field2;
        }

        public void setField2(Boolean field2)
        {
            this.field2 = field2;
        }

        public Double getField3()
        {
            return field3;
        }

        public void setField3(Double field3)
        {
            this.field3 = field3;
        }
    }

    private static final Logger log = Logger.get(TestPulsarRecordCursor.class);

    private static class MockPulsarSchemaInfoProvider
                implements SchemaInfoProvider
    {
        private SchemaInfo schemaInfo;

        public MockPulsarSchemaInfoProvider(SchemaInfo schemaInfo)
        {
            this.schemaInfo = schemaInfo;
        }

        @Override
        public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] bytes)
        {
            return completedFuture(schemaInfo);
        }

        @Override
        public CompletableFuture<SchemaInfo> getLatestSchema()
        {
            return null;
        }

        @Override
        public String getTopicName()
        {
            return null;
        }
    }

    @Test(singleThreaded = true)
    public void testTopics() throws Exception
    {
        for (Map.Entry<TopicName, PulsarRecordCursor> entry : pulsarRecordCursors.entrySet()) {
            log.info("!------ topic %s ------!", entry.getKey());
            setup();

            List<PulsarColumnHandle> fooColumnHandles = topicsToColumnHandles.get(entry.getKey());
            PulsarRecordCursor pulsarRecordCursor = entry.getValue();

            SchemaInfoProvider pulsarSchemaInfoProvider = new MockPulsarSchemaInfoProvider(topicsToSchemas.get(entry.getKey().getSchemaName()));
            pulsarRecordCursor.setPulsarSchemaInfoProvider(pulsarSchemaInfoProvider);

            TopicName topicName = entry.getKey();

            int count = 0;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < fooColumnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(fooColumnHandles.get(i).getName());
                    }
                    else {
                        if (fooColumnHandles.get(i).getName().equals("field1")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Integer) fooFunctions.get("field1").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field2")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), ((String) fooFunctions.get("field2").apply(count)).getBytes(StandardCharsets.UTF_8));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field3")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Float.floatToIntBits(((Float) fooFunctions.get("field3").apply(count)).floatValue()));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field4")) {
                            assertEquals(pulsarRecordCursor.getDouble(i), ((Double) fooFunctions.get("field4").apply(count)).doubleValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field5")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), ((Boolean) fooFunctions.get("field5").apply(count)).booleanValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field6")) {
                            assertEquals(pulsarRecordCursor.getLong(i), ((Long) fooFunctions.get("field6").apply(count)).longValue());
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("timestamp")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("time")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("date")) {
                            pulsarRecordCursor.getLong(i);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("bar")) {
                            assertTrue(fooColumnHandles.get(i).getType() instanceof RowType);
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else if (fooColumnHandles.get(i).getName().equals("field7")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), fooFunctions.get("field7").apply(count).toString().getBytes(StandardCharsets.UTF_8));
                            columnsSeen.add(fooColumnHandles.get(i).getName());
                        }
                        else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(fooColumnHandles.get(i).getName())) {
                                columnsSeen.add(fooColumnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), fooColumnHandles.size());
                count++;
            }
            assertEquals(count, topicsToNumEntries.get(topicName.getSchemaName()).longValue());
            assertEquals(pulsarRecordCursor.getCompletedBytes(), completedBytes.longValue());
            cleanup();
            pulsarRecordCursor.close();
        }
    }

    @Test(singleThreaded = true)
    public void TestKeyValueStructSchema() throws Exception
    {
        TopicName topicName = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-4");
        Long entriesNum = 5L;

        for (KeyValueEncodingType encodingType :
                Arrays.asList(KeyValueEncodingType.INLINE, KeyValueEncodingType.SEPARATED)) {
            KeyValueSchema schema = (KeyValueSchema) Schema.KeyValue(Schema.JSON(Foo.class), Schema.AVRO(Boo.class),
                    encodingType);

            Foo foo = new Foo();
            foo.field1 = "field1-value";
            foo.field2 = 20;
            Boo boo = new Boo();
            boo.field1 = "field1-value";
            boo.field2 = true;
            boo.field3 = 10.2;

            KeyValue message = new KeyValue<>(foo, boo);
            List<PulsarColumnHandle> columnHandles = getColumnColumnHandles(topicName, schema.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);
            PulsarRecordCursor pulsarRecordCursor = mockKeyValueSchemaPulsarRecordCursor(entriesNum, topicName,
                    schema, message, columnHandles);

            assertNotNull(pulsarRecordCursor);
            Long count = 0L;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < columnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(columnHandles.get(i).getName());
                    }
                    else {
                        if (columnHandles.get(i).getName().equals("field1")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), boo.field1.getBytes(StandardCharsets.UTF_8));
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else if (columnHandles.get(i).getName().equals("field2")) {
                            assertEquals(pulsarRecordCursor.getBoolean(i), boo.field2.booleanValue());
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else if (columnHandles.get(i).getName().equals("field3")) {
                            assertEquals((Double) pulsarRecordCursor.getDouble(i), (Double) boo.field3);
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else if (columnHandles.get(i).getName().equals(PulsarColumnMetadata.KEY_SCHEMA_COLUMN_PREFIX +
                                "field1")) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), foo.field1.getBytes(StandardCharsets.UTF_8));
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else if (columnHandles.get(i).getName().equals(PulsarColumnMetadata.KEY_SCHEMA_COLUMN_PREFIX +
                                "field2")) {
                            assertEquals(pulsarRecordCursor.getLong(i), Long.valueOf(foo.field2).longValue());
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(columnHandles.get(i).getName())) {
                                columnsSeen.add(columnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), columnHandles.size());
                count++;
            }
            assertEquals(count, entriesNum);
            pulsarRecordCursor.close();
        }
    }

    @Test(singleThreaded = true)
    public void TestKeyValuePrimitiveSchema() throws Exception
    {
        TopicName topicName = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-4");
        Long entriesNum = 5L;

        for (KeyValueEncodingType encodingType :
                Arrays.asList(KeyValueEncodingType.INLINE, KeyValueEncodingType.SEPARATED)) {
            KeyValueSchema schema = (KeyValueSchema) Schema.KeyValue(Schema.INT32, Schema.STRING,
                    encodingType);

            String value = "primitive_message_value";
            Integer key = 23;
            KeyValue message = new KeyValue<>(key, value);

            List<PulsarColumnHandle> columnHandles = getColumnColumnHandles(topicName, schema.getSchemaInfo(), PulsarColumnHandle.HandleKeyValueType.NONE, true);
            PulsarRecordCursor pulsarRecordCursor = mockKeyValueSchemaPulsarRecordCursor(entriesNum, topicName,
                    schema, message, columnHandles);

            assertNotNull(pulsarRecordCursor);
            Long count = 0L;
            while (pulsarRecordCursor.advanceNextPosition()) {
                List<String> columnsSeen = new LinkedList<>();
                for (int i = 0; i < columnHandles.size(); i++) {
                    if (pulsarRecordCursor.isNull(i)) {
                        columnsSeen.add(columnHandles.get(i).getName());
                    }
                    else {
                        if (columnHandles.get(i).getName().equals(PRIMITIVE_COLUMN_NAME)) {
                            assertEquals(pulsarRecordCursor.getSlice(i).getBytes(), value.getBytes(StandardCharsets.UTF_8));
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else if (columnHandles.get(i).getName().equals(KEY_SCHEMA_COLUMN_PREFIX +
                                PRIMITIVE_COLUMN_NAME)) {
                            assertEquals((Long) pulsarRecordCursor.getLong(i), Long.valueOf(key));
                            columnsSeen.add(columnHandles.get(i).getName());
                        }
                        else {
                            if (PulsarInternalColumn.getInternalFieldsMap().containsKey(columnHandles.get(i).getName())) {
                                columnsSeen.add(columnHandles.get(i).getName());
                            }
                        }
                    }
                }
                assertEquals(columnsSeen.size(), columnHandles.size());
                count++;
            }
            assertEquals(count, entriesNum);
            pulsarRecordCursor.close();
        }
    }

    private static class MockKeyValueSchemaCursor
            extends MockCursor
    {
        private KeyValueSchema schema;

        private KeyValue message;

        public MockKeyValueSchemaCursor(String topic, String schemaName, long entries, Map<String, Integer> positions,
                                        Map<String, Long> topicsToNumEntries, Map<String, Function<Integer, Object>> fooFunctions,
                                        Map<String, SchemaInfo> topicsToSchemas, LongAdder completedBytes, KeyValueSchema schema,
                                        KeyValue message)
        {
            super(topic, schemaName, entries, positions, topicsToNumEntries, fooFunctions,
                    topicsToSchemas, completedBytes);
            this.schema = schema;
            this.message = message;
        }

        @Override
        public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, AsyncCallbacks.ReadEntriesCallback callback, Object ctx, PositionImpl mexPosition)
        {
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    List<Entry> entries = new LinkedList<>();
                    for (int i = 0; i < numberOfEntriesToRead; i++) {
                        MessageMetadata messageMetadata =
                                new MessageMetadata()
                                        .setProducerName("test-producer").setSequenceId(positions.get(topic))
                                        .setPublishTime(System.currentTimeMillis());

                        if (KeyValueEncodingType.SEPARATED.equals(schema.getKeyValueEncodingType())) {
                            messageMetadata
                                    .setPartitionKey(new String(schema
                                            .getKeySchema().encode(message.getKey()), Charset.forName(
                                            "UTF-8")))
                                    .setPartitionKeyB64Encoded(false);
                        }

                        ByteBuf dataPayload = Unpooled.wrappedBuffer(schema.encode(message));

                        ByteBuf byteBuf = org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload(
                                org.apache.pulsar.common.protocol.Commands.ChecksumType.Crc32c, messageMetadata, dataPayload);

                        entries.add(EntryImpl.create(0, positions.get(topic), byteBuf));
                        positions.put(topic, positions.get(topic) + 1);
                    }

                    callback.readEntriesComplete(entries, ctx);
                }
            }).start();
        }
    }

    private static class MockKeyValueSchemaManagedLedgerFactory
            extends MockManagedLedgerFactory
    {
        private long entriesNum;

        private KeyValueSchema schema;

        private KeyValue message;

        public MockKeyValueSchemaManagedLedgerFactory(Map<String, Long> topicsToNumEntries,
                                                      Map<String, Function<Integer, Object>> fooFunctions,
                                                      Map<String, SchemaInfo> topicsToSchemas, LongAdder completedBytes,
                                                      long entriesNum, KeyValueSchema schema, KeyValue message)
        {
            super(topicsToNumEntries, fooFunctions, topicsToSchemas, completedBytes);
            this.entriesNum = entriesNum;
            this.schema = schema;
            this.message = message;
        }

        @Override
        public ReadOnlyCursor openReadOnlyCursor(String topic, Position position, ManagedLedgerConfig managedLedgerConfig) throws InterruptedException, ManagedLedgerException
        {
            PositionImpl positionImpl = (PositionImpl) position;

            int entryId = positionImpl.getEntryId() == -1 ? 0 : (int) positionImpl.getEntryId();

            positions.put(topic, entryId);
            return new MockKeyValueSchemaCursor(topic, "", entriesNum,
                positions, null, null, null, null,
                schema, message);
        }
    }

    /**
     * mock a simple PulsarRecordCursor for KeyValueSchema test.
     *
     * @param entriesNum
     * @param topicName
     * @param schema
     * @param message
     * @param columnHandles
     * @return
     * @throws Exception
     */
    private PulsarRecordCursor mockKeyValueSchemaPulsarRecordCursor(final Long entriesNum, final TopicName topicName,
                                                                    final KeyValueSchema schema, KeyValue message, List<PulsarColumnHandle> columnHandles) throws Exception
    {
        ManagedLedgerFactory managedLedgerFactory = new MockKeyValueSchemaManagedLedgerFactory(null, null,
                null, completedBytes, entriesNum, schema, message);

        ObjectMapper objectMapper = new ObjectMapper();

        PulsarSplit split = new PulsarSplit(0, pulsarConnectorId.toString(),
                topicName.getNamespace(), topicName.getLocalName(), topicName.getLocalName(),
                entriesNum,
                new String(schema.getSchemaInfo().getSchema(), StandardCharsets.ISO_8859_1),
                schema.getSchemaInfo().getType(),
                0, entriesNum,
                0, 0, TupleDomain.all(),
                objectMapper.writeValueAsString(
                        schema.getSchemaInfo().getProperties()), null);

        PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                columnHandles, split,
                pulsarConnectorConfig, managedLedgerFactory, new ManagedLedgerConfig(),
                new PulsarConnectorMetricsTracker(new NullStatsProvider()), dispatchingRowDecoderFactory,
                new MockPulsarConnectorManagedLedgerFactory(managedLedgerFactory));

        SchemaInfoProvider pulsarSchemaInfoProvider = new MockPulsarSchemaInfoProvider(schema.getSchemaInfo());
        pulsarRecordCursor.setPulsarSchemaInfoProvider(pulsarSchemaInfoProvider);

        return pulsarRecordCursor;
    }
}
