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
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.trino.plugin.pulsar.mock.MockManagedLedgerFactory;
import io.trino.plugin.pulsar.mock.MockNamespaces;
import io.trino.plugin.pulsar.mock.MockPulsarAdmin;
import io.trino.plugin.pulsar.mock.MockPulsarConnectorConfig;
import io.trino.plugin.pulsar.mock.MockPulsarConnectorManagedLedgerFactory;
import io.trino.plugin.pulsar.mock.MockPulsarSplitManager;
import io.trino.plugin.pulsar.mock.MockSchemas;
import io.trino.plugin.pulsar.mock.MockTenants;
import io.trino.plugin.pulsar.mock.MockTopics;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorContext;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public abstract class TestPulsarConnector
{
    protected static final long currentTimeMs = 1534806330000L;

    protected PulsarConnectorConfig pulsarConnectorConfig;

    protected PulsarMetadata pulsarMetadata;

    protected PulsarAdmin pulsarAdmin;

    protected Schemas schemas;

    protected PulsarSplitManager pulsarSplitManager;

    protected Map<TopicName, PulsarRecordCursor> pulsarRecordCursors = new HashMap<>();

    protected static PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory;

    protected static final PulsarConnectorId pulsarConnectorId = new PulsarConnectorId("test-connector");

    protected static List<TopicName> topicNames;
    protected static List<TopicName> partitionedTopicNames;
    protected static Map<String, Integer> partitionedTopicsToPartitions;
    protected static Map<String, SchemaInfo> topicsToSchemas;
    protected static Map<String, Long> topicsToNumEntries;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    protected static List<String> fooFieldNames = new ArrayList<>();

    protected static final NamespaceName NAMESPACE_NAME_1 = NamespaceName.get("tenant-1", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_2 = NamespaceName.get("tenant-1", "ns-2");
    protected static final NamespaceName NAMESPACE_NAME_3 = NamespaceName.get("tenant-2", "ns-1");
    protected static final NamespaceName NAMESPACE_NAME_4 = NamespaceName.get("tenant-2", "ns-2");

    protected static final TopicName TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-1");
    protected static final TopicName TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1, "topic-2");
    protected static final TopicName TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2, "topic-1");
    protected static final TopicName TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3, "topic-1");
    protected static final TopicName TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-1");
    protected static final TopicName TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4, "topic-2");
    protected static final TopicName NON_SCHEMA_TOPIC = TopicName.get(
            "persistent", NAMESPACE_NAME_2, "non-schema-topic");

    protected static final TopicName PARTITIONED_TOPIC_1 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_2 = TopicName.get("persistent", NAMESPACE_NAME_1,
            "partitioned-topic-2");
    protected static final TopicName PARTITIONED_TOPIC_3 = TopicName.get("persistent", NAMESPACE_NAME_2,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_4 = TopicName.get("persistent", NAMESPACE_NAME_3,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_5 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-1");
    protected static final TopicName PARTITIONED_TOPIC_6 = TopicName.get("persistent", NAMESPACE_NAME_4,
            "partitioned-topic-2");

    public static class Foo
    {
        public enum TestEnum
        {
            TEST_ENUM_1,
            TEST_ENUM_2,
            TEST_ENUM_3
        }

        public int field1;
        public String field2;
        public float field3;
        public double field4;
        public boolean field5;
        public long field6;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"long\", \"logicalType\": \"timestamp-millis\" }")
        public long timestamp;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"time-millis\" }")
        public int time;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        public int date;
        public Bar bar;
        public TestEnum field7;
    }

    public static class Bar
    {
        public Integer field1;
        public String field2;
        public float field3;
    }

    protected static Map<TopicName, List<PulsarColumnHandle>> topicsToColumnHandles = new HashMap<>();

    protected static Map<TopicName, PulsarSplit> splits;
    protected static Map<String, Function<Integer, Object>> fooFunctions;

    static {
        try {
            topicNames = new LinkedList<>();
            topicNames.add(TOPIC_1);
            topicNames.add(TOPIC_2);
            topicNames.add(TOPIC_3);
            topicNames.add(TOPIC_4);
            topicNames.add(TOPIC_5);
            topicNames.add(TOPIC_6);
            topicNames.add(NON_SCHEMA_TOPIC);

            partitionedTopicNames = new LinkedList<>();
            partitionedTopicNames.add(PARTITIONED_TOPIC_1);
            partitionedTopicNames.add(PARTITIONED_TOPIC_2);
            partitionedTopicNames.add(PARTITIONED_TOPIC_3);
            partitionedTopicNames.add(PARTITIONED_TOPIC_4);
            partitionedTopicNames.add(PARTITIONED_TOPIC_5);
            partitionedTopicNames.add(PARTITIONED_TOPIC_6);

            partitionedTopicsToPartitions = new HashMap<>();
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_1.toString(), 2);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_2.toString(), 3);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_3.toString(), 4);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_4.toString(), 5);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_5.toString(), 6);
            partitionedTopicsToPartitions.put(PARTITIONED_TOPIC_6.toString(), 7);

            topicsToSchemas = new HashMap<>();
            topicsToSchemas.put(TOPIC_1.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_2.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_3.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_4.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_5.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(TOPIC_6.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());

            topicsToSchemas.put(PARTITIONED_TOPIC_1.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_2.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_3.getSchemaName(), Schema.AVRO(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_4.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_5.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());
            topicsToSchemas.put(PARTITIONED_TOPIC_6.getSchemaName(), Schema.JSON(TestPulsarMetadata.Foo.class).getSchemaInfo());

            topicsToNumEntries = new HashMap<>();
            topicsToNumEntries.put(TOPIC_1.getSchemaName(), 1233L);
            topicsToNumEntries.put(TOPIC_2.getSchemaName(), 0L);
            topicsToNumEntries.put(TOPIC_3.getSchemaName(), 100L);
            topicsToNumEntries.put(TOPIC_4.getSchemaName(), 12345L);
            topicsToNumEntries.put(TOPIC_5.getSchemaName(), 8000L);
            topicsToNumEntries.put(TOPIC_6.getSchemaName(), 1L);

            topicsToNumEntries.put(NON_SCHEMA_TOPIC.getSchemaName(), 8000L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_1.getSchemaName(), 1233L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_2.getSchemaName(), 8000L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_3.getSchemaName(), 100L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_4.getSchemaName(), 0L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_5.getSchemaName(), 800L);
            topicsToNumEntries.put(PARTITIONED_TOPIC_6.getSchemaName(), 1L);

            fooFieldNames.add("field1");
            fooFieldNames.add("field2");
            fooFieldNames.add("field3");
            fooFieldNames.add("field4");
            fooFieldNames.add("field5");
            fooFieldNames.add("field6");
            fooFieldNames.add("timestamp");
            fooFieldNames.add("time");
            fooFieldNames.add("date");
            fooFieldNames.add("bar");
            fooFieldNames.add("field7");

            ConnectorContext trinoConnectorContext = new TestingConnectorContext();
            dispatchingRowDecoderFactory = new PulsarDispatchingRowDecoderFactory(trinoConnectorContext.getTypeManager());

            topicsToColumnHandles.put(PARTITIONED_TOPIC_1, getColumnColumnHandles(PARTITIONED_TOPIC_1, topicsToSchemas.get(PARTITIONED_TOPIC_1.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_2, getColumnColumnHandles(PARTITIONED_TOPIC_2, topicsToSchemas.get(PARTITIONED_TOPIC_2.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_3, getColumnColumnHandles(PARTITIONED_TOPIC_3, topicsToSchemas.get(PARTITIONED_TOPIC_3.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_4, getColumnColumnHandles(PARTITIONED_TOPIC_4, topicsToSchemas.get(PARTITIONED_TOPIC_4.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_5, getColumnColumnHandles(PARTITIONED_TOPIC_5, topicsToSchemas.get(PARTITIONED_TOPIC_5.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(PARTITIONED_TOPIC_6, getColumnColumnHandles(PARTITIONED_TOPIC_6, topicsToSchemas.get(PARTITIONED_TOPIC_6.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));

            topicsToColumnHandles.put(TOPIC_1, getColumnColumnHandles(TOPIC_1, topicsToSchemas.get(TOPIC_1.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(TOPIC_2, getColumnColumnHandles(TOPIC_2, topicsToSchemas.get(TOPIC_2.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(TOPIC_3, getColumnColumnHandles(TOPIC_3, topicsToSchemas.get(TOPIC_3.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(TOPIC_4, getColumnColumnHandles(TOPIC_4, topicsToSchemas.get(TOPIC_4.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(TOPIC_5, getColumnColumnHandles(TOPIC_5, topicsToSchemas.get(TOPIC_5.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));
            topicsToColumnHandles.put(TOPIC_6, getColumnColumnHandles(TOPIC_6, topicsToSchemas.get(TOPIC_6.getSchemaName()), PulsarColumnHandle.HandleKeyValueType.NONE, true));

            splits = new HashMap<>();

            List<TopicName> allTopics = new LinkedList<>();
            allTopics.addAll(topicNames);
            allTopics.addAll(partitionedTopicNames);

            for (TopicName topicName : allTopics) {
                if (topicsToSchemas.containsKey(topicName.getSchemaName())) {
                    splits.put(topicName, new PulsarSplit(0, pulsarConnectorId.toString(),
                            topicName.getNamespace(), topicName.getLocalName(), topicName.getLocalName(),
                            topicsToNumEntries.get(topicName.getSchemaName()),
                            new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema(),
                                    StandardCharsets.ISO_8859_1),
                            topicsToSchemas.get(topicName.getSchemaName()).getType(),
                            0, topicsToNumEntries.get(topicName.getSchemaName()),
                            0, 0, TupleDomain.all(),
                            objectMapper.writeValueAsString(
                                    topicsToSchemas.get(topicName.getSchemaName()).getProperties()), null));
                }
            }

            fooFunctions = new HashMap<>();

            fooFunctions.put("field1", integer -> integer);
            fooFunctions.put("field2", String::valueOf);
            fooFunctions.put("field3", Integer::floatValue);
            fooFunctions.put("field4", Integer::doubleValue);
            fooFunctions.put("field5", integer -> integer % 2 == 0);
            fooFunctions.put("field6", Integer::longValue);
            fooFunctions.put("timestamp", integer -> System.currentTimeMillis());
            fooFunctions.put("time", integer -> {
                LocalTime now = LocalTime.now(ZoneId.systemDefault());
                return now.toSecondOfDay() * 1000;
            });
            fooFunctions.put("date", integer -> {
                LocalDate localDate = LocalDate.now();
                LocalDate epoch = LocalDate.ofEpochDay(0);
                return toIntExact(ChronoUnit.DAYS.between(epoch, localDate));
            });
            fooFunctions.put("bar.field1", integer -> integer % 3 == 0 ? null : integer + 1);
            fooFunctions.put("bar.field2", integer -> integer % 2 == 0 ? null : String.valueOf(integer + 2));
            fooFunctions.put("bar.field3", integer -> integer + 3.0f);

            fooFunctions.put("field7", integer -> Foo.TestEnum.values()[integer % Foo.TestEnum.values().length]);
        }
        catch (Throwable e) {
            System.out.println("Error: " + e);
            System.out.println("Stacktrace: " + Arrays.asList(e.getStackTrace()));
        }
    }

    /**
     * Parse PulsarColumnMetadata to PulsarColumnHandle Util
     *
     * @param schemaInfo
     * @param handleKeyValueType
     * @param includeInternalColumn
     * @return
     */
    protected static List<PulsarColumnHandle> getColumnColumnHandles(TopicName topicName, SchemaInfo schemaInfo,
                                                                     PulsarColumnHandle.HandleKeyValueType handleKeyValueType, boolean includeInternalColumn)
    {
        List<PulsarColumnHandle> columnHandles = new ArrayList<>();
        List<ColumnMetadata> columnMetadata = mockColumnMetadata().getPulsarColumns(topicName, schemaInfo,
                includeInternalColumn, handleKeyValueType);
        columnMetadata.forEach(column -> {
            PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) column;
            columnHandles.add(new PulsarColumnHandle(
                    pulsarConnectorId.toString(),
                    pulsarColumnMetadata.getNameWithCase(),
                    pulsarColumnMetadata.getType(),
                    pulsarColumnMetadata.isHidden(),
                    pulsarColumnMetadata.isInternal(),
                    pulsarColumnMetadata.getDecoderExtraInfo().getMapping(),
                    pulsarColumnMetadata.getDecoderExtraInfo().getDataFormat(), pulsarColumnMetadata.getDecoderExtraInfo().getFormatHint(),
                    Optional.of(pulsarColumnMetadata.getHandleKeyValueType())));
        });
        return columnHandles;
    }

    public static PulsarMetadata mockColumnMetadata()
    {
        ConnectorContext trinoConnectorContext = new TestingConnectorContext();
        PulsarConnectorConfig pulsarConnectorConfig = new PulsarConnectorConfig();
        pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);
        PulsarDispatchingRowDecoderFactory dispatchingRowDecoderFactory =
                new PulsarDispatchingRowDecoderFactory(trinoConnectorContext.getTypeManager());
        PulsarMetadata pulsarMetadata = new PulsarMetadata(pulsarConnectorId, pulsarConnectorConfig, dispatchingRowDecoderFactory);
        return pulsarMetadata;
    }

    public static PulsarConnectorId getPulsarConnectorId()
    {
        assertNotNull(pulsarConnectorId);
        return pulsarConnectorId;
    }

    private static List<Entry> getTopicEntries(String topicSchemaName)
    {
        List<Entry> entries = new LinkedList<>();

        long count = topicsToNumEntries.get(topicSchemaName);
        for (int i = 0; i < count; i++) {
            Foo foo = new Foo();
            foo.field1 = (int) count;
            foo.field2 = String.valueOf(count);
            foo.field3 = count;
            foo.field4 = count;
            foo.field5 = count % 2 == 0;
            foo.field6 = count;
            foo.timestamp = System.currentTimeMillis();

            LocalTime now = LocalTime.now(ZoneId.systemDefault());
            foo.time = now.toSecondOfDay() * 1000;

            LocalDate localDate = LocalDate.now();
            LocalDate epoch = LocalDate.ofEpochDay(0);
            foo.date = toIntExact(ChronoUnit.DAYS.between(epoch, localDate));

            MessageMetadata messageMetadata = new MessageMetadata()
                    .setProducerName("test-producer").setSequenceId(i)
                    .setPublishTime(currentTimeMs + i);

            Schema schema = topicsToSchemas.get(topicSchemaName).getType() == SchemaType.AVRO ? AvroSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build()) : JSONSchema.of(SchemaDefinition.<Foo>builder().withPojo(Foo.class).build());

            ByteBuf payload = Unpooled
                    .copiedBuffer(schema.encode(foo));

            ByteBuf byteBuf = org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload(
                    Commands.ChecksumType.Crc32c, messageMetadata, payload);

            Entry entry = EntryImpl.create(0, i, byteBuf);
            log.info("create entry: %s", entry.getEntryId());
            entries.add(entry);
        }
        return entries;
    }

    public LongAdder completedBytes = new LongAdder();

    private static final Logger log = Logger.get(TestPulsarConnector.class);

    protected static List<String> getNamespace(String tenant)
    {
        return topicNames.stream()
                .filter(topicName -> topicName.getTenant().equals(tenant))
                .map(TopicName::getNamespace)
                .distinct()
                .collect(Collectors.toCollection(LinkedList::new));
    }

    protected static List<String> getTopics(String ns)
    {
        List<String> topics = new ArrayList<>(topicNames.stream()
                .filter(topicName -> topicName.getNamespace().equals(ns))
                .map(TopicName::toString).collect(Collectors.toList()));
        partitionedTopicNames.stream().filter(topicName -> topicName.getNamespace().equals(ns)).forEach(topicName -> {
            for (Integer i = 0; i < partitionedTopicsToPartitions.get(topicName.toString()); i++) {
                topics.add(TopicName.get(topicName + "-partition-" + i).toString());
            }
        });
        return topics;
    }

    protected static List<String> getPartitionedTopics(String ns)
    {
        return partitionedTopicNames.stream()
                .filter(topicName -> topicName.getNamespace().equals(ns))
                .map(TopicName::toString)
                .collect(Collectors.toList());
    }

    @BeforeMethod
    public void setup() throws Exception
    {
        Tenants tenants = new MockTenants(new LinkedList<>(topicNames.stream()
                .map(TopicName::getTenant)
                .collect(Collectors.toSet())));

        Namespaces namespaces = new MockNamespaces(topicNames);

        Topics topics = new MockTopics(topicNames, partitionedTopicNames, partitionedTopicsToPartitions);
        ManagedLedgerFactory managedLedgerFactory = new MockManagedLedgerFactory(topicsToNumEntries, fooFunctions, topicsToSchemas,
                completedBytes);

        schemas = new MockSchemas(topicsToSchemas);

        pulsarAdmin = new MockPulsarAdmin("http://localhost", new ClientConfigurationData(),
                tenants, namespaces, topics, schemas);

        this.pulsarConnectorConfig = new MockPulsarConnectorConfig(pulsarAdmin);
        this.pulsarConnectorConfig.setMaxEntryReadBatchSize(1);
        this.pulsarConnectorConfig.setMaxSplitEntryQueueSize(10);
        this.pulsarConnectorConfig.setMaxSplitMessageQueueSize(100);

        PulsarAdminClientProvider.setPulsarAdmin(pulsarAdmin);

        this.pulsarMetadata = new PulsarMetadata(pulsarConnectorId, this.pulsarConnectorConfig, dispatchingRowDecoderFactory);

        this.pulsarSplitManager = new MockPulsarSplitManager(pulsarConnectorId, pulsarConnectorConfig, managedLedgerFactory);

        for (Map.Entry<TopicName, PulsarSplit> split : splits.entrySet()) {
            PulsarRecordCursor pulsarRecordCursor = new PulsarRecordCursor(
                    topicsToColumnHandles.get(split.getKey()), split.getValue(),
                    pulsarConnectorConfig, managedLedgerFactory, new ManagedLedgerConfig(),
                    new PulsarConnectorMetricsTracker(new NullStatsProvider()), dispatchingRowDecoderFactory,
                    new MockPulsarConnectorManagedLedgerFactory(managedLedgerFactory));
            this.pulsarRecordCursors.put(split.getKey(), pulsarRecordCursor);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup()
    {
        completedBytes.reset();
    }

    @DataProvider(name = "rewriteNamespaceDelimiter")
    public static Object[][] serviceUrls()
    {
        return new Object[][]{
                {"|"}, {null}
        };
    }

    protected void updateRewriteNamespaceDelimiterIfNeeded(String delimiter)
    {
        if (!Strings.nullToEmpty(delimiter).trim().isEmpty()) {
            pulsarConnectorConfig.setNamespaceDelimiterRewriteEnable(true);
            pulsarConnectorConfig.setRewriteNamespaceDelimiter(delimiter);
        }
        else {
            pulsarConnectorConfig.setNamespaceDelimiterRewriteEnable(false);
        }
    }
}
