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
package io.trino.tests.product.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.primitives.Ints;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.trino.tempto.ProductTest;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.kafka.KafkaMessage;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.trino.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.KAFKA_CONFLUENT_LICENSE;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.utils.SchemaRegistryClientUtils.getSchemaRegistryClient;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestKafkaProtobufReadsSmokeTest
        extends ProductTest
{
    private static final String KAFKA_SCHEMA = "product_tests";

    private static final String BASIC_DATATYPES_PROTOBUF_TOPIC_NAME = "read_basic_datatypes_protobuf";
    private static final String BASIC_DATATYPES_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/basic_datatypes.proto";

    private static final String BASIC_STRUCTURAL_PROTOBUF_TOPIC_NAME = "read_basic_structural_datatypes_protobuf";
    private static final String BASIC_STRUCTURAL_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/basic_structural_datatypes.proto";

    private static final String ALL_DATATYPES_PROTOBUF_TOPIC_SCHEMA_REGISTRY = "all_datatypes_protobuf_schema_registry";
    private static final String ALL_DATATYPES_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/all_datatypes.proto";

    private static final KafkaCatalog KAFKA_CATALOG = new KafkaCatalog("kafka", "", true, new ProtobufMessageSerializer());
    private static final KafkaCatalog KAFKA_SCHEMA_REGISTRY_CATALOG = new KafkaCatalog("kafka_schema_registry", "_schema_registry", false, new SchemaRegistryProtobufMessageSerializer());

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testSelectPrimitiveDataType()
            throws Exception
    {
        selectPrimitiveDataType(KAFKA_CATALOG);
    }

    @Test(groups = {KAFKA_CONFLUENT_LICENSE, PROFILE_SPECIFIC_TESTS})
    public void testSelectPrimitiveDataTypeWithSchemaRegistry()
            throws Exception
    {
        selectPrimitiveDataType(KAFKA_SCHEMA_REGISTRY_CATALOG);
    }

    private void selectPrimitiveDataType(KafkaCatalog kafkaCatalog)
            throws Exception
    {
        Map<String, Object> record = ImmutableMap.<String, Object>builder()
                .put("a_varchar", "foobar")
                .put("b_integer", 314)
                .put("c_bigint", 9223372036854775807L)
                .put("d_double", 1234567890.123456789)
                .put("e_float", 3.14f)
                .put("f_boolean", true)
                .buildOrThrow();
        String topicName = BASIC_DATATYPES_PROTOBUF_TOPIC_NAME + kafkaCatalog.topicNameSuffix();
        createProtobufTable(BASIC_DATATYPES_SCHEMA_PATH, BASIC_DATATYPES_PROTOBUF_TOPIC_NAME, topicName, record, kafkaCatalog.messageSerializer());

        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = onTrino().executeQuery(format("select * from %s.%s", kafkaCatalog.catalogName(), KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(
                            "foobar",
                            314,
                            9223372036854775807L,
                            1234567890.123456789,
                            3.14f,
                            true));
                });
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testSelectStructuralDataType()
            throws Exception
    {
        selectStructuralDataType(KAFKA_CATALOG);
    }

    @Test(groups = {KAFKA_CONFLUENT_LICENSE, PROFILE_SPECIFIC_TESTS})
    public void testSelectStructuralDataTypeWithSchemaRegistry()
            throws Exception
    {
        selectStructuralDataType(KAFKA_SCHEMA_REGISTRY_CATALOG);
    }

    private void selectStructuralDataType(KafkaCatalog kafkaCatalog)
            throws Exception
    {
        ImmutableMap<String, Object> record = ImmutableMap.of(
                "a_array", ImmutableList.of(100L, 101L),
                "a_map", ImmutableMap.of(
                        "key", "key1",
                        "value", 1234567890.123456789));
        String topicName = BASIC_STRUCTURAL_PROTOBUF_TOPIC_NAME + kafkaCatalog.topicNameSuffix();
        createProtobufTable(BASIC_STRUCTURAL_SCHEMA_PATH, BASIC_STRUCTURAL_PROTOBUF_TOPIC_NAME, topicName, record, kafkaCatalog.messageSerializer());
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = onTrino().executeQuery(format(
                            "SELECT a[1], a[2], m['key1'] FROM (SELECT %s as a, %s as m FROM %s.%s) t",
                            kafkaCatalog.columnMappingSupported() ? "c_array" : "a_array",
                            kafkaCatalog.columnMappingSupported() ? "c_map" : "a_map",
                            kafkaCatalog.catalogName(),
                            KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(100L, 101L, 1234567890.123456789));
                });
    }

    private record KafkaCatalog(String catalogName, String topicNameSuffix, boolean columnMappingSupported, MessageSerializer messageSerializer)
    {
        private KafkaCatalog(String catalogName, String topicNameSuffix, boolean columnMappingSupported, MessageSerializer messageSerializer)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.topicNameSuffix = requireNonNull(topicNameSuffix, "topicNameSuffix is null");
            this.columnMappingSupported = columnMappingSupported;
            this.messageSerializer = requireNonNull(messageSerializer, "messageSerializer is null");
        }

        @Override
        public String toString()
        {
            return catalogName;
        }
    }

    @Test(groups = {KAFKA_CONFLUENT_LICENSE, PROFILE_SPECIFIC_TESTS})
    public void testProtobufWithSchemaReferences()
            throws Exception
    {
        String timestampTopic = "timestamp";
        String timestampProtoFile = "google/protobuf/timestamp.proto";
        ProtobufSchema baseSchema = new ProtobufSchema(
                Resources.toString(Resources.getResource(TestKafkaProtobufReadsSmokeTest.class, "/" + timestampProtoFile), UTF_8),
                ImmutableList.of(),
                ImmutableMap.of(),
                null,
                timestampProtoFile);

        getSchemaRegistryClient().register(timestampTopic, baseSchema);

        ProtobufSchema actualSchema = new ProtobufSchema(
                Files.readString(Path.of(ALL_DATATYPES_SCHEMA_PATH)),
                ImmutableList.of(new SchemaReference(baseSchema.name(), timestampTopic, 1)),
                ImmutableMap.of(timestampProtoFile, baseSchema.canonicalString()),
                null,
                null);

        LocalDateTime timestamp = LocalDateTime.parse("2020-12-12T15:35:45.923");
        com.google.protobuf.Timestamp timestampProto = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(timestamp.toEpochSecond(ZoneOffset.UTC))
                .setNanos(timestamp.getNano())
                .build();

        Map<String, Object> record = ImmutableMap.<String, Object>builder()
                .put("a_varchar", "foobar")
                .put("b_integer", 2)
                .put("c_bigint", 9223372036854775807L)
                .put("d_double", 1234567890.123456789)
                .put("e_float", 3.14f)
                .put("f_boolean", true)
                .put("h_timestamp", timestampProto)
                .buildOrThrow();

        // This is a bit hacky as KafkaTableManager relies on kafka catalog's tables for inserting data into a given topic
        createProtobufTable(actualSchema, BASIC_DATATYPES_PROTOBUF_TOPIC_NAME, ALL_DATATYPES_PROTOBUF_TOPIC_SCHEMA_REGISTRY, record, new SchemaRegistryProtobufMessageSerializer());

        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = onTrino().executeQuery(format("select * from %s.%s.%s", KAFKA_SCHEMA_REGISTRY_CATALOG.catalogName(), KAFKA_SCHEMA, ALL_DATATYPES_PROTOBUF_TOPIC_SCHEMA_REGISTRY));
                    assertThat(queryResult).containsOnly(row(
                            "foobar",
                            2,
                            9223372036854775807L,
                            1234567890.123456789,
                            3.14f,
                            true,
                            "ZERO",
                            Timestamp.valueOf(timestamp)));
                });
    }

    private static void createProtobufTable(String schemaPath, String tableName, String topicName, Map<String, Object> record, MessageSerializer messageSerializer)
            throws Exception
    {
        createProtobufTable(new ProtobufSchema(Files.readString(Path.of(schemaPath))), tableName, topicName, record, messageSerializer);
    }

    private static void createProtobufTable(ProtobufSchema protobufSchema, String tableName, String topicName, Map<String, Object> record, MessageSerializer messageSerializer)
            throws Exception
    {
        byte[] protobufData = messageSerializer.serialize(topicName, protobufSchema, record);

        KafkaTableDefinition tableDefinition = new KafkaTableDefinition(
                KAFKA_SCHEMA + "." + tableName,
                topicName,
                new ListKafkaDataSource(ImmutableList.of(
                        new KafkaMessage(
                                contentsBuilder()
                                        .appendBytes(protobufData)
                                        .build()))),
                1,
                1);
        KafkaTableManager kafkaTableManager = (KafkaTableManager) testContext().getDependency(TableManager.class, "kafka");
        kafkaTableManager.createImmutable(tableDefinition, tableHandle(tableName).inSchema(KAFKA_SCHEMA));
    }

    @FunctionalInterface
    private interface MessageSerializer
    {
        byte[] serialize(String topic, ProtobufSchema protobufSchema, Map<String, Object> values)
                throws Exception;
    }

    private static final class ProtobufMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, ProtobufSchema protobufSchema, Map<String, Object> values)
        {
            return buildDynamicMessage(protobufSchema.toDescriptor(), values).toByteArray();
        }
    }

    private static final class SchemaRegistryProtobufMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, ProtobufSchema protobufSchema, Map<String, Object> values)
                throws Exception
        {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                // Write magic byte
                out.write((byte) 0);

                // Write SchemaId
                int schemaId = getSchemaRegistryClient().register(
                        topic + "-value",
                        protobufSchema);
                out.write(Ints.toByteArray(schemaId));

                // Write empty MessageIndexes
                out.write((byte) 0);

                out.write(buildDynamicMessage(protobufSchema.toDescriptor(), values).toByteArray());
                return out.toByteArray();
            }
        }
    }

    private static DynamicMessage buildDynamicMessage(Descriptor descriptor, Map<String, Object> data)
    {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(entry.getKey());
            if (entry.getValue() instanceof Map<?, ?>) {
                builder.setField(
                        fieldDescriptor,
                        ImmutableList.of(
                                buildDynamicMessage(fieldDescriptor.getMessageType(), (Map<String, Object>) entry.getValue())));
            }
            else {
                builder.setField(fieldDescriptor, entry.getValue());
            }
        }
        return builder.build();
    }
}
