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
import com.google.common.primitives.Ints;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.tempto.ProductTest;
import io.trino.tempto.fulfillment.table.TableManager;
import io.trino.tempto.fulfillment.table.kafka.KafkaMessage;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableDefinition;
import io.trino.tempto.fulfillment.table.kafka.KafkaTableManager;
import io.trino.tempto.fulfillment.table.kafka.ListKafkaDataSource;
import io.trino.tempto.query.QueryResult;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tempto.fulfillment.table.TableHandle.tableHandle;
import static io.trino.tempto.fulfillment.table.kafka.KafkaMessageContentsBuilder.contentsBuilder;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.KAFKA;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.SchemaRegistryClientUtils.getSchemaRegistryClient;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestKafkaAvroReadsSmokeTest
        extends ProductTest
{
    private static final String KAFKA_SCHEMA = "product_tests";

    private static final String ALL_DATATYPES_AVRO_TOPIC_NAME = "read_all_datatypes_avro";
    private static final String ALL_DATATYPE_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/all_datatypes_avro_schema.avsc";

    private static final String ALL_NULL_AVRO_TOPIC_NAME = "read_all_null_avro";

    private static final String STRUCTURAL_AVRO_TOPIC_NAME = "read_structural_datatype_avro";
    private static final String STRUCTURAL_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/structural_datatype_avro_schema.avsc";

    private static final String AVRO_SCHEMA_WITH_REFERENCES_TOPIC_NAME = "schema_with_references_avro";
    private static final String AVRO_SCHEMA_WITH_REFERENCES_SCHEMA_PATH = "/docker/presto-product-tests/conf/presto/etc/catalog/kafka/schema_with_references.avsc";

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS}, dataProvider = "catalogs")
    public void testSelectPrimitiveDataType(KafkaCatalog kafkaCatalog, MessageSerializer messageSerializer)
            throws Exception
    {
        ImmutableMap<String, Object> record = ImmutableMap.of(
                "a_varchar", "foobar",
                "a_bigint", 127L,
                "a_double", 234.567,
                "a_boolean", true);
        String topicName = ALL_DATATYPES_AVRO_TOPIC_NAME + kafkaCatalog.getTopicNameSuffix();
        createAvroTable(ALL_DATATYPE_SCHEMA_PATH, ALL_DATATYPES_AVRO_TOPIC_NAME, topicName, record, messageSerializer);
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = query(format("select * from %s.%s", kafkaCatalog.getCatalogName(), KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(
                            "foobar",
                            127,
                            234.567,
                            true));
                });
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS}, dataProvider = "catalogs")
    public void testNullType(KafkaCatalog kafkaCatalog, MessageSerializer messageSerializer)
            throws Exception
    {
        String topicName = ALL_NULL_AVRO_TOPIC_NAME + kafkaCatalog.getTopicNameSuffix();
        createAvroTable(ALL_DATATYPE_SCHEMA_PATH, ALL_NULL_AVRO_TOPIC_NAME, topicName, ImmutableMap.of(), messageSerializer);
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = query(format("select * from %s.%s", kafkaCatalog.getCatalogName(), KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(
                            null,
                            null,
                            null,
                            null));
                });
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS}, dataProvider = "catalogs")
    public void testSelectStructuralDataType(KafkaCatalog kafkaCatalog, MessageSerializer messageSerializer)
            throws Exception
    {
        ImmutableMap<String, Object> record = ImmutableMap.of(
                "a_array", ImmutableList.of(100L, 102L),
                "a_map", ImmutableMap.of("key1", "value1"));
        String topicName = STRUCTURAL_AVRO_TOPIC_NAME + kafkaCatalog.getTopicNameSuffix();
        createAvroTable(STRUCTURAL_SCHEMA_PATH, STRUCTURAL_AVRO_TOPIC_NAME, topicName, record, messageSerializer);
        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = query(format(
                            "SELECT a[1], a[2], m['key1'] FROM (SELECT %s as a, %s as m FROM %s.%s) t",
                            kafkaCatalog.isColumnMappingSupported() ? "c_array" : "a_array",
                            kafkaCatalog.isColumnMappingSupported() ? "c_map" : "a_map",
                            kafkaCatalog.getCatalogName(),
                            KAFKA_SCHEMA + "." + topicName));
                    assertThat(queryResult).containsOnly(row(100, 102, "value1"));
                });
    }

    @DataProvider
    private static Object[][] catalogs()
    {
        return new Object[][] {
                {
                        new KafkaCatalog("kafka", "", true), new AvroMessageSerializer(),
                },
                {
                        new KafkaCatalog("kafka_schema_registry", "_schema_registry", false), new SchemaRegistryAvroMessageSerializer(),
                },
        };
    }

    private static final class KafkaCatalog
    {
        private final String catalogName;
        private final String topicNameSuffix;
        private final boolean columnMappingSupported;

        private KafkaCatalog(String catalogName, String topicNameSuffix, boolean columnMappingSupported)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.topicNameSuffix = requireNonNull(topicNameSuffix, "topicNameSuffix is null");
            this.columnMappingSupported = columnMappingSupported;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public String getTopicNameSuffix()
        {
            return topicNameSuffix;
        }

        public boolean isColumnMappingSupported()
        {
            return columnMappingSupported;
        }

        @Override
        public String toString()
        {
            return catalogName;
        }
    }

    @Test(groups = {KAFKA, PROFILE_SPECIFIC_TESTS})
    public void testAvroWithSchemaReferences()
            throws Exception
    {
        TestingAvroSchema referredSchema = new TestingAvroSchema(Files.readString(new File(ALL_DATATYPE_SCHEMA_PATH).toPath()), ImmutableList.of(), ImmutableList.of());

        getSchemaRegistryClient().register(
                ALL_DATATYPES_AVRO_TOPIC_NAME + "-value",
                referredSchema);

        Map<String, Object> record = ImmutableMap.of(
                "a_varchar", "foobar",
                "a_bigint", 127L,
                "a_double", 234.567,
                "a_boolean", true);

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder((Schema) referredSchema.rawSchema());
        record.forEach(recordBuilder::set);

        TestingAvroSchema actualSchema = new TestingAvroSchema(
                Files.readString(new File(AVRO_SCHEMA_WITH_REFERENCES_SCHEMA_PATH).toPath()),
                ImmutableList.of(new SchemaReference(referredSchema.name(), ALL_DATATYPES_AVRO_TOPIC_NAME + "-value", 1)),
                ImmutableList.of(referredSchema.canonicalString()));

        // This is a bit hacky as KafkaTableManager relies on kafka catalog's tables for inserting data into a given topic
        createAvroTable(actualSchema, ALL_DATATYPES_AVRO_TOPIC_NAME, AVRO_SCHEMA_WITH_REFERENCES_TOPIC_NAME, ImmutableMap.of("reference", recordBuilder.build()), new SchemaRegistryAvroMessageSerializer());

        assertEventually(
                new Duration(30, SECONDS),
                () -> {
                    QueryResult queryResult = query(format("select reference.a_varchar, reference.a_double from kafka_schema_registry.%s.%s", KAFKA_SCHEMA, AVRO_SCHEMA_WITH_REFERENCES_TOPIC_NAME));
                    assertThat(queryResult).containsOnly(row(
                            "foobar",
                            234.567));
                });
    }

    private static void createAvroTable(String schemaPath, String tableName, String topicName, Map<String, Object> record, MessageSerializer messageSerializer)
            throws Exception
    {
        String schema = Files.readString(new File(schemaPath).toPath());
        createAvroTable(new TestingAvroSchema(schema, ImmutableList.of(), ImmutableList.of()), tableName, topicName, record, messageSerializer);
    }

    private static void createAvroTable(TestingAvroSchema schema, String tableName, String topicName, Map<String, Object> record, MessageSerializer messageSerializer)
            throws Exception
    {
        byte[] avroData = messageSerializer.serialize(topicName, schema, record);

        KafkaTableDefinition tableDefinition = new KafkaTableDefinition(
                KAFKA_SCHEMA + "." + tableName,
                topicName,
                new ListKafkaDataSource(ImmutableList.of(
                        new KafkaMessage(
                                contentsBuilder()
                                        .appendBytes(avroData)
                                        .build()))),
                1,
                1);
        KafkaTableManager kafkaTableManager = (KafkaTableManager) testContext().getDependency(TableManager.class, "kafka");
        kafkaTableManager.createImmutable(tableDefinition, tableHandle(tableName).inSchema(KAFKA_SCHEMA));
    }

    @FunctionalInterface
    private interface MessageSerializer
    {
        byte[] serialize(String topic, ParsedSchema parsedSchema, Map<String, Object> values)
                throws IOException;
    }

    private static final class AvroMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, ParsedSchema parsedSchema, Map<String, Object> values)
                throws IOException
        {
            Schema schema = (Schema) parsedSchema.rawSchema();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            GenericData.Record record = new GenericData.Record(schema);
            values.forEach(record::put);
            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                dataFileWriter.create(schema, outputStream);
                dataFileWriter.append(record);
            }
            return outputStream.toByteArray();
        }
    }

    private static final class SchemaRegistryAvroMessageSerializer
            implements MessageSerializer
    {
        @Override
        public byte[] serialize(String topic, ParsedSchema parsedSchema, Map<String, Object> values)
                throws IOException
        {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Schema schema = (Schema) parsedSchema.rawSchema();
                out.write((byte) 0);

                int schemaId = getSchemaRegistryClient().register(
                        topic + "-value",
                        parsedSchema);
                out.write(Ints.toByteArray(schemaId));

                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
                DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
                encoder.flush();
                GenericData.Record record = new GenericData.Record(schema);
                values.forEach(record::put);
                writer.write(record, encoder);
                return out.toByteArray();
            }
            catch (RestClientException clientException) {
                throw new RuntimeException(clientException);
            }
        }
    }

    /**
     * Using custom ParsedSchema as Confluent dependencies conflicts with Hive dependencies
     * See {@link io.confluent.kafka.schemaregistry.avro.AvroSchema}.
     * We can remove once we move to latest schema registry.
     */
    private static class TestingAvroSchema
            implements ParsedSchema
    {
        private final String avroSchemaString;
        private final Schema avroSchema;
        private final List<SchemaReference> schemaReferences;

        public TestingAvroSchema(String avroSchemaString, List<SchemaReference> schemaReferences, List<String> resolvedReferences)
        {
            this.avroSchemaString = requireNonNull(avroSchemaString, "avroSchemaString is null");
            this.schemaReferences = ImmutableList.copyOf(requireNonNull(schemaReferences, "schemaReferences is null"));
            requireNonNull(resolvedReferences, "resolvedReferences is null");

            Parser parser = new Parser();
            resolvedReferences.forEach(parser::parse);
            this.avroSchema = parser.parse(avroSchemaString);
        }

        @Override
        public String schemaType()
        {
            return "AVRO";
        }

        @Override
        public String name()
        {
            return avroSchema.getName();
        }

        @Override
        public String canonicalString()
        {
            return avroSchemaString;
        }

        @Override
        public List<SchemaReference> references()
        {
            return schemaReferences;
        }

        @Override
        public boolean isBackwardCompatible(ParsedSchema parsedSchema)
        {
            return false;
        }

        @Override
        public Object rawSchema()
        {
            return avroSchema;
        }
    }
}
