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
package io.trino.plugin.kafka.encoder.protobuf;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.trino.plugin.kafka.KafkaColumnHandle;
import io.trino.plugin.kafka.encoder.EncoderColumnHandle;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.plugin.kafka.encoder.RowEncoderSpec;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufSchema;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufSchemaProvider;
import io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.Frame;
import io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryDynamicMessageProvider;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.TestingConnectorSession;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_ERROR;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_SCHEMA_REGISTRY_ERROR;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.KEY;
import static io.trino.plugin.kafka.encoder.KafkaFieldType.MESSAGE;
import static io.trino.plugin.kafka.schema.confluent.ConfluentProtobufWireFormat.readFrame;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryDynamicMessageProvider.readerSchemaReference;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConfluentProtobufRowEncoderFactory
{
    private static final String TOPIC = "orders";
    private static final String SCHEMA =
            """
            syntax = "proto3";
            message schema {
                string col_1 = 1;
                int64 col_2 = 2;
            }
            message Other {
                string name = 1;
            }
            """;
    private static final String EVOLVED_SCHEMA =
            """
            syntax = "proto3";
            message schema {
                string col_1 = 1;
                int64 col_2 = 2;
                int64 col_3 = 3;
            }
            """;

    private static final KafkaColumnHandle COL_1 = new KafkaColumnHandle("col_1", VARCHAR, "col_1", null, null, false, false, false, Optional.empty());
    private static final KafkaColumnHandle COL_2 = new KafkaColumnHandle("col_2", BIGINT, "col_2", null, null, false, false, false, Optional.empty());
    private static final KafkaColumnHandle COL_3 = new KafkaColumnHandle("col_3", BIGINT, "col_3", null, null, false, false, false, Optional.empty());

    @Test
    public void testEncodeInConfluentWireFormat()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register(TOPIC + "-value", schema(client, SCHEMA));
        Descriptor descriptor = ((ConfluentProtobufSchema) client.getSchemaById(schemaId)).getMessageDescriptor();

        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1, COL_2))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 42L), 0);
            byte[] record = encoder.toByteArray();

            Frame frame = readFrame(record);
            assertThat(frame.schemaId()).isEqualTo(schemaId);
            assertThat(frame.messageIndexes()).containsExactly(0);
            DynamicMessage expected = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("col_1"), "Trino")
                    .setField(descriptor.findFieldByName("col_2"), 42L)
                    .build();
            assertThat(Arrays.copyOfRange(record, frame.payloadOffset(), record.length)).isEqualTo(expected.toByteArray());

            // the record decodes with the schema registry based decoder
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getField(descriptor.findFieldByName("col_1"))).isEqualTo("Trino");
            assertThat(decoded.getField(descriptor.findFieldByName("col_2"))).isEqualTo(42L);

            // the encoder is reset after each row
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("second")), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 2L), 0);
            DynamicMessage second = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(encoder.toByteArray());
            assertThat(second.getField(descriptor.findFieldByName("col_1"))).isEqualTo("second");
            assertThat(second.getField(descriptor.findFieldByName("col_2"))).isEqualTo(2L);
        }
    }

    @Test
    public void testLatestSchemaIsUsed()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int firstId = client.register(TOPIC + "-value", schema(client, SCHEMA));
        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1, COL_2))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 1L), 0);
            assertThat(readFrame(encoder.toByteArray()).schemaId()).isEqualTo(firstId);
        }

        int secondId = client.register(TOPIC + "-value", schema(client, EVOLVED_SCHEMA));
        assertThat(secondId).isNotEqualTo(firstId);
        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1, COL_2, COL_3))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 1L), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 3L), 0);
            byte[] record = encoder.toByteArray();
            assertThat(readFrame(record).schemaId()).isEqualTo(secondId);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getField(decoded.getDescriptorForType().findFieldByName("col_3"))).isEqualTo(3L);
        }
    }

    @Test
    public void testSchemaOfTheTableIsUsed()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        String subject = TOPIC + "-value";
        int tableSchemaId = client.register(subject, schema(client, SCHEMA));
        Descriptor tableMessage = ((ConfluentProtobufSchema) client.getSchemaById(tableSchemaId)).getMessageDescriptor();
        String dataSchema = readerSchemaReference(tableSchemaId, tableMessage);
        // a version registered after the table was described renames col_1, which the table still has
        int latestId = client.register(subject, schema(client,
                """
                syntax = "proto3";
                message schema {
                    string renamed = 1;
                    int64 col_2 = 2;
                }
                """));
        assertThat(latestId).isNotEqualTo(tableSchemaId);

        try (RowEncoder encoder = createEncoder(client, Optional.of(dataSchema), Optional.of(subject), ImmutableList.of(COL_1, COL_2))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            encoder.appendColumnValue(writeNativeValue(BIGINT, 1L), 0);
            byte[] record = encoder.toByteArray();
            assertThat(readFrame(record).schemaId()).isEqualTo(tableSchemaId);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getField(decoded.getDescriptorForType().findFieldByName("col_1"))).isEqualTo("Trino");
        }

        // the table's message is used even when the subject does not name it
        Descriptor other = ((ConfluentProtobufSchema) client.getSchemaById(tableSchemaId)).findMessage("Other").orElseThrow();
        KafkaColumnHandle name = new KafkaColumnHandle("name", VARCHAR, "name", null, null, false, false, false, Optional.empty());
        try (RowEncoder encoder = createEncoder(client, Optional.of(readerSchemaReference(tableSchemaId, other)), Optional.of(subject), ImmutableList.of(name))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("other")), 0);
            Frame frame = readFrame(encoder.toByteArray());
            assertThat(frame.schemaId()).isEqualTo(tableSchemaId);
            assertThat(frame.messageIndexes()).containsExactly(1);
        }

        assertTrinoException(
                () -> createEncoder(client, Optional.of("schema-registry-id:" + tableSchemaId + ";message=Missing"), Optional.of(subject), ImmutableList.of(COL_1)),
                KAFKA_SCHEMA_ERROR,
                "Schema id " + tableSchemaId + " of subject 'orders-value' does not declare message Missing");

        // the schema of the table was deleted from the registry after the table was described
        assertTrinoException(
                () -> createEncoder(client, Optional.of("schema-registry-id:999;message=schema"), Optional.of(subject), ImmutableList.of(COL_1)),
                KAFKA_SCHEMA_ERROR,
                "Schema id 999 of subject 'orders-value' does not exist in the schema registry");
    }

    @Test
    public void testKeySubject()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register(TOPIC + "-value", schema(client, SCHEMA));
        int keySchemaId = client.register(TOPIC + "-key", schema(client,
                """
                syntax = "proto3";
                message schema {
                    int64 key = 1;
                }
                """));
        KafkaColumnHandle key = new KafkaColumnHandle("key", BIGINT, "key", null, null, true, false, false, Optional.empty());
        RowEncoderSpec spec = new RowEncoderSpec(ProtobufRowEncoder.NAME, Optional.empty(), Optional.of(TOPIC + "-key"), ImmutableList.of(key), TOPIC, KEY);
        try (RowEncoder encoder = new ConfluentProtobufRowEncoderFactory(client).create(TestingConnectorSession.SESSION, spec)) {
            encoder.appendColumnValue(writeNativeValue(BIGINT, 7L), 0);
            byte[] record = encoder.toByteArray();
            assertThat(readFrame(record).schemaId()).isEqualTo(keySchemaId);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getField(decoded.getDescriptorForType().findFieldByName("key"))).isEqualTo(7L);
        }
    }

    @Test
    public void testSubjectNamingAnotherMessage()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        // the TopicRecordNameStrategy names the subject after the topic and the message, which is not the first one here
        String subject = TOPIC + "-Other";
        int schemaId = client.register(subject, schema(client, SCHEMA));
        Descriptor other = ((ConfluentProtobufSchema) client.getSchemaById(schemaId)).getFileDescriptor().findMessageTypeByName("Other");
        KafkaColumnHandle name = new KafkaColumnHandle("name", VARCHAR, "name", null, null, false, false, false, Optional.empty());

        try (RowEncoder encoder = createEncoder(client, Optional.of(subject), ImmutableList.of(name))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("second")), 0);
            byte[] record = encoder.toByteArray();
            Frame frame = readFrame(record);
            assertThat(frame.schemaId()).isEqualTo(schemaId);
            assertThat(frame.messageIndexes()).containsExactly(1);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(record);
            assertThat(decoded.getDescriptorForType()).isSameAs(other);
            assertThat(decoded.getField(other.findFieldByName("name"))).isEqualTo("second");
        }
    }

    @Test
    public void testColumnMissingFromSchema()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        client.register(TOPIC + "-value", schema(client, SCHEMA));
        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1, COL_3))) {
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("Trino")), 0);
            assertThatThrownBy(() -> encoder.appendColumnValue(writeNativeValue(BIGINT, 3L), 0))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Unknown Field col_3");
        }
    }

    @Test
    public void testEncodeEnumsAndExplicitMapEntries()
            throws Exception
    {
        // the form of schema text a Confluent serializer registers: explicit map entry messages and fully qualified type names
        MockSchemaRegistryClient client = createClient();
        client.register(TOPIC + "-value", schema(client,
                """
                syntax = "proto3";

                message schema {
                  repeated .schema.AttributesEntry attributes = 1;
                  .schema.Color color = 2;
                  repeated .schema.Color palette = 3;

                  message AttributesEntry {
                    option map_entry = true;

                    string key = 1;
                    string value = 2;
                  }
                  enum Color {
                    RED = 0;
                    GREEN = 1;
                  }
                }
                """));
        MapType mapType = new MapType(VARCHAR, VARCHAR, new TypeOperators());
        ArrayType arrayType = new ArrayType(VARCHAR);
        List<EncoderColumnHandle> columns = ImmutableList.of(
                new KafkaColumnHandle("attributes", mapType, "attributes", null, null, false, false, false, Optional.empty()),
                new KafkaColumnHandle("color", VARCHAR, "color", null, null, false, false, false, Optional.empty()),
                new KafkaColumnHandle("palette", arrayType, "palette", null, null, false, false, false, Optional.empty()));

        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), columns)) {
            encoder.appendColumnValue(writeNativeValue(mapType, buildMapValue(mapType, 1, (keyBuilder, valueBuilder) -> {
                VARCHAR.writeSlice(keyBuilder, utf8Slice("size"));
                VARCHAR.writeSlice(valueBuilder, utf8Slice("large"));
            })), 0);
            encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("GREEN")), 0);
            encoder.appendColumnValue(writeNativeValue(arrayType, varcharArray("GREEN", "RED")), 0);
            DynamicMessage decoded = new ConfluentSchemaRegistryDynamicMessageProvider(client).parseDynamicMessage(encoder.toByteArray());

            Descriptor descriptor = decoded.getDescriptorForType();
            assertThat(descriptor.findFieldByName("attributes").isMapField()).isTrue();
            DynamicMessage entry = (DynamicMessage) decoded.getRepeatedField(descriptor.findFieldByName("attributes"), 0);
            assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("key"))).isEqualTo("size");
            assertThat(entry.getField(entry.getDescriptorForType().findFieldByName("value"))).isEqualTo("large");
            assertThat(((EnumValueDescriptor) decoded.getField(descriptor.findFieldByName("color"))).getName()).isEqualTo("GREEN");
            assertThat((List<?>) decoded.getField(descriptor.findFieldByName("palette")))
                    .extracting(value -> ((EnumValueDescriptor) value).getName())
                    .containsExactly("GREEN", "RED");
        }

        // a name that is not a member of the enum cannot be written
        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), columns.subList(1, 2))) {
            assertTrinoException(
                    () -> encoder.appendColumnValue(writeNativeValue(VARCHAR, utf8Slice("BLUE")), 0),
                    CONSTRAINT_VIOLATION,
                    "Value 'BLUE' is not a member of enum schema.Color of field schema.color");
        }
        try (RowEncoder encoder = createEncoder(client, Optional.of(TOPIC + "-value"), columns.subList(2, 3))) {
            assertTrinoException(
                    () -> encoder.appendColumnValue(writeNativeValue(arrayType, varcharArray("RED", "BLUE")), 0),
                    CONSTRAINT_VIOLATION,
                    "Value 'BLUE' is not a member of enum schema.Color of field schema.palette");
        }
    }

    @Test
    public void testSubjectIsRequired()
    {
        MockSchemaRegistryClient client = createClient();
        assertTrinoException(() -> createEncoder(client, Optional.empty(), ImmutableList.of(COL_1)), NOT_SUPPORTED, "Inserting into the message of table 'orders' requires a schema registry subject");
    }

    @Test
    public void testUnknownSubject()
    {
        MockSchemaRegistryClient client = createClient();
        assertTrinoException(() -> createEncoder(client, Optional.of("missing-value"), ImmutableList.of(COL_1)), KAFKA_SCHEMA_ERROR, "Subject 'missing-value' does not exist in the schema registry");
    }

    @Test
    public void testNonProtobufSubject()
            throws Exception
    {
        MockSchemaRegistryClient client = createClient();
        int schemaId = client.register(TOPIC + "-value", new AvroSchema(SchemaBuilder.record("schema").fields().name("col_1").type().stringType().noDefault().endRecord()));
        assertTrinoException(() -> createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1)), KAFKA_SCHEMA_ERROR, "Schema id " + schemaId + " of subject 'orders-value' is a AVRO schema, not a PROTOBUF schema");
    }

    @Test
    public void testRegistryFailure()
    {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient(ImmutableList.of(new ConfluentProtobufSchemaProvider()))
        {
            @Override
            public SchemaMetadata getLatestSchemaMetadata(String subject)
                    throws IOException, RestClientException
            {
                throw new IOException("Connection refused");
            }
        };
        assertTrinoException(() -> createEncoder(client, Optional.of(TOPIC + "-value"), ImmutableList.of(COL_1)), KAFKA_SCHEMA_REGISTRY_ERROR, "Failed to look up the latest schema of subject 'orders-value' in the schema registry: Connection refused");
    }

    private static Block varcharArray(String... values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, values.length);
        for (String value : values) {
            VARCHAR.writeSlice(builder, utf8Slice(value));
        }
        return builder.build();
    }

    private static RowEncoder createEncoder(MockSchemaRegistryClient client, Optional<String> subject, List<EncoderColumnHandle> columns)
    {
        return createEncoder(client, Optional.empty(), subject, columns);
    }

    private static RowEncoder createEncoder(MockSchemaRegistryClient client, Optional<String> dataSchema, Optional<String> subject, List<EncoderColumnHandle> columns)
    {
        RowEncoderSpec spec = new RowEncoderSpec(ProtobufRowEncoder.NAME, dataSchema, subject, columns, TOPIC, MESSAGE);
        return new ConfluentProtobufRowEncoderFactory(client).create(TestingConnectorSession.SESSION, spec);
    }

    private static MockSchemaRegistryClient createClient()
    {
        return new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
    }

    private static ConfluentProtobufSchema schema(MockSchemaRegistryClient client, String schema)
    {
        return new ConfluentProtobufSchema(schema, ImmutableList.of(), client);
    }

    private static void assertTrinoException(Runnable runnable, ErrorCodeSupplier errorCode, String message)
    {
        assertThatThrownBy(runnable::run)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()))
                .hasMessage(message);
    }
}
