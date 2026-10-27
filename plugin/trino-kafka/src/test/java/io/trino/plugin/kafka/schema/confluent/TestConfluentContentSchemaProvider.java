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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.protobuf.ProtobufRowDecoder;
import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.kafka.schema.confluent.ConfluentSchemaRegistryDynamicMessageProvider.readerSchemaReference;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestConfluentContentSchemaProvider
{
    private static final String TOPIC = "test";
    private static final String SUBJECT_NAME = format("%s-value", TOPIC);

    @Test
    public void testAvroConfluentSchemaProvider()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        AvroSchema schema = getAvroSchema();
        mockSchemaRegistryClient.register(SUBJECT_NAME, schema);
        ConfluentContentSchemaProvider avroConfluentSchemaProvider = new ConfluentContentSchemaProvider(mockSchemaRegistryClient);
        KafkaTableHandle tableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, AvroRowDecoderFactory.NAME, AvroRowDecoderFactory.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(SUBJECT_NAME), ImmutableList.of(), TupleDomain.all());
        assertThat(avroConfluentSchemaProvider.getMessage(tableHandle)).isEqualTo(Optional.of(schema).map(AvroSchema::toString));
        assertThat(avroConfluentSchemaProvider.getKey(tableHandle)).isEqualTo(Optional.empty());
        KafkaTableHandle invalidTableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, AvroRowDecoderFactory.NAME, AvroRowDecoderFactory.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("another-schema"), ImmutableList.of(), TupleDomain.all());
        assertThatThrownBy(() -> avroConfluentSchemaProvider.getMessage(invalidTableHandle))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not resolve schema for the 'another-schema' subject");
    }

    @Test
    public void testAvroSchemaWithReferences()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        int schemaId = mockSchemaRegistryClient.register("base_schema-value", getAvroSchema());
        ParsedSchema schemaWithReference = mockSchemaRegistryClient.parseSchema(null, getAvroSchemaWithReference(), ImmutableList.of(new SchemaReference(TOPIC, "base_schema-value", schemaId)))
                .orElseThrow();
        mockSchemaRegistryClient.register(SUBJECT_NAME, schemaWithReference);

        ConfluentContentSchemaProvider avroConfluentSchemaProvider = new ConfluentContentSchemaProvider(mockSchemaRegistryClient);
        assertThat(avroConfluentSchemaProvider.readSchema(Optional.empty(), Optional.of(SUBJECT_NAME)).map(schema -> new Parser().parse(schema))).isPresent();
    }

    @Test
    public void testProtobufSchemaIsReferencedById()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
        int schemaId = mockSchemaRegistryClient.register(SUBJECT_NAME, new ConfluentProtobufSchema(
                """
                syntax = "proto3";
                message schema {
                  string col_1 = 1;
                }
                """,
                ImmutableList.of(),
                mockSchemaRegistryClient));

        // a protobuf schema compiles only with its registry references, so the table refers to the schema by id
        ConfluentContentSchemaProvider schemaProvider = new ConfluentContentSchemaProvider(mockSchemaRegistryClient);
        KafkaTableHandle tableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, ProtobufRowDecoder.NAME, ProtobufRowDecoder.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(SUBJECT_NAME), ImmutableList.of(), TupleDomain.all());
        Descriptor message = ((ConfluentProtobufSchema) mockSchemaRegistryClient.getSchemaById(schemaId)).getMessageDescriptor();
        assertThat(schemaProvider.getMessage(tableHandle)).contains(readerSchemaReference(schemaId, message));
        assertThat(schemaProvider.getKey(tableHandle)).isEmpty();
    }

    @Test
    public void testProtobufReaderIsTheSubjectMessage()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
        String schema =
                """
                syntax = "proto3";
                package shop;
                message Header {
                  string name = 1;
                }
                message Order {
                  int64 id = 1;
                  message Line {
                    string sku = 1;
                  }
                }
                """;
        // a record name strategy names the subject after the message the records hold
        int orderId = mockSchemaRegistryClient.register("shop.Order", new ConfluentProtobufSchema(schema, ImmutableList.of(), mockSchemaRegistryClient));
        int lineId = mockSchemaRegistryClient.register(TOPIC + "-shop.Order.Line", new ConfluentProtobufSchema(schema, ImmutableList.of(), mockSchemaRegistryClient));
        int headerId = mockSchemaRegistryClient.register(TOPIC + "-value", new ConfluentProtobufSchema(schema, ImmutableList.of(), mockSchemaRegistryClient));
        ConfluentProtobufSchema parsed = (ConfluentProtobufSchema) mockSchemaRegistryClient.getSchemaById(orderId);
        Descriptor order = parsed.getFileDescriptor().findMessageTypeByName("Order");

        ConfluentContentSchemaProvider schemaProvider = new ConfluentContentSchemaProvider(mockSchemaRegistryClient);
        assertThat(schemaProvider.getMessage(tableHandle("shop.Order"))).contains(readerSchemaReference(orderId, order));
        assertThat(schemaProvider.getMessage(tableHandle(TOPIC + "-shop.Order.Line"))).contains(readerSchemaReference(lineId, order.findNestedTypeByName("Line")));
        assertThat(schemaProvider.getMessage(tableHandle(TOPIC + "-value"))).contains(readerSchemaReference(headerId, parsed.getMessageDescriptor()));
    }

    @Test
    public void testProtobufReaderOfTheTableIsKept()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient(ImmutableList.of(new AvroSchemaProvider(), new ConfluentProtobufSchemaProvider()));
        int firstId = mockSchemaRegistryClient.register(SUBJECT_NAME, new ConfluentProtobufSchema(
                """
                syntax = "proto3";
                message schema {
                  string col_1 = 1;
                }
                """,
                ImmutableList.of(),
                mockSchemaRegistryClient));
        Descriptor first = ((ConfluentProtobufSchema) mockSchemaRegistryClient.getSchemaById(firstId)).getMessageDescriptor();
        String reference = readerSchemaReference(firstId, first);
        KafkaTableHandle tableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, ProtobufRowDecoder.NAME, ProtobufRowDecoder.NAME, Optional.empty(), Optional.of(reference), Optional.empty(), Optional.of(SUBJECT_NAME), ImmutableList.of(), TupleDomain.all());

        // a schema registered after the table was described does not change the schema the table is read with
        mockSchemaRegistryClient.register(SUBJECT_NAME, new ConfluentProtobufSchema(
                """
                syntax = "proto3";
                message schema {
                  string renamed = 1;
                }
                """,
                ImmutableList.of(),
                mockSchemaRegistryClient));
        ConfluentContentSchemaProvider schemaProvider = new ConfluentContentSchemaProvider(mockSchemaRegistryClient);
        assertThat(schemaProvider.getMessage(tableHandle)).contains(reference);
        assertThat(schemaProvider.getMessage(tableHandle(SUBJECT_NAME))).isNotEqualTo(Optional.of(reference));
    }

    private static KafkaTableHandle tableHandle(String messageSubject)
    {
        return new KafkaTableHandle("default", TOPIC, TOPIC, ProtobufRowDecoder.NAME, ProtobufRowDecoder.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(messageSubject), ImmutableList.of(), TupleDomain.all());
    }

    private static String getAvroSchemaWithReference()
    {
        return """
               {
                   "type":"record",
                   "name":"Schema2",
                   "fields":[
                       {"name":"referred","type": "test"},
                       {"name":"col3","type": "string"}
                   ]
               }
               """;
    }

    private static AvroSchema getAvroSchema()
    {
        return new AvroSchema(SchemaBuilder.record(TOPIC)
                .fields()
                .name("col1").type().intType().noDefault()
                .name("col2").type().stringType().noDefault()
                .endRecord());
    }
}
