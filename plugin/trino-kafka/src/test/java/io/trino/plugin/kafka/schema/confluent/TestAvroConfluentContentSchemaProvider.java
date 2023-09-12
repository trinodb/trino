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
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAvroConfluentContentSchemaProvider
{
    private static final String TOPIC = "test";
    private static final String SUBJECT_NAME = format("%s-value", TOPIC);

    @Test
    public void testAvroConfluentSchemaProvider()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Schema schema = getAvroSchema();
        mockSchemaRegistryClient.register(SUBJECT_NAME, schema);
        AvroConfluentContentSchemaProvider avroConfluentSchemaProvider = new AvroConfluentContentSchemaProvider(mockSchemaRegistryClient);
        KafkaTableHandle tableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, AvroRowDecoderFactory.NAME, AvroRowDecoderFactory.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(SUBJECT_NAME), ImmutableList.of(), TupleDomain.all());
        assertEquals(avroConfluentSchemaProvider.getMessage(tableHandle), Optional.of(schema).map(Schema::toString));
        assertEquals(avroConfluentSchemaProvider.getKey(tableHandle), Optional.empty());
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
        int schemaId = mockSchemaRegistryClient.register("base_schema-value", new AvroSchema(getAvroSchema()));
        ParsedSchema schemaWithReference = mockSchemaRegistryClient.parseSchema(null, getAvroSchemaWithReference(), ImmutableList.of(new SchemaReference(TOPIC, "base_schema-value", schemaId)))
                .orElseThrow();
        mockSchemaRegistryClient.register(SUBJECT_NAME, schemaWithReference);

        AvroConfluentContentSchemaProvider avroConfluentSchemaProvider = new AvroConfluentContentSchemaProvider(mockSchemaRegistryClient);
        assertThat(avroConfluentSchemaProvider.readSchema(Optional.empty(), Optional.of(SUBJECT_NAME)).map(schema -> new Parser().parse(schema))).isPresent();
    }

    private static String getAvroSchemaWithReference()
    {
        return "{\n" +
                "    \"type\":\"record\",\n" +
                "    \"name\":\"Schema2\",\n" +
                "    \"fields\":[\n" +
                "        {\"name\":\"referred\",\"type\": \"test\"},\n" +
                "        {\"name\":\"col3\",\"type\": \"string\"}\n" +
                "    ]\n" +
                "}";
    }

    private static Schema getAvroSchema()
    {
        return SchemaBuilder.record(TOPIC)
                .fields()
                .name("col1").type().intType().noDefault()
                .name("col2").type().stringType().noDefault()
                .endRecord();
    }
}
