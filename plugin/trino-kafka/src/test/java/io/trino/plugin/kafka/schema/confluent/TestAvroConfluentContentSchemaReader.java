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
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.plugin.kafka.KafkaTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestAvroConfluentContentSchemaReader
{
    private static final String TOPIC = "test";
    private static final String SUBJECT_NAME = format("%s-value", TOPIC);

    @Test
    public void testAvroConfluentSchemaReader()
            throws Exception
    {
        MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Schema schema = getAvroSchema();
        mockSchemaRegistryClient.register(SUBJECT_NAME, schema);
        AvroConfluentContentSchemaReader avroConfluentSchemaReader = new AvroConfluentContentSchemaReader(mockSchemaRegistryClient);
        KafkaTableHandle tableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, AvroRowDecoderFactory.NAME, AvroRowDecoderFactory.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of(SUBJECT_NAME), ImmutableList.of(), TupleDomain.all());
        assertEquals(avroConfluentSchemaReader.readValueContentSchema(tableHandle), Optional.of(schema).map(Schema::toString));
        assertEquals(avroConfluentSchemaReader.readKeyContentSchema(tableHandle), Optional.empty());
        KafkaTableHandle invalidTableHandle = new KafkaTableHandle("default", TOPIC, TOPIC, AvroRowDecoderFactory.NAME, AvroRowDecoderFactory.NAME, Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("another-schema"), ImmutableList.of(), TupleDomain.all());
        assertThatThrownBy(() -> avroConfluentSchemaReader.readValueContentSchema(invalidTableHandle))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Could not resolve schema for the 'another-schema' subject");
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
