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
package io.prestosql.decoder.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;

public class AvroConfluentSchemaRegistryRecordReader
        implements AvroRecordReader
{
    private final KafkaAvroDeserializer deserializer;

    AvroConfluentSchemaRegistryRecordReader(SchemaRegistryClient schemaRegistryClient)
    {
        deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    @Override
    public GenericRecord read(byte[] data)
    {
        return (GenericRecord) deserializer.deserialize(null, data);
    }
}
