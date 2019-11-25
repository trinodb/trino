package io.prestosql.decoder.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;

public class AvroConfluentSchemaRegistryRecordReader implements AvroRecordReader {
  final KafkaAvroDeserializer deserializer;

  public AvroConfluentSchemaRegistryRecordReader(SchemaRegistryClient schemaRegistryClient) {
    deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
  }

  @Override public GenericRecord read(byte[] data) {
    return (GenericRecord) deserializer.deserialize(null, data);
  }
}
