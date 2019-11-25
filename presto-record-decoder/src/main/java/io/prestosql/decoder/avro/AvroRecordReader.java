package io.prestosql.decoder.avro;

import org.apache.avro.generic.GenericRecord;

public interface AvroRecordReader {
  GenericRecord read(byte[] data);
}
