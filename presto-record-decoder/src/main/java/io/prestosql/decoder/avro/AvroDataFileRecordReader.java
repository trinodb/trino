package io.prestosql.decoder.avro;

import io.prestosql.spi.PrestoException;
import java.io.ByteArrayInputStream;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.compress.utils.IOUtils.closeQuietly;

public class AvroDataFileRecordReader implements AvroRecordReader {
  private final DatumReader<GenericRecord> avroRecordReader;

  public AvroDataFileRecordReader(DatumReader<GenericRecord> avroRecordReader) {
    this.avroRecordReader = requireNonNull(avroRecordReader, "avroRecordReader is null");
  }

  @Override public GenericRecord read(byte[] data) {
    GenericRecord avroRecord;

    DataFileStream<GenericRecord> dataFileReader = null;
    try {
      // Assumes producer uses DataFileWriter or data comes in this particular format.
      // TODO: Support other forms for producers
      dataFileReader = new DataFileStream<>(new ByteArrayInputStream(data), avroRecordReader);
      if (!dataFileReader.hasNext()) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "No avro record found");
      }
      avroRecord = dataFileReader.next();
      if (dataFileReader.hasNext()) {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected extra record found");
      }
    } catch (Exception e) {
      throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
    } finally {
      closeQuietly(dataFileReader);
    }

    return avroRecord;
  }
}
