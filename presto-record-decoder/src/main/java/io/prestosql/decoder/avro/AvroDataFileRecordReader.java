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

import io.prestosql.spi.PrestoException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.compress.utils.IOUtils.closeQuietly;

public class AvroDataFileRecordReader
        implements AvroRecordReader
{
    private final DatumReader<GenericRecord> avroRecordReader;

    public AvroDataFileRecordReader(DatumReader<GenericRecord> avroRecordReader)
    {
        this.avroRecordReader = requireNonNull(avroRecordReader, "avroRecordReader is null");
    }

    @Override
    public GenericRecord read(byte[] data)
    {
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
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
        finally {
            closeQuietly(dataFileReader);
        }

        return avroRecord;
    }
}
