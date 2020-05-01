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
package io.prestosql.plugin.hive.avro;

import io.prestosql.plugin.hive.RecordFileWriter.ExtendedRecordWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.avro.AvroGenericRecordWriter;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Properties;

import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.mapred.AvroJob.OUTPUT_CODEC;
import static org.apache.avro.mapred.AvroOutputFormat.DEFLATE_LEVEL_KEY;

public class AvroRecordWriter
        implements ExtendedRecordWriter
{
    private final RecordWriter delegate;
    private final FSDataOutputStream outputStream;

    public AvroRecordWriter(Path path, JobConf jobConf, boolean isCompressed, Properties properties)
            throws IOException
    {
        Schema schema;
        try {
            schema = AvroSerdeUtils.determineSchemaOrThrowException(jobConf, properties);
        }
        catch (AvroSerdeException e) {
            throw new IOException(e);
        }
        GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(genericDatumWriter);

        if (isCompressed) {
            int level = jobConf.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
            String codecName = jobConf.get(OUTPUT_CODEC, DEFLATE_CODEC);
            CodecFactory factory = codecName.equals(DEFLATE_CODEC)
                    ? CodecFactory.deflateCodec(level)
                    : CodecFactory.fromString(codecName);
            dataFileWriter.setCodec(factory);
        }

        outputStream = path.getFileSystem(jobConf).create(path);
        dataFileWriter.create(schema, outputStream);
        delegate = new AvroGenericRecordWriter(dataFileWriter);
    }

    @Override
    public long getWrittenBytes()
    {
        return outputStream.getPos();
    }

    @Override
    public void write(Writable writable)
            throws IOException
    {
        delegate.write(writable);
    }

    @Override
    public void close(boolean abort)
            throws IOException
    {
        delegate.close(abort);
    }
}
