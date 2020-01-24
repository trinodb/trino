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
package io.prestosql.plugin.hive.util;

import com.google.common.base.Splitter;
import io.prestosql.plugin.hive.RecordFileWriter.ExtendedRecordWriter;
import io.prestosql.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.HiveSchemaConverter;
import org.apache.hadoop.hive.ql.io.parquet.write.DataWritableWriteSupport;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetRecordWriter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveSessionProperties.getParquetWriterBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getParquetWriterPageSize;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfosFromTypeString;

public final class ParquetRecordWriterUtil
{
    private static final Field REAL_WRITER_FIELD;
    private static final Field INTERNAL_WRITER_FIELD;
    private static final Field FILE_WRITER_FIELD;

    static {
        try {
            REAL_WRITER_FIELD = ParquetRecordWriterWrapper.class.getDeclaredField("realWriter");
            INTERNAL_WRITER_FIELD = ParquetRecordWriter.class.getDeclaredField("internalWriter");
            FILE_WRITER_FIELD = INTERNAL_WRITER_FIELD.getType().getDeclaredField("parquetFileWriter");

            REAL_WRITER_FIELD.setAccessible(true);
            INTERNAL_WRITER_FIELD.setAccessible(true);
            FILE_WRITER_FIELD.setAccessible(true);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private ParquetRecordWriterUtil() {}

    public static RecordWriter createParquetWriter(Path target, JobConf conf, Properties properties, ConnectorSession session)
            throws IOException, ReflectiveOperationException
    {
        conf.setLong(ParquetOutputFormat.BLOCK_SIZE, getParquetWriterBlockSize(session).toBytes());
        conf.setLong(ParquetOutputFormat.PAGE_SIZE, getParquetWriterPageSize(session).toBytes());

        RecordWriter recordWriter = createParquetWriter(target, conf, properties);

        Object realWriter = REAL_WRITER_FIELD.get(recordWriter);
        Object internalWriter = INTERNAL_WRITER_FIELD.get(realWriter);
        ParquetFileWriter fileWriter = (ParquetFileWriter) FILE_WRITER_FIELD.get(internalWriter);

        return new ExtendedRecordWriter()
        {
            private long length;

            @Override
            public long getWrittenBytes()
            {
                return length;
            }

            @Override
            public void write(Writable value)
                    throws IOException
            {
                recordWriter.write(value);
                length = fileWriter.getPos();
            }

            @Override
            public void close(boolean abort)
                    throws IOException
            {
                recordWriter.close(abort);
                if (!abort) {
                    length = fileWriter.getPos();
                }
            }
        };
    }

    private static RecordWriter createParquetWriter(Path target, JobConf conf, Properties properties)
            throws IOException
    {
        if (conf.get(DataWritableWriteSupport.PARQUET_HIVE_SCHEMA) == null) {
            List<String> columnNames = Splitter.on(',').splitToList(properties.getProperty(IOConstants.COLUMNS));
            List<TypeInfo> columnTypes = getTypeInfosFromTypeString(properties.getProperty(IOConstants.COLUMNS_TYPES));
            MessageType schema = HiveSchemaConverter.convert(columnNames, columnTypes);
            setParquetSchema(conf, schema);
        }

        ParquetOutputFormat<ParquetHiveRecord> outputFormat = new ParquetOutputFormat<>(new DataWritableWriteSupport());

        return new ParquetRecordWriterWrapper(outputFormat, conf, target.toString(), Reporter.NULL, properties);
    }

    public static void setParquetSchema(Configuration conf, MessageType schema)
    {
        DataWritableWriteSupport.setSchema(schema, conf);
    }
}
