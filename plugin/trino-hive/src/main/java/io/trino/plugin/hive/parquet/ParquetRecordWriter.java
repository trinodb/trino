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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.RecordFileWriter.ExtendedRecordWriter;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.write.ParquetRecordWriterWrapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.DisabledMemoryManager;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;

import static io.trino.plugin.hive.HiveSessionProperties.getParquetWriterBlockSize;
import static io.trino.plugin.hive.HiveSessionProperties.getParquetWriterPageSize;
import static java.util.Objects.requireNonNull;

public final class ParquetRecordWriter
        implements ExtendedRecordWriter
{
    private static final Field REAL_WRITER_FIELD;
    private static final Field INTERNAL_WRITER_FIELD;
    private static final Field FILE_WRITER_FIELD;

    static {
        try {
            REAL_WRITER_FIELD = ParquetRecordWriterWrapper.class.getDeclaredField("realWriter");
            INTERNAL_WRITER_FIELD = org.apache.parquet.hadoop.ParquetRecordWriter.class.getDeclaredField("internalWriter");
            FILE_WRITER_FIELD = INTERNAL_WRITER_FIELD.getType().getDeclaredField("parquetFileWriter");

            REAL_WRITER_FIELD.setAccessible(true);
            INTERNAL_WRITER_FIELD.setAccessible(true);
            FILE_WRITER_FIELD.setAccessible(true);

            replaceHadoopParquetMemoryManager();
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    public static RecordWriter create(Path target, JobConf conf, Properties properties, ConnectorSession session)
            throws IOException, ReflectiveOperationException
    {
        conf.setLong(ParquetOutputFormat.BLOCK_SIZE, getParquetWriterBlockSize(session).toBytes());
        conf.setLong(ParquetOutputFormat.PAGE_SIZE, getParquetWriterPageSize(session).toBytes());

        RecordWriter recordWriter = new MapredParquetOutputFormat()
                .getHiveRecordWriter(conf, target, Text.class, false, properties, Reporter.NULL);

        Object realWriter = REAL_WRITER_FIELD.get(recordWriter);
        Object internalWriter = INTERNAL_WRITER_FIELD.get(realWriter);
        ParquetFileWriter fileWriter = (ParquetFileWriter) FILE_WRITER_FIELD.get(internalWriter);

        return new ParquetRecordWriter(recordWriter, fileWriter);
    }

    public static void replaceHadoopParquetMemoryManager()
    {
        try {
            Field memoryManager = org.apache.parquet.hadoop.ParquetOutputFormat.class.getDeclaredField("memoryManager");
            memoryManager.setAccessible(true);
            memoryManager.set(null, new DisabledMemoryManager());
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final RecordWriter recordWriter;
    private final ParquetFileWriter fileWriter;
    private long length;

    private ParquetRecordWriter(RecordWriter recordWriter, ParquetFileWriter fileWriter)
    {
        this.recordWriter = requireNonNull(recordWriter, "recordWriter is null");
        this.fileWriter = requireNonNull(fileWriter, "fileWriter is null");
    }

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
}
