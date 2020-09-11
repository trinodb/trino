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

import io.prestosql.plugin.hive.RecordFileWriter.ExtendedRecordWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.hadoop.hive.ql.exec.Utilities.createSequenceWriter;

public class SequenceFileRecordWriter
        implements ExtendedRecordWriter
{
    private long finalWrittenBytes = -1;
    private final Writer writer;
    private static final Writable EMPTY_KEY = new BytesWritable();

    public SequenceFileRecordWriter(Path path, JobConf jobConf, Class<?> valueClass, boolean compressed)
            throws IOException
    {
        writer = createSequenceWriter(jobConf, path.getFileSystem(jobConf), path, BytesWritable.class, valueClass, compressed, Reporter.NULL);
    }

    @Override
    public long getWrittenBytes()
    {
        if (finalWrittenBytes != -1) {
            return finalWrittenBytes;
        }
        try {
            return writer.getLength();
        }
        catch (IOException e) {
            return 0; // do nothing
        }
    }

    @Override
    public void write(Writable writable)
            throws IOException
    {
        writer.append(EMPTY_KEY, writable);
    }

    @Override
    public void close(boolean abort)
            throws IOException
    {
        try (Closeable ignored = writer) {
            if (finalWrittenBytes == -1) {
                writer.hflush();
                finalWrittenBytes = writer.getLength();
            }
        }
    }
}
